package main

import (
	"errors"
	"fmt"
	"github.com/shirou/gopsutil/v3/cpu"
	"go.uber.org/multierr"
	"sync"
	"sync/atomic"
	"time"
)

func Node(
	n uint16,
	address string,
	addresses []string,
	pipes ...uint16,
) error {
	var group sync.WaitGroup
	var lock sync.Mutex
	var reasons error
	group.Add(len(pipes))
	var log = makeLog(n, uint32((65536/len(pipes))*len(pipes)))
	var instances = make([][]uint64, len(pipes))
	//messages map ig?

	for i := 0; i < COUNT; i++ {
		instances[i%len(pipes)] = append(instances[i%len(pipes)], uint64(i))
	}
	fmt.Println("Instance Length: ", len(instances[0]))

	var mark = time.Now().UnixNano()
	var count = uint32(0)
	for index, pipe := range pipes {
		go func(index int, pipe uint16, instance []uint64) {
			defer group.Done()
			var info = func(format string, a ...interface{}) {
				if INFO {
					fmt.Printf(fmt.Sprintf("[Pipe-%d] %s", index, format), a...)
				}
			}

			var current = uint32(index)
			var i = 0
			proposals, reason := TCP(address, pipe+1, addresses...)
			states, reason := TCP(address, pipe+2, addresses...)
			votes, reason := TCP(address, pipe+3, addresses...)
			if reason != nil {
				lock.Lock()
				defer lock.Unlock()
				var result = fmt.Errorf("failed to connect %d: %s", index, reason)
				reasons = multierr.Append(reasons, result)
			}
			info("Connected!\n")
			reason = log.SMR(proposals, states, votes, func() (uint16, uint64, error) {
				if i >= len(instance) {
					return 0, 0, errors.New("ran out of entries")
				}
				return uint16(current % log.size), instance[i], nil
			}, func(slot uint16, message uint64) error {
				var amount = atomic.AddUint32(&count, 1)
				for amount >= AVERAGE && !atomic.CompareAndSwapUint32(&count, amount, 0) {
					amount = atomic.LoadUint32(&count)
				}
				if amount >= AVERAGE {
					percent, reason := cpu.Percent(0, false)
					if reason != nil {
						return reason
					}
					var duration = time.Since(time.Unix(0, atomic.LoadInt64(&mark)))
					atomic.StoreInt64(&mark, time.Now().UnixNano())
					var throughput = float64(amount) / duration.Seconds()
					fmt.Printf("%d - %.2f\n", uint32(throughput), percent[0])
				}
				i++
				if i == len(instance)-1 {
					return errors.New("done")
				}
				current += uint32(len(pipes))
				return nil
			}, info)
			if reason != nil {
				lock.Lock()
				defer lock.Unlock()
				var result = fmt.Errorf("running smr pipe %d: %s", index, reason)
				reasons = result
			}
			return
		}(index, pipe, instances[index])
	}
	group.Wait()
	return reasons
}
