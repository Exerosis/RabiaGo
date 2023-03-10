package rabia

import (
	"fmt"
	"go.uber.org/multierr"
	"sync"
	"sync/atomic"
	"time"
)

const AVERAGE = 10_000
const COUNT = 10_000_000

func OldNode(
	n uint16,
	address string,
	addresses []string,
	pipes ...uint16,
) error {
	var group sync.WaitGroup
	var lock sync.Mutex
	var reasons error
	group.Add(len(pipes))
	var log = MakeLog(n, uint32((65536/len(pipes))*len(pipes)))
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
			proposers, reason := Group(address, pipe+1, addresses...)
			staters, reason := Group(address, pipe+2, addresses...)
			voters, reason := Group(address, pipe+3, addresses...)
			if reason != nil {
				lock.Lock()
				defer lock.Unlock()
				var result = fmt.Errorf("failed to connect %d: %s", index, reason)
				reasons = multierr.Append(reasons, result)
			}
			var proposals = Multicaster(proposers...)
			var states = Multicaster(staters...)
			var votes = Multicaster(voters...)
			info("Connected!\n")
			reason = log.SMR(proposals, states, votes, func() (uint16, uint64, error) {
				return uint16(current % log.Size), instance[i], nil
			}, func(slot uint16, message uint64) error {
				var amount = atomic.AddUint32(&count, 1)
				for amount >= AVERAGE && !atomic.CompareAndSwapUint32(&count, amount, 0) {
					amount = atomic.LoadUint32(&count)
				}
				if amount >= AVERAGE {
					var duration = time.Since(time.Unix(0, atomic.LoadInt64(&mark)))
					atomic.StoreInt64(&mark, time.Now().UnixNano())
					var throughput = float64(amount) / duration.Seconds()
					fmt.Printf("%d\n", uint32(throughput))
				}
				i++
				if i == len(instance)-1 {
					fmt.Println("Done! ", index)
					return fmt.Errorf("done: %d", amount)
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
	fmt.Println("Exiting finally!")
	return reasons
}
