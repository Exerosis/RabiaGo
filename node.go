package main

import (
	"fmt"
	"sync/atomic"
	"time"
)

func Node(
	n uint16,
	address string,
	addresses []string,
	pipes ...uint16,
) error {
	var log = makeLog(n, 65536)
	var instances = make([][]uint64, len(pipes))
	//messages map ig?

	for i := 0; i < COUNT; i++ {
		instances[i%len(pipes)] = append(instances[i%len(pipes)], uint64(i))
	}
	var mark = time.Now().UnixNano()
	var count = uint32(0)
	for index, pipe := range pipes {
		go func(pipe int, instance []uint64) {
			var current = uint16(0)
			var i = uint32(0)
			proposals, reason := TCP(address, uint16(pipe+1), addresses...)
			states, reason := TCP(address, uint16(pipe+2), addresses...)
			votes, reason := TCP(address, uint16(pipe+3), addresses...)
			if reason != nil {
				fmt.Println("Failed to connect: ", reason)
				return
			}
			fmt.Println("Connected!")
			reason = log.SMR(proposals, states, votes, func() (uint16, uint64) {
				return current, instance[i]
			}, func(slot uint16, message uint64) {
				var amount = atomic.AddUint32(&count, 1)
				for amount >= AVERAGE && !atomic.CompareAndSwapUint32(&count, amount, 0) {
					amount = atomic.LoadUint32(&count)
				}
				if amount >= AVERAGE {
					var duration = time.Since(time.Unix(0, atomic.LoadInt64(&mark)))
					atomic.StoreInt64(&mark, time.Now().UnixNano())
					var throughput = float64(amount) / duration.Seconds()
					fmt.Printf("%d\n", uint64(throughput))
				}
				i++
				current++
			})
			if reason != nil {
				fmt.Println("SMR death: ", reason)
			}
		}(int(pipe), instances[index])
	}
	return nil
}
