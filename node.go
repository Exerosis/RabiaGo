package main

import (
	"fmt"
)

func Node(
	n uint32,
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
				if uint64(current) != instance[i] {
					println("problem ", current, " ", instance[i])
				}
				return current, instance[i]
			}, func(slot uint16, message uint64) {
				if i%AVERAGE == 0 {
					println(i)
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
