package main

import "fmt"

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
			println("wtf")
			var slot = uint16(0)
			proposals, reason := TCP(address, pipe+1, addresses...)
			states, reason := TCP(address, pipe+2, addresses...)
			votes, reason := TCP(address, pipe+3, addresses...)
			fmt.Println("Connected!")
			reason = log.SMR(proposals, states, votes, func() (uint16, uint64) {
				var result uint64
				result, instance = instance[0], instance[1:]
				return slot, result
			}, func(slot uint16, message uint64) {
				print("Ok working ig?")
			})
			if reason != nil {
				print("error")
				print(reason)
			}
		}(int(pipe), instances[index])
	}
	return nil
}
