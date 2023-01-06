package Rabia

import (
	. "encoding/binary"
	"errors"
	"math"
)

// new = a current = b
// if new is less than current but not by a huge amount then it's old
// if new is greater than current by a huge amount then it's old
func isOld(a uint16, b uint16, half uint16) bool {
	return a < b && (b-a) < half || a > b && (a-b) > half
}

func (log Log) Node(
	proposes Multicaster,
	states Multicaster,
	votes Multicaster,
	messages func() (uint16, uint64),
	commit func(uint, uint64),
) error {
	var buffer = make([]byte, 10)
	var half = uint16(len(log.logs) / 2)
	var shift = uint32(math.Floor(math.Log2(float64(log.majority)))) + 1
	for proposes.isOpen() {
		current, proposed := messages()
		LittleEndian.PutUint16(buffer[0:], current)
		LittleEndian.PutUint64(buffer[2:], proposed)
		reason := proposes.send(buffer)
		if reason != nil {
			return reason
		}

		for log.indices[current] < log.majority {
			reason := proposes.receive(buffer)
			if reason != nil {
				return reason
			}
			var depth = LittleEndian.Uint16(buffer[0:])
			if isOld(depth, current, half) {
				continue
			}
			var proposal = LittleEndian.Uint64(buffer[2:])
			var index = log.indices[depth]
			if index < log.majority {
				log.proposals[index<<shift|index] = proposal
				log.indices[depth]++
			}
		}
		var proposal = log.proposals[current<<shift]
		var all = false
		for i := uint32(1); i < log.majority; i++ {
			all = log.proposals[i] == proposal
			if !all {
				break
			}
		}
		if !all {
			return errors.New("very strange")
		}
		log.indices[current] = 0
		var phase = uint8(0)
		var state uint8
		if all {
			state = 1
		} else {
			state = 0
		}
		for {
			var height = current<<8 | uint16(phase)
			LittleEndian.PutUint16(buffer[0:], current)
			buffer[2] = phase<<2 | state
			states.send(buffer[:3])
		}
	}
}
