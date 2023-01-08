package main

import (
	. "encoding/binary"
	"errors"
	"math"
	"math/rand"
)

// new = a current = b
// if new is less than current but not by a huge amount then it's old
// if new is greater than current by a huge amount then it's old
func isOld(a uint16, b uint16, half uint16) bool {
	return a < b && (b-a) < half || a > b && (a-b) > half
}

func (log Log) SMR(
	proposes *TcpMulticaster,
	states *TcpMulticaster,
	votes *TcpMulticaster,
	messages func() (uint16, uint64),
	commit func(uint16, uint64),
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
			state = phase<<2 | 1
		} else {
			state = phase<<2 | 0
		}
		for {
			var height = current<<8 | uint16(phase)
			LittleEndian.PutUint16(buffer[0:], current)
			buffer[2] = state
			reason := states.send(buffer[:3])
			if reason != nil {
				return reason
			}
			for log.statesZero[height]+log.statesOne[height] < uint8(log.majority) {
				reason := states.receive(buffer[:3])
				if reason != nil {
					return reason
				}
				var depth = LittleEndian.Uint16(buffer[0:])
				if isOld(depth, current, half) {
					continue
				}
				var round = uint16(buffer[2] >> 2)
				if isOld(round, uint16(phase), 32) {
					continue
				}
				if buffer[2]&3 == 1 {
					log.statesOne[depth<<8|round]++
				} else {
					log.statesZero[depth<<8|round]++
				}
			}
			var vote uint8
			if log.statesOne[height] >= uint8(log.majority) {
				vote = phase<<2 | 1
			} else if log.statesZero[height] >= uint8(log.majority) {
				vote = phase<<2 | 0
			} else {
				vote = phase<<2 | 2
			}
			log.statesZero[height] = 0
			log.statesOne[height] = 0
			buffer[2] = vote
			reason = votes.send(buffer[:3])
			if reason != nil {
				return reason
			}
			for log.votesZero[height]+log.votesOne[height]+log.votesLost[height] < uint8(log.majority) {
				reason := states.receive(buffer[:3])
				if reason != nil {
					return reason
				}
				var depth = LittleEndian.Uint16(buffer[0:])
				if isOld(depth, current, half) {
					continue
				}
				var round = uint16(buffer[2] >> 2)
				if isOld(round, uint16(phase), 32) {
					continue
				}
				var op = buffer[2] & 3
				if op == 1 {
					log.votesOne[depth<<8|round]++
				} else if op == 0 {
					log.votesZero[depth<<8|round]++
				} else {
					log.votesLost[depth<<8|round]++
				}
			}
			var zero = log.votesZero[height]
			var one = log.votesOne[height]
			log.votesZero[height] = 0
			log.votesOne[height] = 0
			log.votesLost[height] = 0

			if one >= uint8(log.f+1) {
				if all {
					commit(current, proposal)
				} else {
					commit(current, 0)
				}
			} else if zero >= uint8(log.f+1) {
				commit(current, math.MaxUint64)
			} else {
				phase++
				if one > 0 {
					state = phase<<2 | 1
				} else if zero > 0 {
					state = phase<<2 | 0
				} else {
					rand.Seed(int64(height))
					state = phase<<2 | uint8(rand.Intn(2))
				}
			}
		}
	}
	return nil
}
