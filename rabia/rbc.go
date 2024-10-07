package rabia

import (
	. "encoding/binary"
	"math"
	"math/bits"
	"math/rand"
	"sync/atomic"
)

const OP_PROPOSE = byte(0)
const OP_STATE_0 = byte(1)
const OP_STATE_1 = byte(2)
const OP_VOTE_0 = byte(3)
const OP_VOTE_1 = byte(4)
const OP_VOTE_2 = byte(5)
const MASK = uint64(0b1111_1111)
const SHIFT_STATE_0 = 1 * 8
const SHIFT_STATE_1 = 2 * 8
const SHIFT_VOTE_0 = 3 * 8
const SHIFT_VOTE_1 = 4 * 8
const SHIFT_STATE = 5 * 8
const SHIFT_VOTE = 6 * 8

var INCREMENTS = []uint64{0,
	(1 << SHIFT_STATE_0) + (1 << SHIFT_STATE),
	(1 << SHIFT_STATE_1) + (1 << SHIFT_STATE),
	(1 << SHIFT_VOTE_0) + (1 << SHIFT_VOTE),
	(1 << SHIFT_VOTE_1) + (1 << SHIFT_VOTE),
}

func (log *Log2) RBC(
	connection Connection,
	index uint16,
	write func([]byte),
	messages func() (uint16, uint64, error),
	commit func(uint16, uint64) error,
	info func(string, ...interface{}),
) error {
	var buffer = make([]byte, 11)
	var currentSlot = uint16(0)
	var half = uint16(len(log.Logs) / 2)
	var shift = uint32(math.Floor(math.Log2(float64(log.N)))) + 1
read:
	for {
		err := connection.Read(buffer)
		if err != nil {
			return err
		}
		var depth = LittleEndian.Uint16(buffer[1:])
		if isOld(depth, currentSlot, half) {
			continue
		}
		var proposal = LittleEndian.Uint64(buffer[3:])
		var operation = buffer[0]
		var code = operation & 0b111
		if code == OP_PROPOSE {
			info("Got Proposal (%d/%d): %d - %d\n", index+1, log.N-log.F, depth, proposal)
			log.Proposals[depth<<shift|index] = proposal
			var current = atomic.AddUint64(&log.Indices[depth], 1<<index)
			var count = uint16(bits.OnesCount64(current))
			if count == log.Majority {
				//got enough proposals iterate them and what not.
				var highest = uint16(0)
				count = 0
				for i := uint16(0); i < log.N; i++ {
					if current & ^(1<<i) != 1<<i {
						continue
					}
					var proposed = log.Proposals[depth<<shift|i]
					if proposed == proposal {
						highest++
					} else {
						count = 1
						for j := uint16(0); j < i; j++ {
							if log.Proposals[depth<<shift|j] == proposed {
								count++
							}
						}
						if count > highest {
							proposal = proposed
							highest = count
						}
					}
				}
				info("Loop Found Majority: %dx %d\n", highest, proposal)
				if highest >= log.Majority {
					buffer[0] = OP_STATE_1
				} else {
					buffer[0] = OP_STATE_0
				}
				write(buffer)
			}
		} else {
			var round = uint16(operation >> 3)
			var pointer = &log.StatesVotesPhases[depth]
			for {
				var current = atomic.LoadUint64(pointer)
				var phase = uint16(current & MASK)
				if round < phase {
					continue read
				}
				if round == phase || atomic.CompareAndSwapUint64(pointer, current, uint64(round)) {
					break
				}
			}

			var result = atomic.AddUint64(pointer, INCREMENTS[code])
			if code < 3 { //is state
				if (result>>SHIFT_VOTE)&MASK > uint64(log.N-log.F) {
					if result>>SHIFT_STATE_1 >= uint64(log.Majority) {
						buffer[0] = OP_VOTE_1 | byte(round)<<3
					} else if result>>SHIFT_STATE_0 >= uint64(log.Majority) {
						buffer[1] = OP_VOTE_0 | byte(round)<<3
					} else {
						buffer[2] = OP_VOTE_2 | byte(round)<<3
					}
				}
				write(buffer)
			} else if (result>>SHIFT_STATE)&MASK > uint64(log.N-log.F) {
				var one = result >> SHIFT_STATE_1 & MASK
				var zero = result >> SHIFT_STATE_0 & MASK
				if one >= uint64(log.F+1) {
					err = commit(depth, proposal)
					if err != nil {
						return err
					}
					depth, proposal, err = messages()
					if err != nil {
						return err
					}
					buffer[0] = OP_PROPOSE
					LittleEndian.PutUint16(buffer[1:], depth)
					LittleEndian.PutUint64(buffer[3:], proposal)
				} else if zero >= uint64(log.F+1) {
					err = commit(currentSlot, SKIP)
					if err != nil {
						return err
					}
					depth, proposal, err = messages()
					if err != nil {
						return err
					}
					buffer[0] = OP_PROPOSE
					LittleEndian.PutUint16(buffer[1:], depth)
					LittleEndian.PutUint64(buffer[3:], proposal)
				} else if one > 0 {
					buffer[0] = OP_STATE_1 | byte(round+1)<<3
				} else if zero > 0 {
					buffer[0] = OP_STATE_0 | byte(round+1)<<3
				} else {
					var random = rand.New(rand.NewSource(int64(depth<<3 | round))).Intn(2)
					if random == 1 {
						buffer[0] = OP_STATE_1 | byte(round+1)<<3
					} else {
						buffer[0] = OP_STATE_0 | byte(round+1)<<3
					}
				}
				write(buffer)
			}
		}
	}
}
