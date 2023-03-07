package rabia

import (
	. "encoding/binary"
	"math"
	"math/rand"
)

const Multiplier = 1
const SizeBuffer = 10 * Multiplier
const SizeProvider = 10 * Multiplier
const SizeVote = 3 * Multiplier
const SizeState = 3 * Multiplier

const NONE = 0
const UNKNOWN = math.MaxUint64 - 1
const SKIP = math.MaxUint64

func IsValid(id uint64) bool {
	return id != 0 && id < UNKNOWN
}

// new = a current = b
// if new is less than current but not by a huge amount then it's old
// if new is greater than current by a huge amount then it's old
func isOld(a uint16, b uint16, half uint16) bool {
	return a < b && (b-a) < half || a > b && (a-b) > half
}

func (log Log) SMR(
	proposes Connection,
	states Connection,
	votes Connection,
	messages func() (uint16, uint64, error),
	commit func(uint16, uint64) error,
	info func(string, ...interface{}),
) error {
	var buffer = make([]byte, SizeBuffer)
	var half = uint16(len(log.Logs) / 2)
	var shift = uint32(math.Floor(math.Log2(float64(log.Majority)))) + 1
outer:
	for {
		current, proposed, reason := messages()
		if reason != nil {
			return reason
		}
		LittleEndian.PutUint16(buffer[0:], current)
		LittleEndian.PutUint64(buffer[2:], proposed)
		reason = proposes.Write(buffer[:SizeProvider])
		if reason != nil {
			return reason
		}
		info("Sent Proposal: %d - %d\n", current, proposed)
		for log.Indices[current] < log.Majority {
			reason := proposes.Read(buffer[:SizeProvider])
			if reason != nil {
				return reason
			}
			var depth = LittleEndian.Uint16(buffer[0:])
			if isOld(depth, current, half) {
				continue
			}
			var proposal = LittleEndian.Uint64(buffer[2:])
			info("Got Proposal (%d/%d): %d - %d\n", log.Indices[depth]+1, log.Majority, depth, proposal)
			var index = log.Indices[depth]
			if index < log.Majority {
				log.Proposals[current<<shift|index] = proposal
				log.Indices[depth]++
			}
		}
		var proposal = log.Proposals[current<<shift]
		var all = false
		for i := uint16(1); i < log.Majority; i++ {
			all = log.Proposals[current<<shift|i] == proposal
			if !all {
				break
			}
		}
		//if !all {
		//	for i := uint16(0); i < log.Majority; i++ {
		//		info("Proposals[%d] = %d\n", i, log.Proposals[current<<shift|i])
		//	}
		//	return errors.New("very strange")
		//}
		log.Indices[current] = 0
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
			reason := states.Write(buffer[:SizeState])
			info("Sent State: %d(%d) - %d\n", current, phase, state)
			if reason != nil {
				return reason
			}
			for log.StatesZero[height]+log.StatesOne[height] < uint8(log.Majority) {
				reason := states.Read(buffer[:SizeState])
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
				var total = log.StatesZero[depth<<8|round] + log.StatesOne[depth<<8|round]
				info("Got State (%d/%d): %d(%d) - %d\n", total+1, log.Majority, depth, round, op)
				if op == 1 {
					log.StatesOne[depth<<8|round]++
				} else {
					log.StatesZero[depth<<8|round]++
				}
			}
			var vote uint8
			if log.StatesOne[height] >= uint8(log.Majority) {
				vote = phase<<2 | 1
			} else if log.StatesZero[height] >= uint8(log.Majority) {
				vote = phase<<2 | 0
			} else {
				vote = phase<<2 | 2
			}
			log.StatesZero[height] = 0
			log.StatesOne[height] = 0
			buffer[2] = vote
			reason = votes.Write(buffer[:SizeVote])
			info("Sent Vote: %d(%d) - %d\n", current, phase, vote)
			if reason != nil {
				return reason
			}
			for log.VotesZero[height]+log.VotesOne[height]+log.VotesLost[height] < uint8(log.Majority) {
				reason := votes.Read(buffer[:SizeVote])
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
				var total = log.VotesZero[depth<<8|round] + log.VotesOne[depth<<8|round] + log.VotesLost[depth<<8|round]
				info("Got Vote (%d/%d): %d(%d) - %d\n", total+1, log.Majority, depth, round, op)
				if op == 1 {
					log.VotesOne[depth<<8|round]++
				} else if op == 0 {
					log.VotesZero[depth<<8|round]++
				} else {
					log.VotesLost[depth<<8|round]++
				}
			}
			var zero = log.VotesZero[height]
			var one = log.VotesOne[height]
			log.VotesZero[height] = 0
			log.VotesOne[height] = 0
			log.VotesLost[height] = 0

			if one >= uint8(log.F+1) {
				if all {
					reason = commit(current, proposal)
					if reason != nil {
						return reason
					}
				} else {
					//Commit a value that will never appear naturally
					//this will force a repair on the slot to get the value.
					reason = commit(current, UNKNOWN)
					if reason != nil {
						return reason
					}
				}
			} else if zero >= uint8(log.F+1) {
				reason = commit(current, SKIP)
				if reason != nil {
					return reason
				}
			} else {
				phase++
				if one > 0 {
					state = phase<<2 | 1
				} else if zero > 0 {
					state = phase<<2 | 0
				} else {
					var random = rand.New(rand.NewSource(int64(height))).Intn(2)
					state = phase<<2 | uint8(random)
				}
				continue
			}
			continue outer
		}
	}
}
