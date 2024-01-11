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

	var count = uint16(0)
	var highest = uint16(0)

	var phase = uint8(0)
	var state uint8
	var vote uint8
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
		for log.Indices[current] < log.N-log.F {
			reason := proposes.Read(buffer[:SizeProvider])
			if reason != nil {
				return reason
			}
			var depth = LittleEndian.Uint16(buffer[0:])
			if isOld(depth, current, half) {
				continue
			}
			var proposal = LittleEndian.Uint64(buffer[2:])
			var index = log.Indices[depth]

			info("Got Proposal (%d/%d): %d - %d\n", index+1, log.N-log.F, depth, proposal)
			log.Proposals[current<<shift|index] = proposal
			log.Indices[depth] = index + 1
		}

		highest = 0
		for i := uint16(0); i < log.N-log.F; i++ {
			var proposal = log.Proposals[current<<shift|i]
			if proposal == proposed {
				highest++
			} else {
				count = 0
				for j := uint16(0); j < i; j++ {
					if log.Proposals[current<<shift|j] == proposal {
						count++
					}
				}
				if count > highest {
					proposed = proposal
					highest = count
				}
			}
		}
		println("Highest: ", highest)
		log.Indices[current] = 0

		phase = 0
		if count >= log.Majority {
			state = 1
		} else {
			state = 0
		}
		if count == 0 || count == log.N-log.F {
			goto cleanup
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
			for log.StatesZero[height]+log.StatesOne[height] < uint8(log.N-log.F) {
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
			for log.VotesZero[height]+log.VotesOne[height]+log.VotesLost[height] < uint8(log.N-log.F) {
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

			phase++
			if one >= uint8(log.Majority) {
				reason = commit(current, proposed)
				if reason != nil {
					return reason
				}
				state = phase<<2 | 1
				goto cleanup
			}
			if zero >= uint8(log.Majority) {
				reason = commit(current, SKIP)
				if reason != nil {
					return reason
				}
				state = phase<<2 | 0
				goto cleanup
			}
			if one > 0 {
				state = phase<<2 | 1
			} else if zero > 0 {
				state = phase<<2 | 0
			} else {
				var random = rand.New(rand.NewSource(int64(height))).Intn(2)
				state = phase<<2 | uint8(random)
			}
		}
	cleanup:
		buffer[2] = state
		reason = states.Write(buffer[:SizeState])
		info("Sent Cleanup State: %d(%d) - 1\n", current, phase)
		if reason != nil {
			return reason
		}
		reason = votes.Write(buffer[:SizeVote])
		info("Sent Cleanup Vote: %d(%d) - 1\n", current, phase)
		if reason != nil {
			return reason
		}
	}
}
