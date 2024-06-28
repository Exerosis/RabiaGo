package rabia

import (
	"fmt"
	"math/rand"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type Comparator struct {
	Comparison func(o1, o2 any) int
}

type Identifier struct {
	Value uint64
}

func (id Identifier) Equals(other any) bool {
	return other.(Identifier).Value == id.Value
}

func (comparator *Comparator) Compare(o1, o2 any) int {
	return comparator.Comparison(o1, o2)
}

func ComparingUint64(a, b uint64) int {
	if a > b {
		return -1
	} else if a == b {
		return 0
	}
	return 1
}

func ComparingProposals(a, b uint64) int {
	// First, compare the timestamps (lower 32 bits)
	var first = ComparingUint64(
		a&0xFFFFFFFF,
		b&0xFFFFFFFF,
	)
	if first != 0 {
		return first
	}
	// If timestamps are equal, compare the random parts (higher 32 bits)
	return ComparingUint64(
		a>>32,
		b>>32,
	)
}

func RandomProposal() uint64 {
	var id uint64
	for !IsValid(id) {
		//Designed to wrap roughly every 5 minutes, this gives us
		//5 minutes to reach consensus on each element before
		//performance is compromised.
		var stamp = uint64((time.Now().UnixNano() / 1000) / 14 & 0xFFFFFFFF)

		//Then add 32 bits of random in case we need to break ties.
		id = (uint64(rand.Uint32()) << 32) | stamp
	}
	return id
}

func GoroutineId() int {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	idField := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))[0]
	id, err := strconv.Atoi(idField)
	if err != nil {
		panic(fmt.Sprintf("cannot get goroutine id: %v", err))
	}
	return id
}
