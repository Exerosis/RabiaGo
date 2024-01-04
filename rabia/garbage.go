package rabia

import (
	"fmt"
	"runtime"
	"strconv"
	"strings"
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

func ComparingUint64(o1, o2 any) int {
	if o1.(uint64) > o2.(uint64) {
		return -1
	} else if o1.(uint64) == o2.(uint64) {
		return 0
	}
	return 1
}

func ComparingProposals(o1, o2 any) int {
	var first = ComparingUint64(
		o1.(Identifier).Value&0xFFFFFFFF,
		o2.(Identifier).Value&0xFFFFFFFF,
	)
	if first != 0 {
		return first
	}
	return ComparingUint64(
		o1.(Identifier).Value>>32,
		o2.(Identifier).Value>>32,
	)
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
