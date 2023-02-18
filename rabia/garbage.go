package rabia

type comparator struct {
	compare func(o1, o2 any) int
}

type identifier struct {
	value uint64
}

func (id identifier) Equals(other any) bool {
	return other.(identifier).value == id.value
}

func (comparator *comparator) Compare(o1, o2 any) int {
	return comparator.compare(o1, o2)
}

func comparingUint64(o1, o2 any) int {
	if o1.(uint64) > o2.(uint64) {
		return 1
	} else if o1.(uint64) == o2.(uint64) {
		return 0
	}
	return -1
}

func comparingProposals(o1, o2 any) int {
	var first = comparingUint64(
		o1.(identifier).value&0xFFFFFFFF,
		o2.(identifier).value&0xFFFFFFFF,
	)
	if first != 0 {
		return first
	}
	return comparingUint64(
		o1.(identifier).value>>32,
		o2.(identifier).value>>32,
	)
}
