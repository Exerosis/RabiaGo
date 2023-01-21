package rabia

type Log struct {
	size     uint32
	n        uint16
	f        uint16
	majority uint16

	logs []uint64

	indices   []uint16
	proposals []uint64

	statesZero []uint8
	statesOne  []uint8

	votesZero []uint8
	votesOne  []uint8
	votesLost []uint8
}

func makeLog(n uint16, size uint32) *Log {
	var majority = (n / 2) + 1
	return &Log{
		size, n, n / 2, majority,
		make([]uint64, size),
		make([]uint16, size),
		make([]uint64, uint32(majority)*size),
		make([]uint8, size*256),
		make([]uint8, size*256),
		make([]uint8, size*256),
		make([]uint8, size*256),
		make([]uint8, size*256),
	}
}
