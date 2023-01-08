package main

type Log struct {
	n        uint32
	f        uint32
	majority uint32

	logs []uint64

	indices   []uint32
	proposals []uint64

	statesZero []uint8
	statesOne  []uint8

	votesZero []uint8
	votesOne  []uint8
	votesLost []uint8
}

func makeLog(n uint32, size uint32) *Log {
	var majority = (n / 2) + 1
	return &Log{
		n, n / 2, majority,
		make([]uint64, size),
		make([]uint32, size),
		make([]uint64, majority*size),
		make([]uint8, size*256),
		make([]uint8, size*256),
		make([]uint8, size*256),
		make([]uint8, size*256),
		make([]uint8, size*256),
	}
}
