package rabia

type Log struct {
	Size     uint32
	N        uint16
	F        uint16
	Majority uint16

	Logs []uint64

	Indices   []uint16
	Proposals []uint64

	StatesZero []uint8
	StatesOne  []uint8

	VotesZero []uint8
	VotesOne  []uint8
	VotesLost []uint8
}

func MakeLog(n uint16, f uint16, size uint32) *Log {
	return &Log{
		size, n, f, (n / 2) + 1,
		make([]uint64, size),
		make([]uint16, size),
		make([]uint64, uint32(n)*size),
		make([]uint8, size*256),
		make([]uint8, size*256),
		make([]uint8, size*256),
		make([]uint8, size*256),
		make([]uint8, size*256),
	}
}
