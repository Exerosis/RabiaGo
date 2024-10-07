package rabia

import (
	"encoding/binary"
	"errors"
	"fmt"
	"go.uber.org/multierr"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Log2 struct {
	Size     uint32
	N        uint16
	F        uint16
	Majority uint16

	Logs []uint64

	Indices   []uint64
	Proposals []uint64

	StatesVotesPhases []uint64
}

func MakeLog2(n uint16, f uint16, size uint32) *Log2 {
	return &Log2{
		size, n, f, (n / 2) + 1,
		make([]uint64, size),
		make([]uint64, size),
		make([]uint64, uint32(n)*size),
		make([]uint64, size),
	}
}

type node2 struct {
	log       *Log2
	pipes     []uint16
	addresses []string
	address   string

	messages   *BlockingMap[uint64, []byte]
	removeList map[uint64]uint64
	removeLock sync.Mutex

	queues []Queue[uint64]

	committed   uint64
	highest     int64
	consumeLock sync.Mutex
	index       int

	spreadersInbound  []Connection
	spreadersOutbound []Connection
	spreader          Connection
	spreadLock        sync.Mutex
}

func MakeNode2(address string, addresses []string, f uint16, pipes ...uint16) (Node, error) {
	sort.Sort(sort.StringSlice(addresses))
	var size = uint32((65536 / len(pipes)) * len(pipes))
	var queues = make([]Queue[uint64], len(pipes))
	var removeList = make(map[uint64]uint64)

	for i := range queues {
		queues[i] = NewPriorityBlockingQueue[uint64](65536, func(a uint64, b uint64) int {
			return ComparingProposals(a, b)
		})
	}
	var index = 0
	var others []string
	for i, other := range addresses {
		if other != address {
			others = append(others, other)
		} else {
			index = i
		}
	}
	spreadersInbound, spreadersOutbound, err := GroupSet(address, 25565, others...)
	if err != nil {
		return nil, err
	}
	var log = MakeLog2(uint16(len(addresses)), f, size)
	return &node2{
		log, pipes, addresses, address,
		NewBlockingMap[uint64, []byte](),
		removeList,
		sync.Mutex{},
		queues,
		uint64(0), int64(-1), sync.Mutex{}, index,
		spreadersInbound, spreadersOutbound,
		Multicaster(spreadersOutbound...), sync.Mutex{},
	}, nil
}

func (node *node2) enqueue(id uint64, data []byte) {
	var index = id % uint64(len(node.pipes))
	node.removeLock.Lock()
	if node.removeList[id] == id {
		delete(node.removeList, id)
		node.removeLock.Unlock()
		return
	}
	node.removeLock.Unlock()
	node.messages.Set(id, data)
	node.queues[index].Offer(id)
}

func (node *node2) Propose(id uint64, data []byte) error {
	header := make([]byte, 12)
	binary.LittleEndian.PutUint64(header[0:], id)
	binary.LittleEndian.PutUint32(header[8:], uint32(len(data)))
	var send = append(header, data...)
	node.spreadLock.Lock()
	reason := node.spreader.Write(send)
	node.spreadLock.Unlock()
	if reason != nil {
		panic(reason)
	}
	node.enqueue(id, data)
	return nil
}

func (node *node2) ProposeEach(id uint64, data [][]byte) error {
	if len(data) != len(node.addresses) {
		return errors.New("not enough data segements to split")
	}
	header := make([]byte, 12)
	binary.LittleEndian.PutUint64(header[0:], id)
	binary.LittleEndian.PutUint32(header[8:], uint32(len(data[0])))
	go func() {
		var group sync.WaitGroup
		var lock sync.Mutex
		var reasons error

		group.Add(len(node.spreadersOutbound))
		node.spreadLock.Lock()
		for i, connection := range node.spreadersOutbound {
			dataIndex := i
			if i >= node.index {
				dataIndex++ // Skip the current node's data
			}
			go func(connection Connection, data []byte, index int) {
				reason := connection.Write(append(header, data...))
				if reason != nil {
					lock.Lock()
					reasons = multierr.Append(reasons, reason)
					lock.Unlock()
				} else {
					group.Done()
				}
			}(connection, data[dataIndex], i)
		}
		group.Wait()
		node.spreadLock.Unlock()
		node.enqueue(id, data[node.index])
	}()
	return nil
}

func (node *node2) Run() error {
	for _, inbound := range node.spreadersInbound {
		go func(inbound Connection) {
			var header = make([]byte, 12)
			for {
				reason := inbound.Read(header)
				if reason != nil {
					panic(reason)
				}
				var id = binary.LittleEndian.Uint64(header[0:])
				var data = make([]byte, binary.LittleEndian.Uint32(header[8:]))
				reason = inbound.Read(data)
				if reason != nil {
					panic(reason)
				}
				node.enqueue(id, data)
			}
		}(inbound)
	}
	for index, pipe := range node.pipes {
		go func(index int, pipe uint16, queue Queue[uint64]) {
			var info = func(format string, a ...interface{}) {}
			if INFO {
				info = func(format string, a ...interface{}) {
					fmt.Printf(fmt.Sprintf("[Pipe-%d] %s", index, format), a...)
				}
			}

			var current = uint64(index)
			connections, err := Group(node.address, 25566, node.addresses...)
			if err != nil {
				panic(err)
			}
			info("Connected!\n")
			var last uint64
			var messages = func() (uint16, uint64, error) {
				next, present := queue.Poll()
				if !present {
					last = SKIP
				} else {
					last = next
				}
				return uint16(current % uint64(node.log.Size)), last, nil
			}
			var commit = func(slot uint16, message uint64) error {
				if message != last {
					if last < SKIP {
						queue.Offer(last)
					}
					if message < SKIP {
						//lock order matters here IIRC
						node.removeLock.Lock()
						if !queue.Remove(message) {
							node.removeList[message] = message
						}
						node.removeLock.Unlock()
					}
				}
				node.log.Logs[current%uint64(node.log.Size)] = message
				var value = atomic.LoadInt64(&node.highest)
				for value < int64(current) && !atomic.CompareAndSwapInt64(&node.highest, value, int64(current)) {
					value = atomic.LoadInt64(&node.highest)
				}
				var committed = atomic.LoadUint64(&node.committed)
				//have to wait here until the next slot has been consumed
				if current-committed >= uint64(node.log.Size) {
					for current-atomic.LoadUint64(&node.committed) >= uint64(node.log.Size) {
						time.Sleep(10 * time.Nanosecond)
					}
					println("Thank you! I was turbo wrapping :(")
				}

				current += uint64(len(node.pipes))
				//node.messages.Delete(log.Logs[current%uint64(log.Size)])
				node.log.Logs[current%uint64(node.log.Size)] = NONE
				return nil
			}
			var done atomic.Uint32
			var write = func(buffer []byte) {
				//this really kinda sucks :\
				copied := make([]byte, len(buffer))
				copy(copied, buffer)
				var group sync.WaitGroup
				done.Store(0)
				group.Add(1)
				for i, c := range connections {
					go func(i int, c Connection) {
						_ = c.Write(copied)
						if done.Add(0) == uint32(node.log.N-node.log.F) {
							group.Done()
						}
					}(i, c)
				}
				group.Wait()
			}
			for i, connection := range connections {
				err := node.log.RBC(connection, uint16(i), write, messages, commit, info)
				if err != nil {
					panic(err)
				}
			}
		}(index, pipe, node.queues[index])
	}
	return nil
}

func (node *node2) Consume(block func(uint64, uint64, []byte) error) error {
	var highest = atomic.LoadInt64(&node.highest)
	for i := atomic.LoadUint64(&node.committed) + 1; int64(i) <= highest; i++ {
		var slot = i % uint64(len(node.log.Logs))
		var proposal = node.log.Logs[slot]
		if proposal == SKIP {
			continue
		}
		if proposal == NONE {
			highest = int64(i)
			break
		}
		var data = node.messages.WaitFor(proposal)
		reason := block(i, proposal, data)
		if reason != nil {
			return reason
		}
	}
	atomic.StoreUint64(&node.committed, uint64(highest))
	return nil
}

func (node *node2) Repair(_ uint64) (uint64, []byte, error) {
	return 0, nil, nil
}
func (node *node2) Size() uint32 {
	return uint32(len(node.log.Logs))
}
