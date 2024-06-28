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

type Node interface {
	Size() uint32
	Propose(id uint64, data []byte) error
	ProposeEach(id uint64, data [][]byte) error
	Run() error
	Repair(index uint64) (uint64, []byte, error)
	Consume(block func(uint64, uint64, []byte) error) error
}
type node struct {
	log *Log

	pipes     []uint16
	addresses []string
	address   string

	queues   []Queue[uint64]
	messages *BlockingMap[uint64, []byte]

	committed   uint64
	highest     int64
	consumeLock sync.Mutex
	index       int

	spreadersInbound  []Connection
	spreadersOutbound []Connection
	spreader          Connection
	spreadLock        sync.Mutex

	removeLists []map[uint64]uint64
	removeLocks []*sync.Mutex
}

const INFO = false

func MakeNode(address string, addresses []string, f uint16, pipes ...uint16) (Node, error) {
	sort.Sort(sort.StringSlice(addresses))
	var size = uint32((65536 / len(pipes)) * len(pipes))
	var queues = make([]Queue[uint64], len(pipes))
	var removeLists = make([]map[uint64]uint64, len(pipes))
	var removeLocks = make([]*sync.Mutex, len(pipes))
	for i := range queues {
		queues[i] = NewPriorityBlockingQueue[uint64](65536, func(a uint64, b uint64) int {
			return ComparingProposals(a, b)
		})
		removeLists[i] = make(map[uint64]uint64)
		removeLocks[i] = &sync.Mutex{}
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
	spreadersInbound, spreadersOutbound, reason := GroupSet(address, 25565, others...)
	if reason != nil {
		return nil, reason
	}
	var log = MakeLog(uint16(len(addresses)), f, size)
	return &node{
		log, pipes, addresses, address,
		queues, NewBlockingMap[uint64, []byte](),
		uint64(0), int64(-1), sync.Mutex{}, index,
		spreadersInbound, spreadersOutbound,
		Multicaster(spreadersOutbound...), sync.Mutex{},
		removeLists, removeLocks,
	}, nil
}

func (node *node) Size() uint32 {
	return uint32(len(node.log.Logs))
}

func (node *node) Repair(index uint64) (uint64, []byte, error) {
	return 0, nil, nil
}

func (node *node) enqueue(id uint64, data []byte) {
	var index = id % uint64(len(node.pipes))
	var lock = node.removeLocks[index]
	var list = node.removeLists[index]
	node.messages.Set(id, data)
	lock.Lock()
	if list[id] == id {
		delete(list, id)
		lock.Unlock()
		return
	}
	node.queues[index].Offer(id)
	lock.Unlock()
}

func (node *node) Propose(id uint64, data []byte) error {
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

func (node *node) ProposeEach(id uint64, data [][]byte) error {
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

func (node *node) Run() error {
	var group sync.WaitGroup
	var lock sync.Mutex
	var reasons error
	group.Add(len(node.pipes))
	var log = node.log

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
			fmt.Println("Well at least I'm trying lol!")
			defer group.Done()
			var info = func(format string, a ...interface{}) {}
			if INFO {
				info = func(format string, a ...interface{}) {
					fmt.Printf(fmt.Sprintf("[Pipe-%d] %s", index, format), a...)
				}
			}

			var current = uint64(index)
			proposers, reason := Group(node.address, pipe+1, node.addresses...)
			staters, reason := Group(node.address, pipe+2, node.addresses...)
			voters, reason := Group(node.address, pipe+3, node.addresses...)
			if reason != nil {
				lock.Lock()
				defer lock.Unlock()
				var result = fmt.Errorf("failed to connect %d: %s", index, reason)
				reasons = multierr.Append(reasons, result)
			}
			var proposals = FixedMulticaster(node.index, fmt.Sprintf("Proposals Pipe [%d]", index), proposers...)
			var states = FixedMulticaster(node.index, fmt.Sprintf("States Pipe [%d]", index), staters...)
			var votes = FixedMulticaster(node.index, fmt.Sprintf("Votes Pipe [%d]", index), voters...)
			info("Connected!\n")
			var last uint64
			reason = log.SMR(proposals, states, votes, func() (uint16, uint64, error) {
				next, present := queue.Poll()
				if !present {
					last = SKIP
				} else {
					last = next
				}
				return uint16(current % uint64(log.Size)), last, nil
			}, func(slot uint16, message uint64) error {
				if message != last {
					if last < SKIP {
						queue.Offer(last)
					}
					if message < SKIP {
						var lock = node.removeLocks[index]
						lock.Lock()
						if !queue.Remove(message) {
							node.removeLists[index][message] = message
						}
						lock.Unlock()
					}
				}
				log.Logs[current%uint64(log.Size)] = message
				var value = atomic.LoadInt64(&node.highest)
				for value < int64(current) && !atomic.CompareAndSwapInt64(&node.highest, value, int64(current)) {
					value = atomic.LoadInt64(&node.highest)
				}
				var committed = atomic.LoadUint64(&node.committed)
				//have to wait here until the next slot has been consumed
				if current-committed >= uint64(log.Size) {
					for current-atomic.LoadUint64(&node.committed) >= uint64(log.Size) {
						time.Sleep(10 * time.Nanosecond)
					}
					println("Thank you! I was turbo wrapping :(")
				}

				current += uint64(len(node.pipes))
				//node.messages.Delete(log.Logs[current%uint64(log.Size)])
				log.Logs[current%uint64(log.Size)] = NONE
				return nil
			}, info)
			if reason != nil {
				lock.Lock()
				defer lock.Unlock()
				var result = fmt.Errorf("running smr pipe %d: %s", index, reason)
				reasons = result
			}
			return
		}(index, pipe, node.queues[index])
	}
	group.Wait()
	return reasons
}

func (node *node) Consume(block func(uint64, uint64, []byte) error) error {
	node.consumeLock.Lock()
	defer node.consumeLock.Unlock()
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
