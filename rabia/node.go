package rabia

import (
	"encoding/binary"
	"errors"
	"fmt"
	"go.uber.org/multierr"
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

	queues      []Queue[uint64]
	messages    map[uint64][]byte
	proposeLock sync.RWMutex

	committed   uint64
	highest     int64
	consumeLock sync.Mutex

	spreadersInbound  []Connection
	spreadersOutbound []Connection
	spreader          Connection
	spreadLock        sync.Mutex

	repairInbound  []Connection
	repairOutbound []Connection
	repairLock     sync.Mutex
	repairIndex    int

	removeLists []map[uint64]uint64
	removeLocks []*sync.Mutex
}

const INFO = false

func MakeNode(address string, addresses []string, pipes ...uint16) (Node, error) {
	//var compare = &Comparator{ComparingProposals}
	var size = uint32((65536 / len(pipes)) * len(pipes))
	var queues = make([]Queue[uint64], len(pipes))
	var removeLists = make([]map[uint64]uint64, len(pipes))
	var removeLocks = make([]*sync.Mutex, len(pipes))
	for i := range queues {
		queues[i] = NewPriorityBlockingQueue[uint64](65536, func(a uint64, b uint64) int {
			return ComparingUint64(a, b)
		})
		removeLists[i] = make(map[uint64]uint64)
		removeLocks[i] = &sync.Mutex{}
	}
	var others []string
	for _, other := range addresses {
		if other != address {
			others = append(others, other)
		}
	}
	spreadersInbound, spreadersOutbound, reason := GroupSet(address, 25565, others...)
	if reason != nil {
		return nil, reason
	}
	repairInbound, repairOutbound, reason := GroupSet(address, 2001, others...)
	if reason != nil {
		return nil, reason
	}
	var log = MakeLog(uint16(len(addresses)), uint16(len(addresses))/4, size)
	return &node{
		log, pipes, addresses, address,
		queues, make(map[uint64][]byte), sync.RWMutex{},
		uint64(0), int64(-1), sync.Mutex{},
		spreadersInbound, spreadersOutbound,
		Multicaster(spreadersOutbound...), sync.Mutex{},
		repairInbound, repairOutbound, sync.Mutex{},
		0, removeLists, removeLocks,
	}, nil
}

func (node *node) Size() uint32 {
	return uint32(len(node.log.Logs))
}

func (node *node) Repair(index uint64) (uint64, []byte, error) {
	node.repairLock.Lock()
	defer node.repairLock.Unlock()
	//println("Trying to repair: ", index)
	var client = node.repairOutbound[node.repairIndex%len(node.repairOutbound)]
	node.repairIndex++
	//println("repairing with: ", client.(connection).Conn.RemoteAddr().String())
	//node.repairIndex++
	var buffer = make([]byte, 8)
	binary.LittleEndian.PutUint64(buffer, index)
	var reason = client.Write(buffer)
	if reason != nil {
		return 0, nil, reason
	}
	var header = make([]byte, 12)
	reason = client.Read(header)
	if reason != nil {
		return 0, nil, reason
	}
	var id = binary.LittleEndian.Uint64(header[0:])
	var amount = binary.LittleEndian.Uint32(header[8:])
	var message = make([]byte, amount)
	reason = client.Read(message)
	if reason != nil {
		return 0, nil, reason
	}
	return id, message, nil
}

func (node *node) enqueue(id uint64, data []byte) {
	var index = id % uint64(len(node.pipes))
	//var index = id >>32%uint64(len(node.queues))
	var lock = node.removeLocks[index]
	var list = node.removeLists[index]
	lock.Lock()
	if list[id] == id {
		delete(list, id)
		lock.Unlock()
		return
	}
	lock.Unlock()
	node.proposeLock.Lock()
	node.messages[id] = data
	node.proposeLock.Unlock()
	//node.queues[index].Offer(Identifier{id})
	node.queues[index].Offer(id)
	//node.queues[id >>32%uint64(len(node.queues))].Offer(Identifier{id})
}

func (node *node) Propose(id uint64, data []byte) error {
	header := make([]byte, 12)
	binary.LittleEndian.PutUint64(header[0:], id)
	binary.LittleEndian.PutUint32(header[8:], uint32(len(data)))
	var send = append(header, data...)
	go func() {
		node.spreadLock.Lock()
		reason := node.spreader.Write(send)
		node.spreadLock.Unlock()
		if reason != nil {
			panic(reason)
		}

	}()
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

		var currentNodeIndex int
		for i, addr := range node.addresses {
			if addr == node.address {
				currentNodeIndex = i
				break
			}
		}
		group.Add(len(node.spreadersOutbound))
		node.spreadLock.Lock()
		for i, connection := range node.spreadersOutbound {
			dataIndex := i
			if i >= currentNodeIndex {
				dataIndex++ // Skip the current node's data
			}
			go func(connection Connection, data []byte) {
				reason := connection.Write(append(header, data...))
				if reason != nil {
					lock.Lock()
					reasons = multierr.Append(reasons, reason)
					lock.Unlock()
				} else {
					group.Done()
				}
			}(connection, data[dataIndex])
		}
		group.Wait()
		node.spreadLock.Unlock()
		node.enqueue(id, data[currentNodeIndex])
	}()
	return nil
}

func (node *node) Run() error {
	var group sync.WaitGroup
	var lock sync.Mutex
	var reasons error
	group.Add(len(node.pipes))
	var log = node.log
	//messages map ig?

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
	var empty = make([]byte, 12)
	for _, inbound := range node.repairInbound {
		go func(connection Connection) {
			var buffer = make([]byte, 8)
			var header = make([]byte, 12)
			for {
				var reason = connection.Read(buffer)
				if reason != nil {
					panic(reason)
				}
				var index = binary.LittleEndian.Uint64(buffer)
				var highest = atomic.LoadInt64(&node.highest)
				//improve this and also what if we have the id but the not message? (possible?)
				if int64(index) <= highest && node.log.Logs[index%uint64(node.log.Size)] != UNKNOWN {
					var id = node.log.Logs[index%uint64(node.log.Size)]
					node.proposeLock.RLock()
					var message = node.messages[id]
					node.proposeLock.RUnlock()
					binary.LittleEndian.PutUint64(header[0:], id)
					binary.LittleEndian.PutUint32(header[8:], uint32(len(message)))
					reason = connection.Write(header)
					if reason != nil {
						panic(reason)
					}
					reason = connection.Write(message)
				} else {
					reason = connection.Write(empty)
				}
				if reason != nil {
					panic(reason)
				}
			}
		}(inbound)
	}

	var currentNodeIndex int
	for i, addr := range node.addresses {
		if addr == node.address {
			currentNodeIndex = i
			break
		}
	}
	//var mark = time.Now().UnixNano()
	for index, pipe := range node.pipes {
		go func(index int, pipe uint16, queue Queue[uint64]) {
			defer group.Done()
			var info = func(format string, a ...interface{}) {
				if INFO {
					fmt.Printf(fmt.Sprintf("[Pipe-%d] %s", index, format), a...)
				}
			}

			var current = uint64(index)
			println("Starting with: ", current)
			proposers, reason := Group(node.address, pipe+1, node.addresses...)
			staters, reason := Group(node.address, pipe+2, node.addresses...)
			voters, reason := Group(node.address, pipe+3, node.addresses...)
			if reason != nil {
				lock.Lock()
				defer lock.Unlock()
				var result = fmt.Errorf("failed to connect %d: %s", index, reason)
				reasons = multierr.Append(reasons, result)
			}
			var proposals = FixedMulticaster(currentNodeIndex, "Proposals", proposers...)
			var states = FixedMulticaster(currentNodeIndex, "States", staters...)
			var votes = FixedMulticaster(currentNodeIndex, "Votes", voters...)
			info("Connected!\n")
			//var three = 0
			var last uint64
			reason = log.SMR(proposals, states, votes, func() (uint16, uint64, error) {
				//if three == 4 {
				//	time.Sleep(60 * time.Second)
				//	println("Entries: ")
				//	for !queue.IsEmpty() {
				//		println(queue.Poll().(Identifier).value)
				//	}
				//}
				//three++
				//time.Sleep(20 * time.Microsecond)
				next, _ := queue.Poll()
				//if next == nil {
				//	println("considering noop ", queue.Size())
				//	time.Sleep(1000 * time.Millisecond)
				//	next = queue.Poll()
				//	if next == nil {
				//		next = Identifier{GIVE_UP}
				//	} else {
				//		println("Second time avoided noop")
				//	}
				//} else {
				//	//println("didn't noop")
				//}
				last = next
				return uint16(current % uint64(log.Size)), last, nil
			}, func(slot uint16, message uint64) error {
				if message != last {
					if last != SKIP {
						queue.Offer(last)
					}
					if message == SKIP {
						println("had to skip")
						println("Proposed: ", last)
						println(queue.String())
						var lock = node.removeLocks[index]
						lock.Lock()
						_, present := node.removeLists[index][last]
						if present {
							panic("Proposed something in the remove list")
						}
						lock.Unlock()
						time.Sleep(500 * time.Millisecond)
					}
					if message < UNKNOWN {
						//println("Removing")
						if !queue.Remove(message) {
							var lock = node.removeLocks[index]
							lock.Lock()
							node.removeLists[index][message] = message
							lock.Unlock()
						}
					}

				}

				//Message cannot be unknown at this point.

				//if message != math.MaxUint64 {/**/
				//	fmt.Printf("[Pipe-%d] %d\n", index, message)
				//}
				log.Logs[current%uint64(log.Size)] = message
				var value = atomic.LoadInt64(&node.highest)
				for value < int64(current) && !atomic.CompareAndSwapInt64(&node.highest, value, int64(current)) {
					value = atomic.LoadInt64(&node.highest)
				}
				var committed = atomic.LoadUint64(&node.committed)
				//have to wait here until the next slot has been consumed
				if current-committed >= uint64(log.Size) {
					println("Wrapping: ", current-committed)
					println("Current: ", current)
					println("Highest: ", atomic.LoadInt64(&node.highest))
					println("Committed: ", committed)
					for current-atomic.LoadUint64(&node.committed) >= uint64(log.Size) {
						time.Sleep(10 * time.Nanosecond)
					}
					println("Thank you! I was turbo wrapping :(")
				}

				current += uint64(len(node.pipes))
				node.proposeLock.Lock()
				delete(node.messages, log.Logs[current%uint64(log.Size)])
				node.proposeLock.Unlock()
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
		if proposal == NONE {
			highest = int64(i)
			println("hit unfilled slot.")
			//if we hit the first unfilled slot stop
			break
		}
		if proposal == SKIP {
			continue
		}
		node.proposeLock.RLock()
		data, present := node.messages[proposal]
		node.proposeLock.RUnlock()
		if !present {
			for {
				id, repaired, _ := node.Repair(i)
				if id != 0 {
					if id != proposal {
						println("ID: ", id)
						println("Proposal: ", proposal)
						panic("SMR HAS FAILED CATASTROPHICALLY!")
					}
					node.proposeLock.Lock()
					node.messages[id] = repaired
					node.proposeLock.Unlock()
					data = repaired
					break
				}
			}
		}
		reason := block(i, proposal, data)
		if reason != nil {
			return reason
		}
	}
	atomic.StoreUint64(&node.committed, uint64(highest))
	return nil
}
