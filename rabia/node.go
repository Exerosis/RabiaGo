package rabia

import (
	"encoding/binary"
	"fmt"
	"github.com/better-concurrent/guc"
	"go.uber.org/multierr"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type Node interface {
	Size() uint32
	Propose(id uint64, data []byte) error
	Run(address string) error
	Consume(block func(uint64, uint64, []byte) error) error
}
type node struct {
	log *Log

	pipes     []uint16
	addresses []string

	queues      []*guc.PriorityBlockingQueue
	messages    map[uint64][]byte
	proposeLock sync.RWMutex

	committed  uint64
	highest    int64
	commitLock sync.Mutex

	spreader   *TcpMulticaster
	spreadLock sync.Mutex
}

const INFO = false

func MakeNode(addresses []string, pipes ...uint16) Node {
	var compare = &Comparator{ComparingProposals}
	var size = uint32((65536 / len(pipes)) * len(pipes))
	var queues = make([]*guc.PriorityBlockingQueue, len(pipes))
	for i := range queues {
		queues[i] = guc.NewPriorityBlockingQueueWithComparator(compare)
	}
	return &node{
		MakeLog(uint16(len(addresses)), size), pipes, addresses,
		queues, make(map[uint64][]byte), sync.RWMutex{},
		uint64(0), int64(-1), sync.Mutex{},
		nil, sync.Mutex{},
	}
}

func (node *node) Size() uint32 {
	return uint32(len(node.log.Logs))
}

func (node *node) Propose(id uint64, data []byte) error {
	header := make([]byte, 12)
	binary.LittleEndian.PutUint64(header[0:], id)
	binary.LittleEndian.PutUint32(header[8:], uint32(len(data)))
	var send = append(header, data...)
	for node.spreader == nil {
		time.Sleep(time.Millisecond)
	}
	go func() {
		node.spreadLock.Lock()
		defer node.spreadLock.Unlock()
		reason := node.spreader.Send(send)
		if reason != nil {
			panic(reason)
		}
		node.proposeLock.Lock()
		node.messages[id] = data
		node.proposeLock.Unlock()
		node.queues[id>>32%uint64(len(node.queues))].Offer(Identifier{id})
	}()
	return nil
}

func (node *node) Run(
	address string,
) error {
	var group sync.WaitGroup
	var lock sync.Mutex
	var reasons error
	group.Add(len(node.pipes))
	var log = node.log
	//messages map ig?

	var others []string
	for _, other := range node.addresses {
		if other != address {
			others = append(others, other)
		}
	}
	spreader, reason := TCP(address, 2000, others...)
	if reason != nil {
		return reason
	}
	node.spreader = spreader
	for _, inbound := range spreader.Inbound {
		go func(inbound net.Conn) {
			for {
				var fill = func(buffer []byte) {
					for i := 0; i < len(buffer); {
						amount, reason := inbound.Read(buffer)
						if reason != nil {
							panic(reason)
						}
						i += amount
					}
				}
				var header = make([]byte, 12)
				fill(header)
				var id = binary.LittleEndian.Uint64(header[0:])
				var data = make([]byte, binary.LittleEndian.Uint32(header[8:]))
				fill(data)
				node.proposeLock.Lock()
				node.messages[id] = data
				node.proposeLock.Unlock()
				node.queues[id>>32%uint64(len(node.queues))].Offer(Identifier{id})
			}
		}(inbound)
	}

	//var mark = time.Now().UnixNano()
	for index, pipe := range node.pipes {
		go func(index int, pipe uint16, queue *guc.PriorityBlockingQueue) {
			defer group.Done()
			var info = func(format string, a ...interface{}) {
				if INFO {
					fmt.Printf(fmt.Sprintf("[Pipe-%d] %s", index, format), a...)
				}
			}

			var current = uint64(index)
			proposals, reason := TCP(address, pipe+1, node.addresses...)
			states, reason := TCP(address, pipe+2, node.addresses...)
			votes, reason := TCP(address, pipe+3, node.addresses...)
			if reason != nil {
				lock.Lock()
				defer lock.Unlock()
				var result = fmt.Errorf("failed to connect %d: %s", index, reason)
				reasons = multierr.Append(reasons, result)
			}
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
				var next = queue.Poll()
				if next == nil {
					//println("considering noop ", queue.Size())
					time.Sleep(50 * time.Nanosecond)
					next = queue.Poll()
					if next == nil {
						next = Identifier{NO_OP}
					} else {
						println("Second time avoided noop")
					}
				} else {
					//println("didn't noop")
				}
				last = next.(Identifier).Value
				return uint16(current % uint64(log.Size)), last, nil
			}, func(slot uint16, message uint64) error {
				if message == SKIP {
					if message != NO_OP {
						queue.Offer(Identifier{last})
					}
					return nil
				}
				if message == UNKNOWN {
					panic("We can't recover from this without repair")
				}
				if message != last {
					if queue.Remove(Identifier{message}) {
						println("Removed one!")
					}
				}
				//if message != math.MaxUint64 {
				//	fmt.Printf("[Pipe-%d] %d\n", index, message)
				//}
				log.Logs[current%uint64(log.Size)] = message
				var value = atomic.LoadInt64(&node.highest)
				for value < int64(current) && !atomic.CompareAndSwapInt64(&node.highest, value, int64(current)) {
					value = atomic.LoadInt64(&node.highest)
				}
				current += uint64(len(node.pipes))
				var committed = atomic.LoadUint64(&node.committed)
				if current-committed >= uint64(log.Size) {
					panic("WRAPPED TOO HARD!")
				}
				log.Logs[current%uint64(log.Size)] = 0
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
	fmt.Println("Exiting finally!")
	return reasons
}

func (node *node) Consume(block func(uint64, uint64, []byte) error) error {
	node.commitLock.Lock()
	defer node.commitLock.Unlock()
	var highest = atomic.LoadInt64(&node.highest)
	for i := atomic.LoadUint64(&node.committed); int64(i) <= highest; i++ {
		var slot = i % uint64(len(node.log.Logs))
		var proposal = node.log.Logs[slot]
		if proposal == 0 {
			highest = int64(i)
			//if we hit the first unfilled slot stop
			break
		}
		if proposal != NO_OP {
			node.proposeLock.Lock()
			data, present := node.messages[proposal]
			if present {
				delete(node.messages, proposal)
			}
			node.proposeLock.Unlock()
			if present {
				reason := block(i, proposal, data)
				if reason != nil {
					return reason
				}
			} else {
				panic("Message is lost, unrecoverable!")
			}
		}
	}
	atomic.StoreUint64(&node.committed, uint64(highest+1))
	return nil
}
