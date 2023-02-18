package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/exerosis/RabiaGo/rabia"
	"math"
	"math/rand"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// 1 - 5.5 (10k)
// 2 - 13 (10k)
// 4 - 29 (10k)
// 8 - 55 (10k)
// 16 - 83 (10k)
// 32 - 111 (1m)
// 64 - 127 (1m)
// 128 - 138 (1m)
// 256 - 139 (1m)
// 512 - 138 (1m)
// 1024 - 138 (1m)
const Pipes = 1
const Count = uint32(1000)

func run() error {
	interfaces, reason := net.Interfaces()
	if reason != nil {
		return reason
	}
	var network net.Interface
	var address net.Addr
	for _, i := range interfaces {
		addresses, reason := i.Addrs()
		if reason != nil {
			return reason
		}
		for _, a := range addresses {
			if strings.Contains(a.String(), "192.168.1.") {
				address = a
				network = i
			}
		}
	}
	if address == nil {
		return errors.New("couldn't find interface")
	}

	fmt.Printf("Interface: %s\n", network.Name)
	fmt.Printf("Address: %s\n", address)

	var addresses = []string{
		"192.168.1.1",
		"192.168.1.2",
		"192.168.1.3",
	}
	var pipes = make([]uint16, Pipes)
	for i := range pipes {
		pipes[i] = uint16(3000 + (i * 10))
	}
	var complete sync.WaitGroup
	complete.Add(1)
	var node = rabia.MakeRabiaNode(addresses, pipes...)
	go func() {
		reason := node.Run(strings.Split(address.String(), "/")[0])
		if reason != nil {
			panic(reason)
		}
	}()
	go func() {
		for {
			var highest = atomic.LoadInt64(&node.Highest)
			for i := node.Committed; int64(i) <= highest; i++ {
				var slot = i % uint64(len(node.Log.Logs))
				var proposal = node.Log.Logs[slot]
				if proposal == 0 {
					highest = int64(i)
					//if we hit the first unfilled slot stop
					break
				}
				if proposal != math.MaxUint64 {
					node.ProposeMutex.RLock()
					data, present := node.Messages[proposal]
					node.ProposeMutex.RUnlock()
					if present {
						var test = binary.LittleEndian.Uint32(data)
						println("handling: ", test)
						node.ProposeMutex.Lock()
						delete(node.Messages, proposal)
						node.ProposeMutex.Unlock()

						if uint64(test) != proposal {
							panic("Out of Order")
						}
						if test == Count-1 {
							complete.Done()
						}
					}
				}
			}
			atomic.StoreUint64(&node.Committed, uint64(highest+1))
		}
	}()

	for i := uint32(0); i < Count; i++ {
		var data = make([]byte, 4)
		binary.LittleEndian.PutUint32(data, i)
		reason := node.Propose(uint64(i), data)
		if reason != nil {
			return reason
		}
		//propose(node, data)
	}
	complete.Wait()
	println("Done!")
	return nil
}

func propose(node *rabia.RabiaNode, data []byte) {
	var id uint64
	for id == 0 || id >= math.MaxUint64-1 {
		var stamp = uint64(time.Now().UnixMilli())
		id = uint64(rand.Uint32())<<32 | stamp
	}
	reason := node.Propose(id, data)
	if reason != nil {
		panic(reason)
	}
}

func main() {
	//file, reason := os.Create("cpu.pprof")
	//if reason != nil {
	//	fmt.Println("failed: ", reason)
	//}
	//reason = pprof.StartCPUProfile(file)
	//if reason != nil {
	//	fmt.Println("failed: ", reason)
	//}
	//defer pprof.StopCPUProfile()
	var reason = run()
	if reason != nil {
		fmt.Println("failed: ", reason)
	}
}
