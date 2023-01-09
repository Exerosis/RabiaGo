package main

import (
	"errors"
	"fmt"
	"net"
	"os"
	"runtime/pprof"
	"strings"
)

const COUNT = 10_000_000
const AVERAGE = 10_000
const INFO = false

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
const PIPES = 256

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

	var nodes = []string{
		"192.168.1.1",
		"192.168.1.2",
		"192.168.1.3",
	}
	var pipes = make([]uint16, PIPES)
	for i := range pipes {
		pipes[i] = uint16(3000 + (i * 10))
	}
	return Node(3, strings.Split(address.String(), "/")[0], nodes, pipes...)
}

func main() {
	file, reason := os.Create("cpu.pprof")
	if reason != nil {
		fmt.Println("failed: ", reason)
	}
	reason = pprof.StartCPUProfile(file)
	if reason != nil {
		fmt.Println("failed: ", reason)
	}
	defer pprof.StopCPUProfile()
	reason = run()
	if reason != nil {
		fmt.Println("failed: ", reason)
	}
}
