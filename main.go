package main

import (
	"errors"
	"fmt"
	"math"
	"net"
	"strings"
	"time"
)

var COUNT = 10_000_000
var AVERAGE = 10_000

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
	var pipes = make([]uint16, 1)
	for i := range pipes {
		pipes[i] = uint16(3000 + (i * 10))
	}
	return Node(3, address.String(), nodes, pipes...)
}

func main() {
	var reason = run()
	if reason != nil {
		fmt.Println("failed: ", reason)
	}
	time.Sleep(math.MaxUint64)
}
