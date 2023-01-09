package main

import (
	"fmt"
	"net"
	"sync"
)

type Multicaster interface {
	send(buffer []byte) error
	receive(buffer []byte) error
	close() error
	isOpen() bool
}

type TcpMulticaster struct {
	inbound  []net.Conn
	outbound []net.Conn
	index    int
}

func (tcp *TcpMulticaster) send(buffer []byte) error {
	var group sync.WaitGroup
	//var lock sync.Mutex
	var reasons []error
	//var cloned = make([]byte, len(buffer))
	//copy(cloned, buffer)
	group.Add(len(tcp.outbound))
	for _, connection := range tcp.outbound {
		go func(connection net.Conn) {
			defer group.Done()
			_, reason := connection.Write(buffer)
			fmt.Println("Wrote for ", connection.RemoteAddr().String())
			fmt.Printf("Timed out! %s %s\n", reason, connection.RemoteAddr().String())

			//if reason != nil {
			//	lock.Lock()
			//	defer lock.Unlock()
			//	reasons = append(reasons, reason)
			//}
		}(connection)
	}
	group.Wait()
	if len(reasons) > 0 {
		return reasons[0]
	}
	return nil
}
func (tcp *TcpMulticaster) receive(buffer []byte) error {
	connection := tcp.inbound[tcp.index%len(tcp.inbound)]
	fmt.Printf("Read from: %s", connection.LocalAddr().String())
	_, reason := connection.Read(buffer)
	if reason != nil {
		return reason
	}
	tcp.index++
	return nil
}
func (tcp *TcpMulticaster) close() error {
	var reasons []error
	for _, connection := range tcp.inbound {
		reasons = append(reasons, connection.Close())
	}
	return reasons[0]
}
func (tcp *TcpMulticaster) isOpen() bool {
	return true
}

func TCP(address string, port uint16, addresses ...string) (*TcpMulticaster, error) {
	var inbound = make([]net.Conn, len(addresses))
	var outbound = make([]net.Conn, len(addresses))
	server, reason := net.Listen("tcp", fmt.Sprintf("%s:%d", address, port))
	if reason != nil {
		return nil, fmt.Errorf("binding server to %s:%d: %w", address, port, reason)
	}
	var group sync.WaitGroup
	var reasons []error
	group.Add(1)
	go func() {
		defer group.Done()
		for i := 0; i < len(addresses); i++ {
			client, reason := server.Accept()
			if reason != nil {
				reasons = append(reasons, reason)
				return
			}
			inbound[i] = client
		}
	}()
	for index, node := range addresses {
		remote, reason := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", node, port))
		if reason != nil {
			return nil, fmt.Errorf("resolving remote %s:%d: %w", node, port, reason)
		}
		for {
			client, reason := net.DialTCP("tcp", nil, remote)
			if reason == nil {
				outbound[index] = client
				break
			}
		}
	}
	group.Wait()
	if len(reasons) > 0 {
		//TODO join errors or whatever.
		return nil, reasons[0]
	}
	return &TcpMulticaster{inbound: inbound, outbound: outbound}, nil
}
