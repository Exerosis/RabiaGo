package main

import (
	"context"
	"fmt"
	"go.uber.org/multierr"
	"net"
	"sync"
	"syscall"
	"time"
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
	var lock sync.Mutex
	var reasons error
	//var cloned = make([]byte, len(buffer))
	//copy(cloned, buffer)
	group.Add(len(tcp.outbound))
	for _, connection := range tcp.outbound {
		go func(connection net.Conn) {
			defer group.Done()
			reason := connection.SetDeadline(time.Now().Add(time.Second))
			if reason != nil {
				lock.Lock()
				defer lock.Unlock()
				reasons = multierr.Append(reasons, reason)
			}
			_, reason = connection.Write(buffer)
			if reason != nil {
				lock.Lock()
				defer lock.Unlock()
				reasons = multierr.Append(reasons, reason)
			}
		}(connection)
	}
	group.Wait()
	return reasons
}
func (tcp *TcpMulticaster) receive(buffer []byte) error {
	connection := tcp.inbound[tcp.index%len(tcp.inbound)]
	//fmt.Printf("Read from: %s\n", connection.RemoteAddr().String())
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
	var control = func(network, address string, conn syscall.RawConn) error {
		var reason error
		if reason := conn.Control(func(fd uintptr) {
			reason = syscall.SetsockoptInt(syscall.Handle(fd), syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)
		}); reason != nil {
			return reason
		}
		return reason
	}
	var listener = net.ListenConfig{
		Control: control,
	}
	var dialer = &net.Dialer{
		Control: control,
	}

	var inbound = make([]net.Conn, len(addresses))
	var outbound = make([]net.Conn, len(addresses))
	var local = fmt.Sprintf("%s:%d", address, port)
	server, reason := listener.Listen(context.Background(), "tcp", local)
	if reason != nil {
		return nil, fmt.Errorf("binding server to %s:%d: %w", address, port, reason)
	}
	var group sync.WaitGroup
	var reasons error
	group.Add(1)
	go func() {
		defer group.Done()
		for i := 0; i < len(addresses); i++ {
			client, reason := server.Accept()
			if reason != nil {
				reasons = multierr.Append(reasons, reason)
				return
			}
			inbound[i] = client
		}
	}()
	for index, node := range addresses {
		var remote = fmt.Sprintf("%s:%d", node, port)
		for {
			client, reason := dialer.Dial("tcp", remote)
			if reason == nil {
				outbound[index] = client
				break
			}
		}
	}
	group.Wait()
	if reasons != nil {
		return nil, reasons
	}
	return &TcpMulticaster{inbound: inbound, outbound: outbound}, nil
}
