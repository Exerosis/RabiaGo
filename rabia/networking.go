package rabia

import (
	"context"
	"fmt"
	"go.uber.org/multierr"
	"golang.org/x/sys/unix"
	"net"
	"sync"
	"syscall"
	"time"
)

type Multicaster interface {
	Send(buffer []byte) error
	Receive(buffer []byte) error
	Close() error
	IsOpen() bool
}

type TcpMulticaster struct {
	Inbound  []net.Conn
	Outbound []net.Conn
	Index    int
}

func (tcp *TcpMulticaster) Send(buffer []byte) error {
	var group sync.WaitGroup
	var lock sync.Mutex
	var reasons error
	//var cloned = make([]byte, len(buffer))
	//copy(cloned, buffer)
	group.Add(len(tcp.Outbound))
	for _, connection := range tcp.Outbound {
		go func(connection net.Conn) {
			defer group.Done()
			reason := connection.SetDeadline(time.Now().Add(time.Second))
			if reason != nil {
				lock.Lock()
				defer lock.Unlock()
				reasons = multierr.Append(reasons, reason)
			}
			var start = 0
			for start != len(buffer) {
				amount, reason := connection.Write(buffer[start:])
				if reason != nil {
					lock.Lock()
					defer lock.Unlock()
					reasons = multierr.Append(reasons, reason)
					return
				}
				start += amount
			}
		}(connection)
	}
	group.Wait()
	return reasons
}
func (tcp *TcpMulticaster) Receive(buffer []byte) error {
	connection := tcp.Inbound[tcp.Index%len(tcp.Inbound)]
	//fmt.Printf("Read from: %s\n", connection.RemoteAddr().String())
	var start = 0
	for start != len(buffer) {
		amount, reason := connection.Read(buffer[start:])
		if reason != nil {
			return reason
		}
		start += amount
	}
	tcp.Index++
	return nil
}
func (tcp *TcpMulticaster) Close() error {
	var reasons []error
	for _, connection := range tcp.Inbound {
		reasons = append(reasons, connection.Close())
	}
	return reasons[0]
}
func (tcp *TcpMulticaster) IsOpen() bool {
	return true
}

func TCP(address string, port uint16, addresses ...string) (*TcpMulticaster, error) {
	var control = func(network, address string, conn syscall.RawConn) error {
		var reason error
		if reason := conn.Control(func(fd uintptr) {
			reason = unix.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)
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
	return &TcpMulticaster{Inbound: inbound, Outbound: outbound}, nil
}
