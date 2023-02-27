package rabia

import (
	"context"
	"fmt"
	"go.uber.org/multierr"
	"golang.org/x/sys/unix"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
)

type Connection interface {
	Read(buffer []byte) error
	Write(buffer []byte) error
	Close() error
}

type multicaster struct {
	connections []Connection
	index       int
	closed      atomic.Bool
}

func Multicaster(connections []Connection) Connection {
	return &multicaster{connections: connections}
}

func (multicaster *multicaster) Write(buffer []byte) error {
	var group sync.WaitGroup
	var lock sync.Mutex
	var reasons error
	group.Add(len(multicaster.connections))
	for _, connection := range multicaster.connections {
		go func(connection Connection) {
			defer group.Done()
			//reason := connection.SetDeadline(time.Now().Add(time.Second))
			//if reason != nil {
			//	lock.Lock()
			//	defer lock.Unlock()
			//	reasons = multierr.Append(reasons, reason)
			//	return
			//}
			reason := connection.Write(buffer)
			if reason != nil {
				lock.Lock()
				defer lock.Unlock()
				reasons = multierr.Append(reasons, reason)
				return
			}
		}(connection)
	}
	group.Wait()
	return reasons
}
func (multicaster *multicaster) Read(buffer []byte) error {
	connection := multicaster.connections[multicaster.index%len(multicaster.connections)]
	multicaster.index++
	return connection.Read(buffer)
}
func (multicaster *multicaster) Close() error {
	var current = multicaster.closed.Load()
	for !current && multicaster.closed.CompareAndSwap(current, true) {
		current = multicaster.closed.Load()
	}
	if current {
		return nil
	}
	var reasons error
	for _, connection := range multicaster.connections {
		reasons = multierr.Append(reasons, connection.Close())
	}
	return reasons
}

type connection struct {
	net.Conn
}

func (instance connection) Address() string {
	return instance.RemoteAddr().String()
}
func (instance connection) Read(buffer []byte) error {
	for start := 0; start != len(buffer); {
		amount, reason := instance.Conn.Read(buffer[start:])
		if reason != nil {
			return reason
		}
		start += amount
	}
	return nil
}
func (instance connection) Write(buffer []byte) error {
	for start := 0; start != len(buffer); {
		amount, reason := instance.Conn.Write(buffer[start:])
		if reason != nil {
			return reason
		}
		start += amount
	}
	return nil
}

type pipe struct {
	read  *io.PipeReader
	write *io.PipeWriter
}

func (pipe *pipe) Read(buffer []byte) error {
	for start := 0; start != len(buffer); {
		amount, reason := pipe.read.Read(buffer[start:])
		if reason != nil {
			return reason
		}
		start += amount
	}
	return nil
}
func (pipe *pipe) Write(buffer []byte) error {
	for start := 0; start != len(buffer); {
		amount, reason := pipe.write.Write(buffer[start:])
		if reason != nil {
			return reason
		}
		start += amount
	}
	return nil
}
func (pipe *pipe) Close() error {
	return multierr.Combine(pipe.read.Close(), pipe.write.Close())
}

func Pipe() Connection {
	read, write := io.Pipe()
	return &pipe{read, write}
}

func Connections(address string, port uint16, addresses ...string) ([]Connection, error) {
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

	var local = fmt.Sprintf("%s:%d", address, port)
	server, reason := listener.Listen(context.Background(), "tcp", local)
	if reason != nil {
		return nil, fmt.Errorf("binding server to %s:%d: %w", address, port, reason)
	}
	var connections = make([]Connection, len(addresses))
	for i, c := range connections {
		println("Connection: ", i, " - ", c)
	}
	for i, other := range addresses {
		//if we are trying to connect to us make a pipe
		if other == address {
			connections[i] = Pipe()
			for range addresses[i+1:] {
				client, reason := server.Accept()
				if reason != nil {
					return nil, reason
				}
				i++
				connections[i] = connection{client}
			}
			break
		} else {
			var remote = fmt.Sprintf("%s:%d", other, port)
			for {
				client, reason := dialer.Dial("tcp", remote)
				if reason == nil {
					connections[i] = connection{client}
					break
				}
			}
		}
	}
	return connections, nil
}
