package rabia

import (
	"context"
	"fmt"
	"github.com/BertoldVdb/go-misc/bufferedpipe"
	"go.uber.org/multierr"
	"golang.org/x/sys/unix"
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

type Dmulticaster struct {
	connections []Connection
	Index       int
	closed      atomic.Bool
	advance     bool
	name        *string
}

func (multicaster *Dmulticaster) Write(buffer []byte) error {
	var group sync.WaitGroup
	var lock sync.Mutex
	var reasons error
	group.Add(len(multicaster.connections))
	for i, c := range multicaster.connections {
		go func(i int, c Connection) {
			defer group.Done()
			//reason := connection.SetDeadline(time.Now().Add(time.Second))
			//if reason != nil {
			//	lock.Lock()
			//	defer lock.Unlock()
			//	reasons = multierr.Append(reasons, reason)
			//	return
			//}
			reason := c.Write(buffer)
			if reason != nil {
				lock.Lock()
				reasons = multierr.Append(reasons, reason)
				lock.Unlock()
			}
		}(i, c)
	}
	group.Wait()
	return reasons
}
func (multicaster *Dmulticaster) Read(buffer []byte) error {
	connection := multicaster.connections[multicaster.Index%len(multicaster.connections)]
	if multicaster.advance {
		multicaster.Index++
	}
	return connection.Read(buffer)
}
func (multicaster *Dmulticaster) Close() error {
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
	*bufferedpipe.BufferedPipe
}

func (p *pipe) Read(buffer []byte) error {
	for start := 0; start != len(buffer); {
		amount, reason := p.BufferedPipe.Read(buffer[start:])
		if reason != nil {
			return reason
		}
		start += amount
	}
	return nil
}
func (p *pipe) Write(buffer []byte) error {
	for start := 0; start != len(buffer); {
		amount, reason := p.BufferedPipe.Write(buffer[start:])
		if reason != nil {
			return reason
		}
		start += amount
	}
	return nil
}

func (p *pipe) Close() error {
	return p.BufferedPipe.Close()
}

func Pipe(size uint32) Connection {
	p := &pipe{
		bufferedpipe.NewBufferedPipe(int(size)),
	}
	return p
}

//type pipe struct {
//	channel chan byte
//}
//
//func (pipe *pipe) Read(buffer []byte) error {
//	for i := range buffer {
//		buffer[i] = <-pipe.channel
//	}
//	return nil
//}
//func (pipe *pipe) Write(buffer []byte) error {
//	for i := range buffer {
//		pipe.channel <- buffer[i]
//	}
//	return nil
//}
//func (pipe *pipe) Close() error {
//	close(pipe.channel)
//	return nil
//}
//
//func Pipe(size uint32) Connection {
//	return &pipe{make(chan byte, size)}
//}

func control(network, address string, conn syscall.RawConn) error {
	var reason error
	if reason := conn.Control(func(fd uintptr) {
		reason = unix.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)
	}); reason != nil {
		return reason
	}
	return reason
}

func Multicaster(connections ...Connection) *Dmulticaster {
	return &Dmulticaster{connections: connections, advance: true}
}
func FixedMulticaster(index int, name string, connections ...Connection) *Dmulticaster {
	return &Dmulticaster{connections: connections, Index: index, advance: false, name: &name}
}
func Group(address string, port uint16, addresses ...string) ([]Connection, error) {
	var listener = net.ListenConfig{Control: control}
	var dialer = &net.Dialer{Control: control}
	var local = fmt.Sprintf("%s:%d", address, port)
	server, reason := listener.Listen(context.Background(), "tcp", local)
	if reason != nil {
		return nil, fmt.Errorf("binding server to %s:%d: %w", address, port, reason)
	}
	var connections = make([]Connection, len(addresses))
	for i, other := range addresses {
		//if we are trying to connect to us make a pipe
		if other == address {
			connections[i] = Pipe(65536)
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
func GroupSet(address string, port uint16, addresses ...string) ([]Connection, []Connection, error) {
	var listener = net.ListenConfig{Control: control}
	var dialer = &net.Dialer{Control: control}
	var local = fmt.Sprintf("%s:%d", address, port)
	server, reason := listener.Listen(context.Background(), "tcp", local)
	if reason != nil {
		return nil, nil, fmt.Errorf("binding server to %s:%d: %w", address, port, reason)
	}
	var group sync.WaitGroup
	var outbound = make([]Connection, len(addresses))
	group.Add(1)
	go func() {
		for i, other := range addresses {
			var remote = fmt.Sprintf("%s:%d", other, port)
			for {
				client, reason := dialer.Dial("tcp", remote)
				if reason == nil {
					outbound[i] = connection{client}
					break
				}
			}
		}
		group.Done()
	}()

	var inbound = make([]Connection, len(addresses))
	for i := range addresses {
		client, reason := server.Accept()
		if reason != nil {
			return nil, nil, reason
		}
		inbound[i] = connection{client}
	}

	group.Wait()
	return inbound, outbound, nil
}
