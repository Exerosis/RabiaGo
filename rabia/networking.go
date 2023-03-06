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

type multicaster struct {
	connections []Connection
	index       int
	closed      atomic.Bool
}

func (multicaster *multicaster) Write(buffer []byte) error {
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

//
//type pipe struct {
//	cond   *sync.Cond
//	buffer []byte
//	read   int
//	write  int
//}
//
//func (p *pipe) Read(buffer []byte) error {
//	p.cond.L.Lock()
//	defer p.cond.L.Unlock()
//
//	for p.read == p.write {
//		p.cond.Wait()
//	}
//
//	for len(buffer) > 0 {
//		if p.read == p.write {
//			p.cond.Wait()
//		}
//
//		n := copy(buffer, p.buffer[p.read:])
//		p.read = (p.read + n) % len(p.buffer)
//		buffer = buffer[n:]
//
//		if p.read == p.write {
//			p.read = 0
//			p.write = 0
//			break
//		}
//	}
//
//	p.cond.Broadcast()
//
//	return nil
//}
//
//func (p *pipe) Write(buffer []byte) error {
//	p.cond.L.Lock()
//	defer p.cond.L.Unlock()
//
//	for len(buffer) > 0 {
//		if len(p.buffer)-p.write < len(buffer) {
//			p.cond.Wait()
//		}
//
//		n := copy(p.buffer[p.write:], buffer)
//		p.write = (p.write + n) % len(p.buffer)
//		buffer = buffer[n:]
//
//		if len(buffer) > 0 && p.write == p.read {
//			p.cond.Wait()
//		}
//	}
//
//	p.cond.Broadcast()
//
//	return nil
//}
//
//func (p *pipe) Close() error {
//	p.cond.L.Lock()
//	defer p.cond.L.Unlock()
//	return nil
//}
//
//func Pipe(size uint32) Connection {
//	p := &pipe{
//		cond:   sync.NewCond(&sync.Mutex{}),
//		buffer: make([]byte, size),
//	}
//	return p
//}

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

func Multicaster(connections ...Connection) Connection {
	return &multicaster{connections: connections}
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
