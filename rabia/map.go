package rabia

import (
	"sync"
)

type BlockingMap[K comparable, V any] struct {
	cond *sync.Cond
	data map[K]V
}

func NewBlockingMap[K comparable, V any]() *BlockingMap[K, V] {
	var mutex = &sync.Mutex{}
	bm := &BlockingMap[K, V]{
		data: make(map[K]V),
		cond: sync.NewCond(mutex),
	}
	return bm
}

func (m *BlockingMap[K, V]) Set(key K, value V) {
	m.cond.L.Lock()
	m.data[key] = value
	m.cond.Broadcast()
	m.cond.L.Unlock()
}

func (m *BlockingMap[K, V]) Delete(key K) {
	m.cond.L.Lock()
	delete(m.data, key)
	m.cond.L.Unlock()
}

func (m *BlockingMap[K, V]) Get(key K) (V, bool) {
	m.cond.L.Lock()
	defer m.cond.L.Unlock()
	value, ok := m.data[key]
	return value, ok
}

func (m *BlockingMap[K, V]) WaitFor(key K) V {
	m.cond.L.Lock()
	defer m.cond.L.Unlock()
	for {
		if value, ok := m.data[key]; ok {
			return value
		}
		m.cond.Wait()
	}
}
