package rabia

import (
	"sync"
)

type BlockingMap[K comparable, V any] struct {
	mutex sync.Mutex
	cond  *sync.Cond
	data  map[K]V
}

func NewBlockingMap[K comparable, V any]() *BlockingMap[K, V] {
	bm := &BlockingMap[K, V]{
		data: make(map[K]V),
	}
	bm.cond = sync.NewCond(&bm.mutex)
	return bm
}

func (m *BlockingMap[K, V]) Set(key K, value V) {
	m.mutex.Lock()
	m.data[key] = value
	m.mutex.Unlock()
	m.cond.Broadcast()
}

func (m *BlockingMap[K, V]) Get(key K) (V, bool) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	value, ok := m.data[key]
	return value, ok
}

func (m *BlockingMap[K, V]) WaitFor(key K) V {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	for {
		if value, ok := m.data[key]; ok {
			return value
		}
		m.cond.Wait()
	}
}
