package rabia

import (
	"sort"
	"sync"
	"time"
)

type Queue[T any] interface {
	Offer(item T) bool
	Poll() (T, bool)
	Size() int
}

type priority[T any] struct {
	slice   []T
	size    int
	compare func(T, T) bool
	lock    sync.Mutex
	cond    *sync.Cond
	timer   *time.Timer
	timeout time.Duration
}

func NewPriorityBlockingQueue[T any](
	capacity int,
	timeout time.Duration,
	compare func(T, T) bool,
) Queue[T] {
	queue := &priority[T]{
		slice:   make([]T, capacity),
		compare: compare,
		timeout: timeout,
	}
	queue.cond = sync.NewCond(&queue.lock)
	return queue
}

func (queue *priority[T]) Offer(item T) bool {
	queue.lock.Lock()
	defer queue.lock.Unlock()

	var capacity = cap(queue.slice)
	if queue.size == capacity {
		queue.slice = queue.slice[: len(queue.slice)+1 : capacity*2]
	}
	index := sort.Search(queue.size, func(i int) bool {
		return queue.compare(queue.slice[i], item)
	})
	if index < queue.size {
		copy(queue.slice[index:], queue.slice[index+1:queue.size+1])
	}
	queue.slice[index] = item
	queue.size++
	queue.cond.Signal()
	return true
}

func (queue *priority[T]) Poll() (T, bool) {
	queue.lock.Lock()
	defer queue.lock.Unlock()
	var timer *time.Timer = nil
	var size = queue.size
	if size == 0 {
		timer = time.AfterFunc(queue.timeout, queue.cond.Signal)
		queue.cond.Wait()
		timer.Stop()
		if queue.size == 0 {
			var nothing T
			return nothing, false
		}
	}
	queue.size--
	item := queue.slice[queue.size]
	return item, true
}

func (queue *priority[T]) Size() int {
	queue.lock.Lock()
	defer queue.lock.Unlock()
	return queue.size
}
