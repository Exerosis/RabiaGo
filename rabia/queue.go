package rabia

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

type Queue[T any] interface {
	Offer(item T) bool
	Remove(item T) bool
	Poll() (T, bool)
	PollFast() (T, bool)
	Size() int
	String() string
}

type priority[T any] struct {
	slice   []T
	size    int
	compare func(T, T) int
	cond    *sync.Cond
}

func NewPriorityBlockingQueue[T any](
	capacity int,
	compare func(T, T) int,
) Queue[T] {
	queue := &priority[T]{
		slice:   make([]T, capacity),
		compare: compare,
		cond:    sync.NewCond(&sync.Mutex{}),
	}
	return queue
}

func (queue *priority[T]) String() string {
	queue.cond.L.Lock()
	defer queue.cond.L.Unlock()
	var start = ""
	var count = 0
	for i := queue.size - 1; i >= 0; i-- {
		if count < 5 {
			count++
			start = start + "\n" + fmt.Sprintf("%d", queue.slice[i])
		}
	}
	return start
}

func (queue *priority[T]) Offer(item T) bool {
	queue.cond.L.Lock()
	defer queue.cond.L.Unlock()
	if queue.size > 1 {
		//println("Queue: ", queue.size)
	}
	var capacity = cap(queue.slice)
	if queue.size == capacity {
		panic("overflow not implemented")
	}
	index := sort.Search(queue.size, func(i int) bool {
		return queue.compare(queue.slice[i], item) >= 0
	})
	if index != -1 {
		copy(queue.slice[index+1:], queue.slice[index:queue.size])
	}
	queue.slice[index] = item
	queue.size++
	queue.cond.Signal()
	return true
}

func (queue *priority[T]) Poll() (T, bool) {
	queue.cond.L.Lock()
	defer queue.cond.L.Unlock()

	if queue.size == 0 {
		queue.cond.Wait()
	}
	queue.size--
	return queue.slice[queue.size], true
}

func (queue *priority[T]) PollFast() (T, bool) {
	queue.cond.L.Lock()
	defer queue.cond.L.Unlock()

	if queue.size == 0 {
		queue.cond.L.Unlock()
		time.Sleep(10 * time.Nanosecond)
		queue.cond.L.Lock()
	}
	if queue.size == 0 {
		var result T
		return result, false
	}
	queue.size--
	return queue.slice[queue.size], true
}

func (queue *priority[T]) Remove(item T) bool {
	queue.cond.L.Lock()
	defer queue.cond.L.Unlock()
	for i := 0; i < queue.size; i++ {
		if queue.compare(queue.slice[i], item) == 0 {
			copy(queue.slice[i:], queue.slice[i+1:queue.size])
			queue.size--
			return true
		}
	}
	return false
}

func (queue *priority[T]) Size() int {
	queue.cond.L.Lock()
	defer queue.cond.L.Unlock()
	return queue.size
}
