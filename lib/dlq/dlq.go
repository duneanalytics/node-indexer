package dlq

import (
	"sync"
	"time"

	pq "github.com/emirpasic/gods/queues/priorityqueue"
	"github.com/emirpasic/gods/utils"
)

type DLQ[T any] struct {
	priorityQueue pq.Queue // structure not thread safe
	mutex         sync.Mutex
	retryDelay    RetryStrategy
}

// RetryStrategy takes the number of retries and returns a Duration.
// This allows the caller to implement any strategy they like, be it constant, linear, exponential, etc.
type RetryStrategy func(retries int) time.Duration

// Item This is generic so that metadata about retries can be maintained in an envelope during processing for when
// an item needs to make its way back onto the DLQ later
type Item[T any] struct {
	Value       T
	Retries     int
	nextRunTime time.Time
}

func MapItem[T, U any](b Item[T], mapper func(T) U) Item[U] {
	return Item[U]{
		Value:       mapper(b.Value),
		Retries:     b.Retries,
		nextRunTime: b.nextRunTime,
	}
}

func NewDLQ[T any]() *DLQ[T] {
	return NewDLQWithDelay[T](RetryDelayLinear(time.Minute))
}

func RetryDelayLinear(backoff time.Duration) RetryStrategy {
	return func(retries int) time.Duration {
		return time.Duration(retries) * backoff // retries must be converted to a Duration for multiplication
	}
}

func NewDLQWithDelay[T any](retryDelay func(retries int) time.Duration) *DLQ[T] {
	return &DLQ[T]{priorityQueue: *pq.NewWith(byNextRunTime[T]), retryDelay: retryDelay}
}

// Comparator function (sort by nextRunTime in ascending order)
func byNextRunTime[T any](a, b interface{}) int {
	return utils.TimeComparator(a.(Item[T]).nextRunTime, b.(Item[T]).nextRunTime)
}

func (dlq *DLQ[T]) AddItem(item T, retries int) {
	nextRunTime := time.Now().Add(dlq.retryDelay(retries))

	dlq.mutex.Lock()
	defer dlq.mutex.Unlock()

	dlq.priorityQueue.Enqueue(Item[T]{Value: item, Retries: retries + 1, nextRunTime: nextRunTime})
}

func (dlq *DLQ[T]) AddItemHighPriority(item T) {
	dlq.mutex.Lock()
	defer dlq.mutex.Unlock()

	dlq.priorityQueue.Enqueue(Item[T]{Value: item, Retries: 0, nextRunTime: time.Time{}})
}

func (dlq *DLQ[T]) GetNextItem() (value *Item[T], ok bool) {
	dlq.mutex.Lock()
	defer dlq.mutex.Unlock()

	peek, ok := dlq.priorityQueue.Peek()
	if !ok || peek.(Item[T]).nextRunTime.After(time.Now()) {
		return nil, false
	}

	item, ok := dlq.priorityQueue.Dequeue()
	if ok {
		itemCasted := item.(Item[T])
		return &itemCasted, ok
	}
	return nil, ok
}

func (dlq *DLQ[T]) Size() int {
	dlq.mutex.Lock()
	defer dlq.mutex.Unlock()

	return dlq.priorityQueue.Size()
}
