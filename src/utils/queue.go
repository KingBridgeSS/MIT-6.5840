package utils

type Queue[T any] struct {
	items []T
}

func (q *Queue[T]) Enqueue(item T) {
	q.items = append(q.items, item)
}

func (q *Queue[T]) Dequeue() (T, bool) {
	if q.IsEmpty() {
		var none T
		return none, false
	}
	item := q.items[0]
	newItems := make([]T, len(q.items)-1)
	copy(newItems, q.items[1:])
	q.items = newItems
	return item, true
}
func (q *Queue[T]) Size() int {
	return len(q.items)
}

func (q *Queue[T]) IsEmpty() bool {
	return q.Size() == 0
}
