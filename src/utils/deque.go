package utils

type Deque[T any] struct {
	items []T
}

func (d *Deque[T]) EnqueueFront(item T) {
	d.items = append([]T{item}, d.items...)
}

func (d *Deque[T]) EnqueueBack(item T) {
	d.items = append(d.items, item)
}

func (d *Deque[T]) DequeueFront() (T, bool) {
	if len(d.items) == 0 {
		var none T
		return none, false
	}
	item := d.items[0]
	newItems := make([]T, len(d.items)-1)
	copy(newItems, d.items[1:])
	d.items = newItems
	return item, true
}

func (d *Deque[T]) DequeueBack() (T, bool) {
	if len(d.items) == 0 {
		var none T
		return none, false
	}
	item := d.items[len(d.items)-1]
	newItems := make([]T, len(d.items)-1)
	copy(newItems, d.items[:len(d.items)-1])
	d.items = newItems
	return item, true
}

func (d *Deque[T]) Size() int {
	return len(d.items)
}

func (d *Deque[T]) IsEmpty() bool {
	return d.Size() == 0
}
