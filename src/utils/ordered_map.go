package utils

import (
	"fmt"
	"strings"
)

// Node represents a node in the doubly linked list.
type Node[V any] struct {
	key   int
	value V
	prev  *Node[V]
	next  *Node[V]
}

// OrderedMap represents a map that maintains insertion order.
type OrderedMap[V any] struct {
	mapping map[int]*Node[V]
	head    *Node[V]
	tail    *Node[V]
}

// NewOrderedMap creates a new OrderedMap.
func NewOrderedMap[V any]() *OrderedMap[V] {
	return &OrderedMap[V]{
		mapping: make(map[int]*Node[V]),
	}
}

// Set adds a key-value pair to the map, or updates the value if the key exists.
func (om *OrderedMap[V]) Set(key int, value V) {
	if node, exists := om.mapping[key]; exists {
		node.value = value
		return
	}

	newNode := &Node[V]{key: key, value: value}

	if om.tail == nil {
		om.head = newNode
		om.tail = newNode
	} else {
		om.tail.next = newNode
		newNode.prev = om.tail
		om.tail = newNode
	}

	om.mapping[key] = newNode
}

// Get retrieves the value associated with the key. The second return value indicates whether the key was found.
func (om *OrderedMap[V]) Get(key int) (V, bool) {
	node, exists := om.mapping[key]
	if !exists {
		var zero V
		return zero, false
	}
	return node.value, true
}

// Delete removes a key-value pair from the map.
func (om *OrderedMap[V]) Delete(key int) {
	node, exists := om.mapping[key]
	if !exists {
		return
	}

	if node.prev != nil {
		node.prev.next = node.next
	} else {
		om.head = node.next
	}

	if node.next != nil {
		node.next.prev = node.prev
	} else {
		om.tail = node.prev
	}

	delete(om.mapping, key)
}

// Keys returns a slice of keys in the order they were inserted.
func (om *OrderedMap[V]) Keys() []int {
	keys := make([]int, 0, len(om.mapping))
	for node := om.head; node != nil; node = node.next {
		keys = append(keys, node.key)
	}
	return keys
}

// ReversedKeys returns a slice of keys in the reverse order they were inserted.
func (om *OrderedMap[V]) ReversedKeys() []int {
	return reverseSlice(om.Keys())
}

// reverseSlice reverses the order of elements in a slice.
func reverseSlice[T any](s []T) []T {
	n := len(s)
	reversed := make([]T, n)
	for i, v := range s {
		reversed[n-1-i] = v
	}
	return reversed
}

// Values returns a slice of values in the order they were inserted.
func (om *OrderedMap[V]) Values() []V {
	values := make([]V, 0, len(om.mapping))
	for node := om.head; node != nil; node = node.next {
		values = append(values, node.value)
	}
	return values
}

// Len returns the number of elements in the map.
func (om *OrderedMap[V]) Len() int {
	return len(om.mapping)
}

// Clear removes all key-value pairs from the map.
func (om *OrderedMap[V]) Clear() {
	om.mapping = make(map[int]*Node[V])
	om.head = nil
	om.tail = nil
}

// String returns a string representation of the OrderedMap.
func (om *OrderedMap[V]) String() string {
	var sb strings.Builder
	sb.WriteString("OrderedMap: {")

	keys := om.Keys()
	for i, key := range keys {
		value, _ := om.Get(key)
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(fmt.Sprintf("%v: %v", key, value))
	}

	sb.WriteString("}")
	return sb.String()
}

// Sort sorts the OrderedMap's elements in-place by the key using a bottom-up merge sort.
func (om *OrderedMap[V]) Sort() {
	if om.head == nil || om.head.next == nil {
		return
	}

	// Determine the length of the list
	length := 0
	for node := om.head; node != nil; node = node.next {
		length++
	}

	// Dummy node to simplify the merging process
	dummy := &Node[V]{}
	for size := 1; size < length; size *= 2 {
		tail := dummy
		cur := om.head

		for cur != nil {
			// Split the list into two halves of size `size`
			left := cur
			right := split(left, size)
			cur = split(right, size)

			// Merge the two halves and attach to the sorted part
			tail = merge(left, right, tail)
		}

		om.head = dummy.next
	}

	// Fix the tail
	om.tail = om.head
	for om.tail != nil && om.tail.next != nil {
		om.tail = om.tail.next
	}
	// Ensure the head's prev is nil
	if om.head != nil {
		om.head.prev = nil
	}

	// Ensure the tail's next is nil
	if om.tail != nil {
		om.tail.next = nil
	}
}

// split splits the list after `size` nodes and returns the second half.
func split[V any](head *Node[V], size int) *Node[V] {
	for i := 1; head != nil && i < size; i++ {
		head = head.next
	}
	if head == nil {
		return nil
	}

	second := head.next
	head.next = nil
	return second
}

// merge merges two sorted lists and attaches the result to `tail`, returning the new tail.
func merge[V any](l1, l2, tail *Node[V]) *Node[V] {
	cur := tail
	for l1 != nil && l2 != nil {
		if l1.key <= l2.key {
			cur.next = l1
			l1.prev = cur
			l1 = l1.next
		} else {
			cur.next = l2
			l2.prev = cur
			l2 = l2.next
		}
		cur = cur.next
	}

	if l1 != nil {
		cur.next = l1
		l1.prev = cur
	} else if l2 != nil {
		cur.next = l2
		l2.prev = cur
	}

	// Advance to the end of the merged list
	for cur.next != nil {
		cur = cur.next
	}

	return cur
}
