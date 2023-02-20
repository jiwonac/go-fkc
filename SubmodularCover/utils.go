package main

import (
	"container/heap"
	"log"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type Result struct {
	thread int
	index  int
	gain   int
}

func getBestResult(results chan *Result) *Result {
	best := &Result{
		thread: -1,
		index:  -1,
		gain:   -1,
	}
	for result := range results {
		if result.gain > best.gain {
			best = result
		}
	}
	return best
}

type Point struct {
	ID        primitive.ObjectID `bson:"_id"`
	Index     int                `bson:"index"`
	Group     int                `bson:"group"`
	Neighbors []int              `bson:"neighbors"`
}

// Very basic error handling
func handleError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func removeFromSlice(s []int, index int) []int {
	for i := 0; i < len(s); i++ {
		if i == index {
			s[i] = s[len(s)-1]
			return s[:len(s)-1]
		}
	}
	return s
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func sum(slice []int) int {
	sum := 0
	for i := range slice {
		sum += slice[i]
	}
	return sum
}

func rangeSlice(n int) []int {
	result := make([]int, n)
	for i := 0; i < n; i++ {
		result[i] = i
	}
	return result
}

/**
Everything required to implement priority queue.
*/

type Item struct {
	value    int
	priority int
	index    int
}

type PriorityQueue []*Item

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].priority > pq[j].priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x any) {
	n := len(*pq)
	item := x.(*Item)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// update modifies the priority and value of an Item in the queue.
func (pq *PriorityQueue) update(item *Item, value int, priority int) {
	item.value = value
	item.priority = priority
	heap.Fix(pq, item.index)
}

/**
Test method for priority queue.
*/
/*
func main() {
	pq := make(PriorityQueue, 0)
	item1 := &Item{
		value:    1,
		priority: 10,
	}
	heap.Push(&pq, item1)
	item2 := &Item{
		value:    2,
		priority: 5,
	}
	heap.Push(&pq, item2)
	item3 := &Item{
		value:    3,
		priority: 8,
	}
	heap.Push(&pq, item3)
	items := make([]int, 0)
	for i := 0; i < 3; i++ {
		item := heap.Pop(&pq).(*Item)
		items = append(items, item.value)
	}
	fmt.Printf("%v\n", items)
	item4 := &Item{
		value:    4,
		priority: 10,
	}
	heap.Push(&pq, item4)
	item5 := &Item{
		value:    5,
		priority: 5,
	}
	heap.Push(&pq, item5)
	item6 := &Item{
		value:    6,
		priority: 8,
	}
	heap.Push(&pq, item6)
	items = make([]int, 0)
	for i := 0; i < 3; i++ {
		item := heap.Pop(&pq).(*Item)
		items = append(items, item.value)
	}
	fmt.Printf("%v\n", items)
}
*/
