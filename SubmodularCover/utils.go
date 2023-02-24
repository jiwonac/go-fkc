package main

import (
	"container/heap"
	"fmt"
	"log"
	"reflect"
	"sync"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

/**
Utility related to reasoning about the result of a marginal gain evaluation
*/

type Result struct {
	index int
	gain  int
}

func setEmptyResult() *Result {
	return &Result{
		index: -1,
		gain:  -1,
	}
}

func getBestResult(results chan interface{}) *Result {
	best := setEmptyResult()
	for r := range results {
		if res, ok := r.(*Result); ok {
			if res.gain > best.gain {
				best = res
			}
		} else {
			fmt.Println("Interpret error")
		}
	}
	return best
}

/**
The type of a database entry.
*/

type Point struct {
	ID        primitive.ObjectID `bson:"_id"`
	Index     int                `bson:"index"`
	Group     int                `bson:"group"`
	Neighbors []int              `bson:"neighbors"`
}

/**
Miscellaneous functions.
*/

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

func rangeSet(n int) map[int]bool {
	result := make(map[int]bool, n)
	for i := 0; i < n; i++ {
		result[i] = true
	}
	return result
}

func deleteAllFromSet(set map[int]bool, keys []int) map[int]bool {
	for i := 0; i < len(keys); i++ {
		key := keys[i]
		delete(set, key)
	}
	return set
}

func report(message string, print bool) {
	if print {
		fmt.Printf(message)
	}
}

func mapToSlice(set map[int]bool) []int {
	keys := make([]int, 0)
	for k := range set {
		keys = append(keys, k)
	}
	return keys
}

func splitSet(set map[int]bool, threads int) []map[int]bool {
	result := make([]map[int]bool, threads)
	for i := 0; i < threads; i++ {
		result[i] = make(map[int]bool, len(set)/threads+1)
	}
	i := 0
	for key := range set {
		assign := i % threads
		result[assign][key] = true
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
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
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

func PeekPriority(pq *PriorityQueue) int {
	return (*pq)[0].priority
}

/**
Concurrency wrapper to avoid rewriting the same boilerplate code
*/

func concurrentlyExecute(f interface{}, args [][]interface{}) chan interface{} {
	threads := len(args)
	results := make(chan interface{}, threads)
	var wg sync.WaitGroup
	for t := 0; t < threads; t++ {
		wg.Add(1)
		go func(args []interface{}) {
			argValues := make([]reflect.Value, len(args))
			for i, arg := range args {
				argValues[i] = reflect.ValueOf(arg)
			}
			result := reflect.ValueOf(f).Call(argValues)[0].Interface()
			results <- result
			wg.Done()
		}(args[t])
	}
	wg.Wait()
	close(results)
	return results
}
