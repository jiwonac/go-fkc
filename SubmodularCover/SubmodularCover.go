package main

import (
	"container/heap"
	"context"
	"flag"
	"log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Point struct {
	index     int
	neighbors []int
}

func removeFromSlice(s []int, i int) []int {
	s[i] = s[len(s)-1]
	return s[:len(s)-1]
}

func importMongoCollection(dbName string, collectionName string) *mongo.Collection {
	// Create mongoDB server connection
	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017")
	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		log.Fatal(err)
	}

	// Create handles
	db := client.Database(dbName)
	collection := db.Collection(collectionName)

	return collection
}

func loadGraphFromCollection(collection mongo.Collection) [][]int {
	// Query the collection and grab cursor
	cur, err := collection.Find(context.Background(), bson.D{})
	if err != nil {
		log.Fatal(err)
	}
	defer cur.Close(context.Background())

	// Internal representation of the graph
	var graph [][]int

	// Iterate over cursor
	for cur.Next(context.Background()) {
		var entry Point
		/*if err != cur.Decode(&entry); err != nil {
			log.Fatal(err)
		}*/
		// Set the graph's element as adjacency list
		neighborList := []int
		for _, neighbor := range entry.neighbors {
			neighborList.append(neighbor)
		}
		graph[entry.index] = neighborList
	}

	return graph
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

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

func marginalGain(point int, coverageTracker []int, groupTracker []int) int {

}

/**
 * parameters:
 * graph: The graph as a slice of adjacency linked lists
 * coverageReq: k-coverage requirement
 * numGroups: total number of groups
 * groupReq: group count requirement
 * lazyLevel:
 *   0: regular greedy
 *   1: lazy greedy
 *   2: lazylazy greedy
 * layers: number of layers to use for multi-level submodular optimization speedup
 * threads: number of threads to use for concurrent evaluation
 * returns the resultant coreset
 */
func submodularCover(graph [][]int, coverageReq int, numGroups int, groupReq int, lazyLevel int, layers int, threads int) map[int]bool {
	// Create trackers & containers
	n := len(graph)                   // number of points in graph
	coverageTracker := make([]int, n) // k-coverage requirement remaining for each point
	for i := 0; i < n; i++ {
		coverageTracker[i] = max(coverageReq, graph[i].Len())
	}
	groupTracker := make([]int, numGroups) // group requirement remaining for each group
	for i := 0; i < numGroups; i++ {
		groupTracker[i] = groupReq
	}
	coreset := map[int]bool{} // use hashmap as a "hashset" for the coreset

	// If using classic greedy, use a linked list of points not in coreset
	// Otherwise, use a priority queue
	candidatesSet := make([]int, 100)
	candidatesPriorityQueue := make(PriorityQueue, 100)

	// Initialize the candidates priority queue with initial value
	if lazyLevel > 0 {
		for _, point := range len(graph) {
			gain := marginalGain(point, coverageTracker, groupTracker)
			item := &Item{
				value:    point,
				priority: gain,
			}
			heap.Push(&candidatesPriorityQueue, gain)
		}
	}

	return coreset
}

func main() {
	dbFlag := flag.String("db", "dummydb", "The MongoDB DB")
	collectionFlag := flag.String("collection", "random100", "The Collection containing Adjancy lists")
	coverageFlag := flag.int("k", 5, "k-Coverage requirement")
	groupFlag := flag.int("group", 20, "group count requirement")

}
