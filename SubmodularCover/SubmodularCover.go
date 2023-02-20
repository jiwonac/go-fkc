package main

import (
	"container/heap"
	"context"
	"flag"
	"fmt"
	"sync"

	"go.mongodb.org/mongo-driver/mongo"
)

/*
*
Optimization modes:
0: Classic greedy
1: Lazy greedy
*/
func SubmodularCover(dbName string, collectionName string, coverageReq int,
	groupReqs []int, optimMode int, threads int) []int {
	// Get the collection from DB
	collection := getMongoCollection(dbName, collectionName)
	fmt.Println("obtained collection")

	// Initialize trackers
	n := getCollectionSize(collection)
	coverageTracker := getCoverageTracker(collection, coverageReq)
	groupTracker := getGroupTracker(collection, groupReqs)
	fmt.Println("initialized trackers")

	// Main logic
	switch optimMode {
	case 0:
		return classicGreedy(collection, n, coverageTracker, groupTracker, threads)
	case 1:
		return lazyGreedy(collection, n, coverageTracker, groupReqs)
	default:
		return []int{}
	}
}

func getCoverageTracker(collection *mongo.Collection, coverageReq int) []int {
	coverageTracker := make([]int, 0)
	cur := getFullCursor(collection)
	defer cur.Close(context.Background())
	for cur.Next(context.Background()) {
		point := getEntryFromCursor(cur)
		numNeighbors := len(point.Neighbors)
		thisCoverageReq := min(numNeighbors, coverageReq)
		coverageTracker = append(coverageTracker, thisCoverageReq)
	}
	return coverageTracker
}

func getGroupTracker(collection *mongo.Collection, groupReqs []int) []int {
	return groupReqs
}

func marginalGain(point Point, coverageTracker []int, groupTracker []int) int {
	gain := 0
	for i := 0; i < len(point.Neighbors); i++ { // Marginal gain from k-Coverage
		n := point.Neighbors[i]
		gain += coverageTracker[n]
	}
	gain += groupTracker[point.Group] // Marginal gain from group requirement
	return gain
}

func classicGreedy(collection *mongo.Collection, n int, coverageTracker []int,
	groupTracker []int, threads int) []int {
	fmt.Println("Executing classic greedy algorithm...")
	// Initialize sets
	coreset := make([]int, 0)
	candidates := make(map[int]bool) // Using map as a hashset
	for i := 0; i < n; i++ {         // Initial points
		candidates[i] = true
	}
	chunkSize := n / threads

	// Main logic
	fmt.Println("Entering the main loop...")
	for notSatisfied(coverageTracker, groupTracker) {
		var wg sync.WaitGroup
		for i := 0; i < threads; i++ { // Use multithreading
			c := make(chan *Result, threads)
			for i := 0; i < threads; i++ { // Concurrently call threads
				lo := i * chunkSize
				hi := min(n, lo+chunkSize) - 1
				wg.Add(1)
				go classicWorker(collection, candidates,
					coverageTracker, groupTracker, &wg, c, lo, hi)
			}
			go func() { // Wait for all threads to finish
				wg.Wait()
				close(c)
			}()
			// Figure out the overall maximum marginal gain element
			chosen := getBestResult(c)
			//fmt.Printf("%v\n", chosen)
			// Add to coreset
			coreset = append(coreset, chosen.index)
			// Remove from candidates set
			delete(candidates, chosen.index)
			// Decrement trackers
			point := getPointFromDB(collection, chosen.index)
			decrementTrackers(&point, coverageTracker, groupTracker)

			fmt.Printf("\rIteration: %d", len(coreset))
		}
	}
	fmt.Printf("\n")
	return coreset
}

func lazyGreedy(collection *mongo.Collection, n int, coverageTracker []int,
	groupTracker []int) []int {
	fmt.Println("Executing lazy greedy algorithm...")
	coreset := make([]int, 0)
	// Candidates set is a priority queue with initial gain
	candidates := PriorityQueue{}
	cur := getFullCursor(collection)
	defer cur.Close(context.Background())
	for i := 0; i < n; i++ { // Add in points with their initial gain
		cur.Next(context.Background())
		point := getEntryFromCursor(cur)
		gain := marginalGain(point, coverageTracker, groupTracker)
		item := &Item{
			value:    point.Index,
			priority: gain,
		}
		heap.Push(&candidates, item)
	}

	// Main iteration loop
	fmt.Println("Entering the main loop...")
	for notSatisfied(coverageTracker, groupTracker) {
		for { // Loop while we find an optimal element
			index := heap.Pop(&candidates).(*Item).value
			point := getPointFromDB(collection, index)
			gain := marginalGain(point, coverageTracker, groupTracker)
			threshold := PeekPriority(&candidates)
			if gain >= threshold { // Optimal element found
				// Add to coreset
				coreset = append(coreset, index)
				// Decrement trackers
				decrementTrackers(&point, coverageTracker, groupTracker)
				break
			} else { // Add the point back to heap with updated marginal gain
				item := &Item{
					value:    index,
					priority: gain,
				}
				heap.Push(&candidates, item)
			}
		}
		fmt.Printf("\rIteration: %d, remaining candidates: %d", len(coreset), len(candidates))
	}
	fmt.Printf("\n")
	return coreset
}

func notSatisfied(coverageTracker []int, groupTracker []int) bool {
	for i := 0; i < len(groupTracker); i++ {
		if groupTracker[i] > 0 {
			return true
		}
	}
	for i := 0; i < len(coverageTracker); i++ {
		if coverageTracker[i] > 0 {
			return true
		}
	}
	return false
}

func classicWorker(collection *mongo.Collection, candidates map[int]bool, coverageTracker []int,
	groupTracker []int, wg *sync.WaitGroup, c chan *Result, lo int, hi int) {
	defer wg.Done()
	cur := getRangeCursor(collection, lo, hi)
	defer cur.Close(context.Background())
	result := &Result{
		index: -1,
		gain:  -1,
	}
	for cur.Next(context.Background()) { // Iterate over query results
		point := getEntryFromCursor(cur)
		index := point.Index
		// If the point is a candidate AND it is assigned to thid worker thread
		if candidates[index] {
			gain := marginalGain(point, coverageTracker, groupTracker)
			if gain > result.gain { // Update result if better marginal gain found
				result = &Result{
					index: index,
					gain:  gain,
				}
			}
		} else {
			continue
		}
	}
	c <- result
}

func decrementTrackers(point *Point, coverageTracker []int, groupTracker []int) {
	for i := 0; i < len(point.Neighbors); i++ {
		n := point.Neighbors[i]
		val := coverageTracker[n]
		coverageTracker[n] = max(0, val-1)
	}
	gr := point.Group
	val := groupTracker[gr]
	groupTracker[gr] = max(0, val-1)
}

func main() {
	// Define command-line flags
	dbFlag := flag.String("db", "dummydb", "MongoDB DB")
	collectionFlag := flag.String("col", "n1000d3m5r20", "ollection containing points")
	coverageFlag := flag.Int("k", 20, "k-coverage requirement")
	groupReqFlag := flag.Int("group", 100, "group count requirement")
	groupCntFlag := flag.Int("m", 5, "number of groups")
	optimFlag := flag.Int("optim", 0, "optimization mode")
	threadsFlag := flag.Int("t", 1, "number of threads")
	// Parse all flags
	flag.Parse()

	fmt.Println("Flags: ", *dbFlag, *collectionFlag, *coverageFlag, *groupReqFlag, *groupCntFlag, *optimFlag, *threadsFlag)

	// Make the groupReqs array
	groupReqs := make([]int, *groupCntFlag)
	for i := 0; i < *groupCntFlag; i++ {
		groupReqs[i] = *groupReqFlag
	}

	// Run submodularCover
	result := SubmodularCover(*dbFlag, *collectionFlag, *coverageFlag, groupReqs, *optimFlag, *threadsFlag)
	fmt.Printf("%v\n", result)
	fmt.Println(len(result))
}
