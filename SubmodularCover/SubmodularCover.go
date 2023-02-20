package main

import (
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
	candidates := rangeSlice(n) // Points not in coreset

	// Main logic
	for notSatisfied(coverageTracker, groupTracker) {
		for i := 0; i < threads; i++ { // Use multithreading
			c := make(chan *Result, threads)
			var wg sync.WaitGroup
			chunkSize := (len(candidates) + threads - 1) / threads
			for i := 0; i < threads; i++ { // Concurrently call threads
				start := i * chunkSize
				end := min(start+chunkSize, n)
				wg.Add(1)
				go classicWorker(collection, candidates[start:end],
					coverageTracker, groupTracker, &wg, c, threads, i)
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
			candidates = removeFromSlice(candidates, chosen.index)
			// Decrement trackers
			point := getPointFromDB(collection, chosen.index)
			decrementTrackers(&point, coverageTracker, groupTracker)

			fmt.Printf("\rIteration: %d", len(coreset))
		}
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

func classicWorker(collection *mongo.Collection, candidates []int, coverageTracker []int,
	groupTracker []int, wg *sync.WaitGroup, c chan *Result, numThreads int, thread int) {
	//fmt.Println("entered classic worker ", thread, len(candidates))
	//fmt.Printf("%v\n", candidates)
	defer wg.Done()
	cur := getFullCursor(collection)
	result := &Result{
		thread: -1,
		index:  -1,
		gain:   -1,
	}
	for cur.Next(context.Background()) { // Iterate over query results
		point := getEntryFromCursor(cur)
		if point.Index%numThreads == thread {
			gain := marginalGain(point, coverageTracker, groupTracker)
			if gain > result.gain { // Update result if better marginal gain found
				result = &Result{
					thread: thread,
					index:  point.Index,
					gain:   gain,
				}
			}
		} else {
			continue
		}
	}
	//fmt.Println("Worker ", thread, "'s best result")
	//fmt.Printf("%v\n", result)
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
