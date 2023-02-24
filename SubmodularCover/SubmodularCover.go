package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
)

/*
*
Optimization modes:
0: Classic greedy
1: Lazy greedy
2: Distributed submodular cover (DisCover) using GreeDi & lazygreedy as subroutines
*/
func SubmodularCover(dbName string, collectionName string, coverageReq int,
	groupReqs []int, optimMode int, threads int, dense bool) []int {
	// Get the collection from DB
	collection := getMongoCollection(dbName, collectionName)
	report("obtained collection\n", true)

	// Initialize trackers
	n := getCollectionSize(collection)
	coverageTracker := getCoverageTracker(collection, coverageReq, dense, n)
	groupTracker := groupReqs
	report("initialized trackers\n", true)

	// Choose algorithm to run
	switch optimMode {
	case 0:
		return classicGreedy(collection, coverageTracker, groupTracker, rangeSet(n), -1, threads, true)
	case 1:
		return lazyGreedy(collection, coverageTracker, groupTracker, rangeSet(n), -1, threads, true)
	//case 2:
	//	return disCover(collection, n, coverageTracker, groupReqs, threads, 0.5)
	default:
		return []int{}
	}
}

func getCoverageTracker(collection *mongo.Collection, coverageReq int, dense bool, n int) []int {
	if dense {
		coverageTracker := make([]int, n)
		for i := 0; i < n; i++ {
			coverageTracker[i] = coverageReq
		}
		return coverageTracker
	} else {
		coverageTracker := make([]int, n)
		cur := getFullCursor(collection)
		defer cur.Close(context.Background())
		for i := 0; cur.Next(context.Background()); i++ {
			point := getEntryFromCursor(cur)
			numNeighbors := len(point.Neighbors)
			thisCoverageReq := min(numNeighbors, coverageReq)
			coverageTracker = append(coverageTracker, thisCoverageReq)
			fmt.Printf("\rCoverage tracker iteration %d", i)
		}
		fmt.Printf("\n")
		return coverageTracker
	}
}

func marginalGain(point Point, coverageTracker []int, groupTracker []int, threads int) int {
	numNeighbors := len(point.Neighbors)
	if threads <= 1 { // Singlethreaded
		gain := 0
		for i := 0; i < numNeighbors; i++ { // Marginal gain from k-Coverage
			n := point.Neighbors[i]
			gain += coverageTracker[n]
		}
		gain += groupTracker[point.Group] // Marginal gain from group requirement
		return gain
	} else { // Multithreaded
		// Make a list of arguments
		chunkSize := numNeighbors / threads
		args := make([][]interface{}, threads)
		for t := 0; t < threads; t++ {
			lo := t * chunkSize
			hi := min(numNeighbors, lo+chunkSize)
			arg := []interface{}{
				point.Neighbors[lo:hi],
				coverageTracker,
			}
			args[t] = arg
		}
		// Call workers
		results := concurrentlyExecute(gainWorker, args)
		// Total up results
		gain := 0
		for sum := range results {
			gain += sum.(int)
		}
		gain += groupTracker[point.Group]
		return gain
	}
}

func gainWorker(points []int, coverageTracker []int) int {
	sum := 0
	for i := 0; i < len(points); i++ {
		sum += coverageTracker[points[i]]
	}
	return sum
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

func decrementAllTrackers(points []Point, coverageTracker []int, groupTracker []int) {
	for i := 0; i < len(points); i++ {
		point := points[i]
		decrementTrackers(&point, coverageTracker, groupTracker)
	}
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
	dense := flag.Bool("dense", true, "whether the graph is denser than the k-Coverage requirement")

	// Parse all flags
	flag.Parse()

	// Make the groupReqs array
	groupReqs := make([]int, *groupCntFlag)
	for i := 0; i < *groupCntFlag; i++ {
		groupReqs[i] = *groupReqFlag
	}

	// Run submodularCover
	start := time.Now()
	result := SubmodularCover(*dbFlag, *collectionFlag, *coverageFlag, groupReqs, *optimFlag, *threadsFlag, *dense)
	elapsed := time.Since(start)
	fmt.Print("Obtained solution of size ", len(result), " in ")
	fmt.Printf("%s\n", elapsed)
}
