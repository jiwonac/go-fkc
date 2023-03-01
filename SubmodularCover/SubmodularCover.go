package main

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/mongo"
)

/*
*
Optimization modes:
0: Classic greedy
1: Lazy greedy
2: Lazy Lazy greedy
3: Multilevel with lazylazy -> lazy
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
	report("initialized trackers\n", true)

	// Choose algorithm to run
	switch optimMode {
	case 0:
		result := classicGreedy(collection, coverageTracker, groupReqs, rangeSet(n), -1, threads, true)
		//fmt.Printf("%v\n", result)
		return result
	case 1:
		result := lazyGreedy(collection, coverageTracker, groupReqs, rangeSet(n), -1, threads, true)
		//fmt.Printf("%v\n", result)
		return result
	case 2:
		result := lazyLazyGreedy(collection, coverageTracker, groupReqs, rangeSet(n), -1, threads, true, 0.1, 1.0)
		//fmt.Printf("%v\n", result)
		return result
	case 3:
		firstStage := lazyLazyGreedy(collection, coverageTracker, groupReqs, rangeSet(n), -1, threads, true, 0.2, 0.5)
		//report("solution size for first stage: "+strconv.Itoa(len(firstStage))+"\n", true)
		//fmt.Printf("%v\n", firstStage)
		candidates := setMinus(rangeSet(n), sliceToSet(firstStage))
		//fmt.Println("candidates for second stage: ", len(candidates))
		secondStage := lazyGreedy(collection, coverageTracker, groupReqs, candidates, -1, threads, true)
		totalSolution := append(firstStage, secondStage...)
		//fmt.Printf("%v\n", totalSolution)
		return totalSolution
	//case 2:
	//	return disCover(collection, coverageTracker, groupReqs, rangeSet(n), -1, threads, true, 0)
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
		//fmt.Println(len(coverageTracker))
		return coverageTracker
	} else {
		coverageTracker := make([]int, 0)
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

func getMarginalGains(collection *mongo.Collection, coverageTracker []int,
	groupTracker []int, candidates map[int]bool) []*Item {
	// Query the database
	cur := getSetCursor(collection, candidates)
	defer cur.Close(context.Background())

	// Get results by iterating the cursor
	results := make([]*Item, 0)
	for cur.Next(context.Background()) {
		point := getEntryFromCursor(cur)
		gain := marginalGain(point, coverageTracker, groupTracker, 1)
		item := &Item{
			value:    point.Index,
			priority: gain,
		}
		results = append(results, item)
	}
	return results
}

func notSatisfied(coverageTracker []int, groupTracker []int) bool {
	return remainingScore(coverageTracker, groupTracker) > 0
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

func remainingScore(coverageTracker []int, groupTracker []int) int {
	return sum(coverageTracker) + sum(groupTracker)
}
