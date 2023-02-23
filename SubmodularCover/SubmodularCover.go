package main

import (
	"container/heap"
	"context"
	"flag"
	"fmt"
	"math"
	"sync"
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
		candidates := make(map[int]bool, n)
		for i := 0; i < n; i++ {
			candidates[i] = true
		}
		return lazyGreedy(collection, coverageTracker, groupReqs, threads, -1, candidates, nil, nil, true)
	case 2:
		return disCover(collection, n, coverageTracker, groupReqs, threads, 0.5)
	default:
		return []int{}
	}
}

func getCoverageTracker(collection *mongo.Collection, coverageReq int) []int {
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

func getGroupTracker(collection *mongo.Collection, groupReqs []int) []int {
	return groupReqs
}

func marginalGain(point Point, coverageTracker []int, groupTracker []int, threads int) int {
	if threads <= 1 { // Singlethreaded
		gain := 0
		for i := 0; i < len(point.Neighbors); i++ { // Marginal gain from k-Coverage
			n := point.Neighbors[i]
			gain += coverageTracker[n]
		}
		gain += groupTracker[point.Group] // Marginal gain from group requirement
		return gain
	} else { // Multithreaded
		var wg sync.WaitGroup
		c := make(chan int, threads)
		chunkSize := len(point.Neighbors) / threads
		for i := 0; i < threads; i++ {
			lo := i * chunkSize
			hi := min(len(point.Neighbors), lo+chunkSize)
			wg.Add(1)
			go gainWorker(point.Neighbors[lo:hi], coverageTracker, c, &wg)
		}
		go func() {
			wg.Wait()
			close(c)
		}()
		gain := 0
		for sum := range c {
			gain += sum
		}
		gain += groupTracker[point.Group]
		return gain
	}
}

func gainWorker(points []int, coverageTracker []int, c chan int, wg *sync.WaitGroup) {
	defer wg.Done()
	sum := 0
	for i := 0; i < len(points); i++ {
		sum += coverageTracker[points[i]]
	}
	c <- sum
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
		c := make(chan *Result, threads)
		for i := 0; i < threads; i++ { // Use multithreading
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
	fmt.Printf("\n")
	return coreset
}

func lazyGreedy(collection *mongo.Collection, coverageTracker []int,
	groupTracker []int, threads int, constraint int, candidates map[int]bool,
	c chan []int, wg *sync.WaitGroup, print bool) []int {
	if wg != nil {
		defer wg.Done()
	}
	if print {
		fmt.Println("Executing lazy greedy algorithm...")
	}
	coreset := make([]int, 0)
	candidatesPQ := PriorityQueue{}
	cur := getFullCursor(collection)
	defer cur.Close(context.Background())
	for cur.Next(context.Background()) { // Add in points with their initial gain
		point := getEntryFromCursor(cur)
		if candidates[point.Index] {
			// Either no constraint, or constraint was set and prob passes
			gain := marginalGain(point, coverageTracker, groupTracker, 1)
			item := &Item{
				value:    point.Index,
				priority: gain,
			}
			heap.Push(&candidatesPQ, item)
		}
	}
	//fmt.Println(len(candidatesPQ))

	// Main iteration loop
	if print {
		fmt.Println("Entering the main loop...")
	}
	for notSatisfied(coverageTracker, groupTracker) && len(candidatesPQ) > 0 {
		for { // Loop while we find an optimal element
			index := heap.Pop(&candidatesPQ).(*Item).value
			point := getPointFromDB(collection, index)
			gain := marginalGain(point, coverageTracker, groupTracker, 1)
			if len(candidatesPQ) == 0 || gain >= PeekPriority(&candidatesPQ) { // Optimal element found
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
				heap.Push(&candidatesPQ, item)
			}
		}
		if print {
			fmt.Printf("\rIteration: %d, remaining candidates: %d", len(coreset), len(candidatesPQ))
		}
		if constraint > 0 && len(coreset) >= constraint {
			break
		}
	}
	if print {
		fmt.Printf("\n")
	}
	if c == nil {
		return coreset
	} else {
		c <- coreset
		return coreset
	}
}

func disCover(collection *mongo.Collection, n int, coverageTracker []int,
	groupTracker []int, threads int, alpha float64) []int {
	fmt.Println("Executing DisCover...")
	coreset := make([]int, 0)
	candidates := make(map[int]bool) // Using map as a hashset
	for i := 0; i < n; i++ {         // Initial points
		candidates[i] = true
	}
	lambda := 1.0 / math.Sqrt(float64(threads))

	// Main logic loop
	fmt.Println("Entering the main loop...")
	cardinalityConstraint := 2
	for r := 1; notSatisfied(coverageTracker, groupTracker); r++ {
		// Run DisCover subroutine
		remainingBefore := sum(coverageTracker) + sum(groupTracker)
		newSet := greeDi(candidates, coverageTracker, groupTracker, threads, cardinalityConstraint, collection)
		// Update trackers
		//cur := getSetCursor(collection, newSet)
		//newPoints := make([]Point, len(newSet))
		//for cur.Next(context.Background()) {
		//	newPoints = append(newPoints, getEntryFromCursor(cur))
		//}
		//decrementAllTrackers(newPoints, coverageTracker, groupTracker)
		coreset = append(coreset, newSet...)
		candidates = deleteAllFromSet(candidates, newSet)
		remainingAfter := sum(coverageTracker) + sum(groupTracker)
		// Decide whether to double cardinality coustraint or not
		if float64(remainingBefore-remainingAfter) < alpha*lambda*float64(remainingBefore) {
			cardinalityConstraint *= 2 // Double if marginal gain is too small
		}

		fmt.Printf("\rRound: %d, remaining candidates: %d", r, len(candidates))
	}
	fmt.Printf("\n")
	return coreset
}

func greeDi(candidates map[int]bool, coverageTracker []int, groupTracker []int,
	threads int, cardinalityConstraint int, collection *mongo.Collection) []int {
	// Make a copy of trackers since we don't want to mess with them
	newCoverageTracker := make([]int, len(coverageTracker))
	newGroupTracker := make([]int, len(groupTracker))
	copy(newCoverageTracker, coverageTracker)
	copy(newGroupTracker, groupTracker)

	// Bookkeeping stuff for goroutines
	var wg sync.WaitGroup
	c := make(chan []int, threads)

	// Split candidates into subsets
	splitCandidates := make([]map[int]bool, threads)
	for i := 0; i < threads; i++ {
		splitCandidates[i] = make(map[int]bool, len(candidates)/threads+1)
	}
	t := 0
	for candidate := range candidates {
		splitCandidates[t][candidate] = true
		if t == threads-1 {
			t = 0
		} else {
			t++
		}
	}

	// Call centralized greedy as goroutines with split candidates
	for i := 0; i < threads; i++ {
		wg.Add(1)
		go func(i int) {
			go lazyGreedy(collection, newCoverageTracker, newGroupTracker,
				1, cardinalityConstraint, splitCandidates[i], c, &wg, false)
		}(i)
	}
	go func() {
		wg.Wait()
		close(c)
	}()

	// Filtered candidates = union of solutions from each thread
	filteredCandidates := make(map[int]bool, cardinalityConstraint*threads)
	for slice := range c {
		for _, num := range slice {
			filteredCandidates[num] = true
		}
	}

	// Run centralized greedy on the filtered candidates
	return lazyGreedy(collection, coverageTracker, groupTracker, 1,
		cardinalityConstraint, filteredCandidates, nil, nil, false)
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
			gain := marginalGain(point, coverageTracker, groupTracker, 1)
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
	// Parse all flags
	flag.Parse()

	fmt.Println("Flags: ", *dbFlag, *collectionFlag, *coverageFlag, *groupReqFlag, *groupCntFlag, *optimFlag, *threadsFlag)

	// Make the groupReqs array
	groupReqs := make([]int, *groupCntFlag)
	for i := 0; i < *groupCntFlag; i++ {
		groupReqs[i] = *groupReqFlag
	}

	// Run submodularCover
	start := time.Now()
	result := SubmodularCover(*dbFlag, *collectionFlag, *coverageFlag, groupReqs, *optimFlag, *threadsFlag)
	elapsed := time.Since(start)
	fmt.Print("Obtained solution of size ", len(result), " in ")
	fmt.Printf("%s\n", elapsed)
}
