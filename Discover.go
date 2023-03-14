package main

import (
	"fmt"
	"math"
	"sync"

	"go.mongodb.org/mongo-driver/mongo"
)

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
