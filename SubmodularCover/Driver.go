package main

import (
	"flag"
	"fmt"
	"time"
)

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
	//batchSize := flag.Int("batch", 10000, "number of entries to query from MongoDB at once")

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
