package main

import (
	"bufio"
	"context"
	"flag"
	"log"
	"os"
	"strconv"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

/**
Given adjacency list in a text file, load it onto MongoDB.
The text file has format:
index : { neighbor, neighbor, ..., neighbor}
Group assignment is given by another text file, of format:
index : group
*/

type Point struct {
	ID        primitive.ObjectID `bson:"_id,omitempty"`
	Index     int                `bson:"index"`
	Group     int                `bson:"group"`
	Neighbors []int              `bson:"neighbors"`
}

func main() {
	// Parse flags
	db := flag.String("db", "dummydb", "Name of MongoDB database")
	col := flag.String("col", "dummycol", "Name of MongoDB collection")
	adjFileName := flag.String("adjfile", "test1.txt", "File containing adjacency lists")
	groupFileName := flag.String("groupfile", "test2.txt", "FIle containing group assignments")
	flag.Parse()

	// Access DB & files
	collection, client := getMongoCollection(*db, *col)
	defer client.Disconnect(context.Background())
	adjFileScanner, adjFile := getFileScanner(*adjFileName)
	defer adjFile.Close()
	groupFileScanner, groupFile := getFileScanner(*groupFileName)
	defer groupFile.Close()
	//fmt.Printf("%v %v\n", adjFile, groupFile)

	// Iterate over line of files
	for i := 0; adjFileScanner.Scan() && groupFileScanner.Scan(); i++ {
		adjList := parseAdjLine(adjFileScanner)
		group := parseGroupLine(groupFileScanner)

		point := &Point{
			Index:     i,
			Group:     group,
			Neighbors: adjList,
		}

		// Insert point to collection
		_, err := collection.InsertOne(context.Background(), point)
		handleError(err)
	}

	// Create index
	indexModel := mongo.IndexModel{
		Keys: bson.M{
			"index": 1,
		},
	}
	_, err := collection.Indexes().CreateOne(context.Background(), indexModel)
	handleError(err)
}

func getMongoCollection(db string, col string) (*mongo.Collection, *mongo.Client) {
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI("mongodb://localhost:27017"))
	handleError(err)

	//ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	//defer cancel()
	//err = client.Connect(ctx)
	//handleError(err)
	//defer client.Disconnect(context.Background())

	database := client.Database(db)
	collection := database.Collection(col)

	_, err = collection.DeleteMany(context.Background(), bson.M{})
	handleError(err)
	return collection, client
}

func getFileScanner(fileName string) (*bufio.Scanner, *os.File) {
	file, err := os.Open(fileName)
	handleError(err)

	scanner := bufio.NewScanner(file)
	return scanner, file
}

func handleError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func parseAdjLine(scanner bufio.Scanner) []int {
	ints := make([]int, 0)
	for {
		line := scanner.Text()
		split := strings.Split(line, " : ")
		if len(split) > 1 { // Index on left side
			line = split[1]
		}
		line = strings.TrimSpace(line)
		breakThisLoop := false
		if strings.HasSuffix(line, "}") {
			breakThisLoop = true
		}
		line = strings.Trim(line, "{ }")
		split = strings.Split(line, ", ")
		for _, v := range split {
			n, _ := strconv.Atoi(v)
			ints = append(ints, n)
		}
		if breakThisLoop {
			break
		}
	}
	return ints
}

func parseGroupLine(scanner bufio.Scanner) int {
	line := scanner.Text()
	parts := strings.Split(line, " : ")
	i, err := strconv.Atoi(parts[1])
	handleError(err)
	return i
}
