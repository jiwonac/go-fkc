package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"io"

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
	//fmt.Println("got adjfile", adjFileScanner.Scan())
	groupFileScanner, groupFile := getFileScanner(*groupFileName)
	//fmt.Printf("%v %v\n", adjFile, groupFile)

	// Iterate over line of files
	for i := 0; true; i++ {
		_, err1 := adjFileScanner.Peek(1)
		_, err2 := groupFileScanner.Peek(1)
		if err1 == io.EOF || err2 == io.EOF {
			break
		}

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

	//handleError(adjFileScanner.Err())
	//handleError(groupFileScanner.Err())

	// Create index
	indexModel := mongo.IndexModel{
		Keys: bson.M{
			"index": 1,
		},
	}
	_, err := collection.Indexes().CreateOne(context.Background(), indexModel)
	handleError(err)

	adjFile.Close()
	groupFile.Close()
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

func getFileScanner(fileName string) (*bufio.Reader, *os.File) {
	file, err := os.Open(fileName)
	handleError(err)

	reader := bufio.NewReader(file)

	//scanner := bufio.NewScanner(file)
	return reader, file
}

func handleError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func parseAdjLine(scanner *bufio.Reader) []int {
	fmt.Println("parsing adjlines...")
	ints := make([]int, 0)
	for {
		line, err := scanner.ReadString('\n')
		handleError(err)
		split := strings.Split(line, " : ")
		if len(split) > 1 { // Index on left side
			line = split[1]
		}
		line = strings.TrimSpace(line)
		breakThisLoop := false
		if strings.HasSuffix(line, "}") {
			breakThisLoop = true
		}
		line = strings.Trim(line, "{ } \n")
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

func parseGroupLine(scanner *bufio.Reader) int {
	line, err := scanner.ReadString('\n')
	handleError(err)
	line = strings.Trim(line, "\n")
	parts := strings.Split(line, " : ")
	i, err := strconv.Atoi(parts[1])
	handleError(err)
	return i
}
