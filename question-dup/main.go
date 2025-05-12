package main

import (
	"context"
	"fmt"
	_ "github.com/go-sql-driver/mysql" // Import the MySQL driver
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"os"
	"strconv"
	"sync"
)

// GOOS=linux GOARCH=arm64 go build -o binaryfile ./ ./...      for ec2

// MySQL and MongoDB Configurations
const (
	mysqlDSN    = ""
	mongoURI    = ""
	batchSize   = 1000
	workerCount = 5
)

// Connect to MongoDB
func connectMongo() (*mongo.Client, *mongo.Collection, error) {
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(mongoURI))
	if err != nil {
		return nil, nil, err
	}
	collection := client.Database("qb").Collection("questions")
	return client, collection, nil
}

// Fetch document from MongoDB and update it
func updateMongo(collection *mongo.Collection, oldQuestionID int64) error {
	ctx := context.TODO()

	// Find document in MongoDB
	filter := bson.M{"oldQuestionId": oldQuestionID, "status": 2} // Assuming MySQL ID matches MongoDB document _id
	findOptions := options.Find()
	findOptions.SetSort(bson.D{{Key: "version", Value: -1}})

	cursor, err := collection.Find(ctx, filter, findOptions)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			fmt.Printf("No document found for oldQuestionId: %d", oldQuestionID)
			return nil
		}
		fmt.Printf(" updateMongo |  oldQuestionId: %d, err %+v", oldQuestionID, err)
		return err
	}

	if cursor == nil || cursor.RemainingBatchLength() == 0 {
		fmt.Println("cursor is 0 for oldQuestionId ", oldQuestionID)
		return nil
	}

	var resultList []QuestionDocument

	for cursor.Next(ctx) {
		questionDoc := &QuestionDocument{}
		if err = cursor.Decode(&questionDoc); err != nil {
			fmt.Printf("Error decoding document err: %+v", err)
			continue
		}
		resultList = append(resultList, *questionDoc)
	}

	fmt.Printf("question ID %+v result List %+v \n", oldQuestionID, len(resultList))

	var objectIDs []primitive.ObjectID

	for i, ques := range resultList {
		if i == 0 {
			continue
		}
		objId, objErr := primitive.ObjectIDFromHex(ques.ID)
		if objErr != nil {
			fmt.Printf("Error in generating objectID err %+v", objErr)
		}
		objectIDs = append(objectIDs, objId)
	}
	if len(objectIDs) == 0 {
		return nil
	}
	filter1 := bson.M{"_id": bson.M{"$in": objectIDs}}
	update := bson.M{"$set": bson.M{"status": 3}}

	fmt.Printf("records update for questionID %+v, length %+v ", oldQuestionID, len(objectIDs))

	result, err := collection.UpdateMany(ctx, filter1, update)
	if err != nil {
		fmt.Printf("Failed to update document Error: %v", err)
	}
	fmt.Printf(" Document Update Result oldQuestionID %d MatchedCount:%d ModifiedCount:%d", oldQuestionID, result.MatchedCount, result.ModifiedCount)
	return nil
}

func main() {

	args := os.Args

	// Check if parameters are passed
	if len(args) < 3 {
		fmt.Println("Usage: ./main <param1> <param2> <param2>")
		return
	}
	param1, err := strconv.ParseInt(args[1], 10, 64) // First parameter
	param2, err := strconv.ParseInt(args[2], 10, 64) // Second parameter
	param3, err := strconv.ParseInt(args[3], 10, 64) // Third parameter

	// Print parameters
	fmt.Println("Start OldQuestionID Parameter 1:", param1)
	fmt.Println("End OldQuestionID Parameter 2:", param2)
	fmt.Println("BatchSize Parameter 3: ", param3)

	client, collection, err := connectMongo()
	if err != nil {
		log.Fatal("MongoDB Connection Error:", err)
		fmt.Println("MongoDB Connection Error:", err)
		return
	}
	defer client.Disconnect(context.TODO())

	for i := param1; i <= param2; i = i + param3 {
		oldQuestionIDStart := i
		oldQuestionIDEnd := i + param3 - 1

		var wg sync.WaitGroup

		for j := oldQuestionIDStart; j <= oldQuestionIDEnd; j++ {
			wg.Add(1)
			go func(j int64) {
				defer wg.Done()
				updateMongo(collection, j)
			}(j)
		}

		wg.Wait()
		fmt.Printf("Data backfill completed for  %+v %+v \n", oldQuestionIDStart, oldQuestionIDEnd)
	}

	fmt.Println("Data backfill completed successfully!")
}

func ConvertStructToBsonInterface[T any](document T) interface{} {
	bsonData, err := bson.Marshal(document)
	if err != nil {
		log.Default().Panic("error while converting to bson ")
	}
	// Convert BSON to bson.M
	var bsonM bson.M
	err = bson.Unmarshal(bsonData, &bsonM)
	if err != nil {
		log.Default().Panic("error while converting to bson ")
	}
	return bsonM
}
