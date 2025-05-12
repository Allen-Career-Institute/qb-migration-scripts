package main

import (
	"context"
	"encoding/csv"
	"fmt"
	_ "github.com/go-sql-driver/mysql" // Import the MySQL driver
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"os"
	"strconv"
)

// GOOS=linux GOARCH=arm64 go build -o binaryfile ./ ./...      for ec2

// MySQL and MongoDB Configurations
const (
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
	filter := bson.M{"oldQuestionId": oldQuestionID} // Assuming MySQL ID matches MongoDB document _id
	cursor, err := collection.Find(ctx, filter)
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

	fmt.Printf("question ID %+v result List %+v", oldQuestionID, len(resultList))

	var updatedDocs []mongo.WriteModel

	for _, ques := range resultList {

		var taxonomyList []TaxonomyData

		for _, taxonomyData := range ques.TaxonomyData {
			if taxonomyData.SubjectId == "" || taxonomyData.TopicId == "" || taxonomyData.SubTopicId == "" {
				continue
			} else {
				taxonomyList = append(taxonomyList, taxonomyData)
			}
		}
		ques.TaxonomyData = taxonomyList

		objId, objErr := primitive.ObjectIDFromHex(ques.ID)
		if objErr != nil {
			fmt.Printf("Error in generating objectID err %+v", objErr)
		}
		ques.ID = ""
		bsonDoc := ConvertStructToBsonInterface(ques)
		updatedDocs = append(updatedDocs, mongo.NewReplaceOneModel().SetFilter(bson.M{"_id": objId}).SetReplacement(bsonDoc))
	}

	fmt.Printf("records update for questionID %+v, length %+v ", oldQuestionID, len(updatedDocs))

	result, err := collection.BulkWrite(ctx, updatedDocs)
	if err != nil {
		fmt.Printf("Failed to update document Error: %v", err)
	}
	fmt.Printf(" Document Update Result oldQuestionID %d MatchedCount:%d ModifiedCount:%d", oldQuestionID, result.MatchedCount, result.ModifiedCount)
	return nil
}

func main() {

	file, err := os.Open("data.csv")
	if err != nil {
		log.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	// Create a new CSV reader
	reader := csv.NewReader(file)

	// Read all records
	records, err := reader.ReadAll()
	if err != nil {
		log.Fatalf("Failed to read CSV: %v", err)
	}

	// Iterate through the records and print the first column

	client, collection, err := connectMongo()
	if err != nil {
		log.Fatal("MongoDB Connection Error:", err)
		fmt.Println("MongoDB Connection Error:", err)
		return
	}
	defer client.Disconnect(context.TODO())

	for _, record := range records {
		fmt.Println(record[0])
		oldQuestionID, err1 := strconv.ParseInt(record[0], 10, 64)
		if err1 != nil {
			fmt.Printf("Error occurred for  %+v \n", record[0])
		}
		// oldQuestionID := int64(4820893)
		updateMongo(collection, oldQuestionID)
		fmt.Printf("Data backfill completed for  %+v \n", oldQuestionID)
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
