package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	_ "github.com/go-sql-driver/mysql" // Import the MySQL driver
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"os"
	"strconv"
	"time"
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

func redisClient() *redis.Client {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		// Password: "",
		// Username: "default",
		// PoolSize: 100,
		// TLSConfig: &tls.Config{
		// 	MinVersion: tls.VersionTLS12,
		// },
	})
	return rdb
}

func updateMongo(client *redis.Client, collection *mongo.Collection, oldQuestionID int64) (string, int64, error) {
	ctx := context.TODO()

	// Find Latest document in MongoDB
	filter := bson.M{"oldQuestionId": oldQuestionID}
	findOptions := options.FindOne()
	findOptions.SetSort(bson.D{{Key: "version", Value: -1}})

	result := collection.FindOne(ctx, filter, findOptions)
	if result.Err() != nil {
		if result.Err() == mongo.ErrNoDocuments {
			fmt.Printf("No document found for oldQuestionId: %d", oldQuestionID)
			return "", 0, nil
		}
		fmt.Printf(" GetQuestion |  oldQuestionId: %d, err %+v", oldQuestionID, result.Err())
		return "", 0, result.Err()
	}

	quesDoc := &QuestionDocument{}
	err := result.Decode(quesDoc)
	if err != nil {
		fmt.Printf("Error decoding question document for oldQuestionId = %v = %v with err = %v", oldQuestionID, err)
		return "", 0, err
	}

	// Modifying others documents to status 3
	filter1 := bson.M{
		"oldQuestionId": oldQuestionID,
		"version":       bson.M{"$ne": quesDoc.Version},
	}

	update := bson.M{
		"$set": bson.M{
			"status":    3,
			"updatedAt": time.Now().UnixMilli(),
		},
	}

	opts := options.Update().SetUpsert(false)

	result1, errNew := collection.UpdateMany(ctx, filter1, update, opts)
	if errNew != nil {
		fmt.Printf("Failed to update document Error: %v", errNew)
	}
	fmt.Printf(" Document Update Result oldQuestionID %d MatchedCount:%d ModifiedCount:%d \n", oldQuestionID, result1.MatchedCount, result1.ModifiedCount)

	// Modifying Latest documents to status 2 if it is not
	if quesDoc.Status != 2 {
		filter2 := bson.M{
			"oldQuestionId": oldQuestionID,
			"version":       quesDoc.Version,
		}

		update2 := bson.M{
			"$set": bson.M{
				"status":    2,
				"updatedAt": time.Now().UnixMilli(),
			},
		}
		opts2 := options.Update().SetUpsert(false)
		result2, errNew2 := collection.UpdateMany(ctx, filter2, update2, opts2)
		if errNew2 != nil {
			fmt.Printf("Failed to update document Error: %v", errNew2)
		}
		fmt.Printf(" Document Update2 Result oldQuestionID %d MatchedCount:%d ModifiedCount:%d \n", oldQuestionID, result2.MatchedCount, result2.ModifiedCount)
	}

	// Updating Redis Key

	questionKey := REDIS_QB_SERVICE + "." + REDIS_MAPPING + "." + REDIS_QUESTIONS + "." + strconv.FormatInt(oldQuestionID, 10)
	redisVal := quesDoc.QuestionID + "_" + strconv.FormatInt(quesDoc.Version, 10)
	_, err1 := client.RPush(context.Background(), questionKey, redisVal).Result()
	if err1 != nil {
		fmt.Printf("error redis mapping failed for %d \n", oldQuestionID)

	}
	fmt.Printf("redis mapping added for %d \n", oldQuestionID)
	questionActiveKey := REDIS_QB_SERVICE + "." + REDIS_MAPPING + "." + REDIS_QUESTIONS + "." + QUESTION_ACTIVE_VERSION + "." + strconv.FormatInt(oldQuestionID, 10)

	questionDataRedis := RedisData{
		QuestionID:       quesDoc.QuestionID,
		Version:          quesDoc.Version,
		UniqueIdentifier: quesDoc.UniqueIdentifier,
	}

	jsonData, marshalErr := json.Marshal(questionDataRedis)
	if marshalErr != nil {
		fmt.Printf("unable to marshal mapping for key %s: %v \n", questionActiveKey, marshalErr)
	}
	_, err2 := client.Set(ctx, questionActiveKey, jsonData, 0).Result()
	if err2 != nil {
		fmt.Printf("error redis active version mapping failed for  %d \n", oldQuestionID)
	}

	return quesDoc.QuestionID, quesDoc.Version, nil
}

// Fetch document from MongoDB and update it
func updateMongoSol(collection *mongo.Collection, oldQuestionID int64, questionID string, version int64) error {

	ctx := context.TODO()

	filter := bson.M{"questionId": questionID, "versionId": version} // Assuming MySQL ID matches MongoDB document _id
	result := collection.FindOne(ctx, filter)
	if result.Err() != nil {
		if result.Err() == mongo.ErrNoDocuments {
			fmt.Printf("No solution document found for oldQuestionId: %d", oldQuestionID)
		} else {
			fmt.Printf(" GetQuestionSolution |  oldQuestionId: %d, err %+v", oldQuestionID, result.Err())
			return result.Err()
		}
	} else {
		return nil
	}

	filter = bson.M{"oldQuestionId": oldQuestionID} // Assuming MySQL ID matches MongoDB document _id
	findOptions := options.FindOne()
	findOptions.SetSort(bson.D{{Key: "versionId", Value: -1}})

	result = collection.FindOne(ctx, filter, findOptions)
	if result.Err() != nil {
		if result.Err() == mongo.ErrNoDocuments {
			fmt.Printf("No solution document found for oldQuestionId: %d", oldQuestionID)
			return nil
		}
		fmt.Printf(" GetQuestionSolution |  oldQuestionId: %d, err %+v", oldQuestionID, result.Err())
		return result.Err()
	}

	quesSolDoc := &QuestionSolution{}
	err := result.Decode(quesSolDoc)
	if err != nil {
		fmt.Printf("Error decoding solution  document for oldQuestionId = %v = %v with err = %v", oldQuestionID, err)
		return err
	}

	quesSolDoc.QuestionID = questionID
	quesSolDoc.VersionID = version
	quesSolDoc.ID = ""

	fmt.Printf("records update for questionID %+v \n", oldQuestionID)

	singleResult, err := collection.InsertOne(ctx, quesSolDoc)
	if err != nil {
		fmt.Printf("Failed to update document Error: %v", err)
	}
	fmt.Printf(" solution Document added oldQuestionID %d  with %+v \n", oldQuestionID, singleResult.InsertedID)

	return nil
}

func main() {

	// Reader file
	file, err := os.Open("data.csv")
	if err != nil {
		log.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		log.Fatalf("Failed to read CSV: %v", err)
	}

	// client connection
	client, collection, err := connectMongo()
	if err != nil {
		log.Fatal("MongoDB Connection Error:", err)
		fmt.Println("MongoDB Connection Error:", err)
		return
	}
	defer client.Disconnect(context.TODO())
	solutionCollection := client.Database("qb").Collection("questionSolutions")
	rdsClient := redisClient()

	for i, record := range records {
		fmt.Println(record[0])
		oldQuestionID, err1 := strconv.ParseInt(record[0], 10, 64)
		if err1 != nil {
			fmt.Printf("Error occurred for  %+v \n", record[0])
		}

		qID, version, _ := updateMongo(rdsClient, collection, oldQuestionID)

		if qID != "" {
			updateMongoSol(solutionCollection, oldQuestionID, qID, version)
		}

		fmt.Printf("Data update completed for  %+v \n", oldQuestionID)

		if i%500 == 0 {
			time.Sleep(2 * time.Second)
		}
	}

	fmt.Println("Data update completed successfully!")

}

const (
	REDIS_QB_SERVICE        = "qb"
	REDIS_MAPPING           = "mapping"
	REDIS_QUESTIONS         = "questions"
	QUESTION_ACTIVE_VERSION = "activeVersion"
)

type RedisData struct {
	QuestionID       string `json:"questionId,omitempty"`
	UniqueIdentifier string `json:"uniqueIdentifier,omitempty"`
	OldPaperID       int64  `json:"oldPaperId,omitempty"`
	Version          int64  `json:"version,omitempty"`
}
