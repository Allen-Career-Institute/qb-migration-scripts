package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"github.com/go-redis/redis/v8"
	_ "github.com/go-sql-driver/mysql" // Import the MySQL driver
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"os"
	"strconv"
	"strings"
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
		Addr:     "", // Redis server address
		Password: "", // Set password if required
		DB:       0,  // Use default DB
	})
	return rdb
}

func getQuestion(collection *mongo.Collection, oldQuestionID int64) (string, int64, error) {
	ctx := context.TODO()

	// Find document in MongoDB
	filter := bson.M{"oldQuestionId": oldQuestionID} // Assuming MySQL ID matches MongoDB document _id
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
	return quesDoc.QuestionID, quesDoc.Version, nil
}

func getQuestionSolution(collection *mongo.Collection, oldQuestionID int64) (string, int64, error) {
	ctx := context.TODO()

	// Find document in MongoDB
	filter := bson.M{"oldQuestionId": oldQuestionID} // Assuming MySQL ID matches MongoDB document _id
	findOptions := options.FindOne()
	findOptions.SetSort(bson.D{{Key: "version", Value: -1}})

	result := collection.FindOne(ctx, filter, findOptions)
	if result.Err() != nil {
		if result.Err() == mongo.ErrNoDocuments {
			fmt.Printf("No solution document found for oldQuestionId: %d", oldQuestionID)
			return "", 0, nil
		}
		fmt.Printf(" GetQuestionSolution |  oldQuestionId: %d, err %+v", oldQuestionID, result.Err())
		return "", 0, result.Err()
	}

	quesSolDoc := &QuestionSolution{}
	err := result.Decode(quesSolDoc)
	if err != nil {
		fmt.Printf("Error decoding solution  document for oldQuestionId = %v = %v with err = %v", oldQuestionID, err)
		return "", 0, err
	}
	return quesSolDoc.QuestionID, quesSolDoc.VersionID, nil
}

func getRedisKey(client *redis.Client, oldQuestionID int64) (string, string, error) {
	questionKey := REDIS_QB_SERVICE + "." + REDIS_MAPPING + "." + REDIS_QUESTIONS + "." + strconv.FormatInt(oldQuestionID, 10)
	questionMongoID, keyErr := client.LRange(context.Background(), questionKey, -1, -1).Result()
	if keyErr != nil || len(questionMongoID) == 0 {
		if len(questionMongoID) == 0 {
			fmt.Printf("empty List in redis for questionID = %v", oldQuestionID)
			return "", "", nil
		} else {
			fmt.Printf("unable to fetch questionID from redis for questionID = %v with err =%v", oldQuestionID, keyErr)
			return "", "", keyErr
		}
	}
	questionAndVersion := strings.Split(questionMongoID[0], "_")
	return questionAndVersion[0], questionAndVersion[1], nil
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

	// creating file 1

	file1, err := os.Create("mismatch.csv")
	if err != nil {
		log.Fatalf("Failed to create file: %v", err)
	}
	defer file1.Close()

	writer1 := csv.NewWriter(file1)
	defer writer1.Flush() // Ensure all buffered data is written

	header1 := []string{"OldQuestionID", "QuestionId", "QuestionVersion", "QuestionSolutionId", "QuestionSolutionVersion", "RedisQuestionId", "RedisQuestionVersion"}
	if err := writer1.Write(header1); err != nil {
		log.Fatalf("Failed to write header: %v", err)
	}

	// creating file 2
	file2, err := os.Create("correct.csv")
	if err != nil {
		log.Fatalf("Failed to create file: %v", err)
	}
	defer file2.Close()

	// Initialize the CSV writer
	writer2 := csv.NewWriter(file2)
	defer writer2.Flush() // Ensure all buffered data is written

	// Define the header row
	if err := writer2.Write(header1); err != nil {
		log.Fatalf("Failed to write header: %v", err)
	}

	for _, record := range records {
		fmt.Println(record[0])
		oldQuestionID, err1 := strconv.ParseInt(record[0], 10, 64)
		if err1 != nil {
			fmt.Printf("Error occurred for  %+v \n", record[0])
		}

		qID, version, err1 := getQuestion(collection, oldQuestionID)
		if err1 != nil {
			fmt.Printf("GetQuestion | Error occurred for %+v while\n", record[0])
		}

		qSolID, solVersion, err2 := getQuestionSolution(solutionCollection, oldQuestionID)
		if err2 != nil {
			fmt.Printf("GetQuestionSolution | Error occurred for %+v while\n", record[0])
		}
		redisID, redisVersion, err3 := getRedisKey(rdsClient, oldQuestionID)
		if err3 != nil {
			fmt.Printf("GetRedis | Error occurred for %+v while\n", record[0])
		}

		row := []string{record[0], qID, strconv.FormatInt(version, 10), qSolID, strconv.FormatInt(solVersion, 10), redisID, redisVersion}

		fmt.Printf("%v", row)

		if qID != qSolID || qID != redisID || redisVersion != strconv.FormatInt(solVersion, 10) || solVersion != version {
			if err := writer1.Write(row); err != nil {
				log.Fatalf("Failed to write row: %v", err)
			}
		} else {
			if err := writer2.Write(row); err != nil {
				log.Fatalf("Failed to write row: %v", err)
			}
		}
		fmt.Printf("Data fetched completed for  %+v \n", oldQuestionID)
	}

	fmt.Println("Data fetched completed successfully!")
}

const (
	REDIS_QB_SERVICE = "qb"
	REDIS_MAPPING    = "mapping"
	REDIS_QUESTIONS  = "questions"
)
