package main

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql" // Import the MySQL driver
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"os"
	"strconv"
	"sync"
)

// MySQL and MongoDB Configurations
const (
	mysqlDSN    = ""
	mongoURI    = ""
	batchSize   = 1000
	workerCount = 5
)

func connectMySQL() (*sql.DB, error) {
	db, err := sql.Open("mysql", mysqlDSN)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	return db, nil
}

// Connect to MongoDB
func connectMongo() (*mongo.Client, *mongo.Collection, error) {
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(mongoURI))
	if err != nil {
		return nil, nil, err
	}
	collection := client.Database("qb").Collection("questionSolutions")
	return client, collection, nil
}

// Fetch records from MySQL in batches
func fetchQuestionRecords(db *sql.DB, oldQuestionIDStart, oldQuestionIDEnd int64) (map[int64]MQuestion, error) {

	query := `
       select
	    q.id,
	    q.vtag,
	    q.vtag_type
	FROM questions q
	WHERE q.id BETWEEN ? AND ?;
    `

	rows, err := db.Query(query, oldQuestionIDStart, oldQuestionIDEnd)

	if err != nil {
		fmt.Printf("error in questions query %+v", err)
		return nil, err
	}
	defer rows.Close()

	records := make(map[int64]MQuestion, 0)
	for rows.Next() {
		var rec MQuestion
		err := rows.Scan(&rec.ID, &rec.VTag, &rec.VTagType)
		if err != nil {
			fmt.Printf("error in fetching question records %+v", err)
			return nil, err
		}
		if rec.VTag != nil && rec.VTagType != nil {
			records[int64(rec.ID)] = rec
		}
	}
	return records, nil
}

// Fetch document from MongoDB and update it
func updateMongo(collection *mongo.Collection, oldQuestionID int64, question MQuestion) error {
	ctx := context.TODO()

	// Find document in MongoDB
	filter := bson.M{"oldQuestionId": oldQuestionID} // Assuming MySQL ID matches MongoDB document _id
	cursor, err := collection.Find(ctx, filter)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			fmt.Printf("No document found for oldQuestionId: %d \n", oldQuestionID)
			return nil
		}
		fmt.Printf(" updateMongo |  oldQuestionId: %d, err %+v \n", oldQuestionID, err)
		return err
	}

	if cursor == nil || cursor.RemainingBatchLength() == 0 {
		fmt.Println("cursor is 0 for oldQuestionId \n", oldQuestionID)
		return nil
	}

	var resultList []QuestionSolution

	for cursor.Next(ctx) {
		questionSolDoc := &QuestionSolution{}
		if err = cursor.Decode(&questionSolDoc); err != nil {
			fmt.Printf("Error decoding document err: %+v \n", err)
			continue
		}
		resultList = append(resultList, *questionSolDoc)
	}

	fmt.Printf("question ID %+v result List %+v \n", oldQuestionID, len(resultList))

	var updatedDocs []mongo.WriteModel

	for _, quesSol := range resultList {
		var videoSol []*VideoSolutionDocument

		for _, videoSolution := range quesSol.VideoSolutions {

			if videoSolution.VTag == convertString(question.VTag) {
				vTagType := convertString(question.VTagType)
				val, exist := VideoTypeMap[vTagType]
				if exist {
					videoSolution.VTagType = val
				} else {
					videoSolution.VTagType = 0
				}
			}
			videoSol = append(videoSol, videoSolution)
		}
		quesSol.VideoSolutions = videoSol

		bsonDoc := ConvertStructToBsonInterface(quesSol)
		updatedDocs = append(updatedDocs, mongo.NewReplaceOneModel().SetFilter(bson.M{"_id": quesSol.ID}).SetReplacement(bsonDoc))
	}

	fmt.Printf("records update for questionID %+v, length %+v \n", oldQuestionID, len(updatedDocs))

	result, err := collection.BulkWrite(ctx, updatedDocs)
	if err != nil {
		fmt.Printf("Failed to update document Error: %v \n", err)
	}
	fmt.Printf(" Document Update Result oldQuestionID %d MatchedCount:%d ModifiedCount:%d \n", oldQuestionID, result.MatchedCount, result.ModifiedCount)
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

	// Connect to MySQL and MongoDB
	db, err := connectMySQL()
	if err != nil {
		log.Fatal("MySQL Connection Error:", err)
		fmt.Println("MySQL Connection Error:", err)
		return
	}
	defer db.Close()

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
		mapQuestionRecords, _ := fetchQuestionRecords(db, oldQuestionIDStart, oldQuestionIDEnd)

		var wg sync.WaitGroup

		for j := oldQuestionIDStart; j <= oldQuestionIDEnd; j++ {
			wg.Add(1)
			go func(j int64) {
				defer wg.Done()

				questionRecords, exist := mapQuestionRecords[j]
				if !exist {
					fmt.Printf("questionRecords not found Question ID, %+v \n", j)
				} else {
					updateMongo(collection, j, questionRecords)
				}
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

func convertString(input *string) string {
	if input != nil {
		return *input
	}
	return ""
}

func convertInt32(input *int32) int32 {
	if input != nil {
		return *input
	}
	return 0 // Default value if NULL
}

func convertInt64(input *int64) int64 {
	if input != nil {
		return *input
	}
	return 0 // Default value if NULL
}

var VideoTypeMap = map[string]int32{
	"":      0,
	"video": 1,
	"image": 2,
	"audio": 3,
}
