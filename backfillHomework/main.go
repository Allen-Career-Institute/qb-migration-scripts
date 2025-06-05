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
	collection := client.Database("qb").Collection("homeworkQuestions")
	return client, collection, nil
}

func fetchQuestionRecords(db *sql.DB, startID, endID int64) ([]MHomework, error) {

	query := `
       select
	    q.paper_id,
	    q.question_id,
	    q.center_name,
        q.id
	FROM homework_questions_mapping q
	WHERE q.id BETWEEN ? AND ?;
    `

	rows, err := db.Query(query, startID, endID)

	if err != nil {
		fmt.Printf("error in questions query %+v \n", err)
		return nil, err
	}
	defer rows.Close()

	records := make([]MHomework, 0)
	for rows.Next() {
		var rec MHomework
		err := rows.Scan(&rec.PaperID, &rec.QuestionID, &rec.CenterName, &rec.ID)
		if err != nil {
			fmt.Printf("error in fetching question records %+v \n", err)
			continue
			// return nil, err
		}
		if rec.CenterName != nil {
			records = append(records, rec)
		}
	}
	return records, nil
}

// Fetch document from MongoDB and update it
func updateMongo(collection *mongo.Collection, oldPaperID, oldQuestionID int64, center string) error {
	ctx := context.TODO()

	// Find document in MongoDB
	filter := bson.M{"oldPaperId": oldPaperID, "oldQuestionId": oldQuestionID} // Assuming MySQL ID matches MongoDB document _id

	update := bson.M{
		"$set": bson.M{
			"centerName": center,
		},
	}

	result, err := collection.UpdateMany(ctx, filter, update)
	if err != nil {
		fmt.Printf("Failed to update document Error: %v \n", err)
	}
	fmt.Printf(" Document Update Result oldPaperId %d oldQuestionID %d MatchedCount:%d ModifiedCount:%d \n", oldPaperID, oldQuestionID, result.MatchedCount, result.ModifiedCount)
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
		oldQuestionIDStart := param1
		oldQuestionIDEnd := i + param3 - 1
		homeworkRecords, _ := fetchQuestionRecords(db, oldQuestionIDStart, oldQuestionIDEnd)

		var wg sync.WaitGroup

		for _, homework := range homeworkRecords {
			hm := homework
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := updateMongo(collection, hm.PaperID, hm.QuestionID, convertString(hm.CenterName)); err != nil {
					fmt.Printf("Error in updating MongoDB for paperID %d QuestionID, %+v \n", hm.PaperID, hm.QuestionID)
				}
			}()
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
