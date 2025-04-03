package main

import (
	"context"
	"database/sql"
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
	collection := client.Database("qb").Collection("testSets")
	return client, collection, nil
}

func getString(paperID, setID int64) string {
	return strconv.FormatInt(paperID, 10) + "_" + strconv.FormatInt(setID, 10)
}

func fetchQuestionRecords(db *sql.DB, oldQuestionIDStart, oldQuestionIDEnd int64) (map[string]MTestSet, error) {

	query := `
       select
	    q.paper_id,
	    q.id,
	    q.set_pdf_url
	FROM test_sets q
	WHERE q.id BETWEEN ? AND ?;
    `

	rows, err := db.Query(query, oldQuestionIDStart, oldQuestionIDEnd)

	if err != nil {
		fmt.Printf("error in questions query %+v \n", err)
		return nil, err
	}
	defer rows.Close()

	records := make(map[string]MTestSet, 0)
	for rows.Next() {
		var rec MTestSet
		err := rows.Scan(&rec.PaperID, &rec.ID, &rec.SetPdfUrl)
		if err != nil {
			fmt.Printf("error in fetching question records %+v \n", err)
			continue
			// return nil, err
		}
		if rec.SetPdfUrl != nil {
			records[getString(rec.PaperID, rec.ID)] = rec
		}
	}
	return records, nil
}

// Fetch document from MongoDB and update it
func updateMongo(collection *mongo.Collection, oldPaperID, setID int64, mTestSet MTestSet) error {
	ctx := context.TODO()

	// Find document in MongoDB
	filter := bson.M{"oldPaperId": oldPaperID, "setId": setID} // Assuming MySQL ID matches MongoDB document _id
	cursor, err := collection.Find(ctx, filter)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			fmt.Printf("No document found for oldPaperId: %d setID %d \n", oldPaperID, setID)
			return nil
		}
		fmt.Printf(" updateMongo |  oldPaperId: %d, setID %d , err %+v \n", oldPaperID, setID, err)
		return err
	}

	if cursor == nil || cursor.RemainingBatchLength() == 0 {
		fmt.Printf("cursor is 0 for oldPaperId %v \n", oldPaperID)
		return nil
	}

	var resultList []TestSet

	for cursor.Next(ctx) {
		testSetDoc := &TestSet{}
		if err = cursor.Decode(&testSetDoc); err != nil {
			fmt.Printf("Error decoding document err: %+v \n", err)
			continue
		}
		resultList = append(resultList, *testSetDoc)
	}

	fmt.Printf(" oldPaperId: %d, setID %d result List %+v \n", oldPaperID, setID, len(resultList))

	var updatedDocs []mongo.WriteModel

	for _, testSet := range resultList {
		testSet.SetPdfUrl = convertString(mTestSet.SetPdfUrl)

		objId, objErr := primitive.ObjectIDFromHex(testSet.ID)
		if objErr != nil {
			fmt.Printf("Error in generating objectID err %+v", objErr)
		}
		testSet.ID = ""
		bsonDoc := ConvertStructToBsonInterface(testSet)
		updatedDocs = append(updatedDocs, mongo.NewReplaceOneModel().SetFilter(bson.M{"_id": objId}).SetReplacement(bsonDoc))
	}

	fmt.Printf("records update for oldPaperId %+v, length %+v \n", oldPaperID, len(updatedDocs))

	result, err := collection.BulkWrite(ctx, updatedDocs)
	if err != nil {
		fmt.Printf("Failed to update document Error: %v \n", err)
	}
	fmt.Printf(" Document Update Result oldPaperId %d MatchedCount:%d ModifiedCount:%d \n", oldPaperID, result.MatchedCount, result.ModifiedCount)
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

	// for i := param1; i <= param2; i = i + param3 {
	oldQuestionIDStart := param1
	oldQuestionIDEnd := param2
	mapQuestionRecords, _ := fetchQuestionRecords(db, oldQuestionIDStart, oldQuestionIDEnd)

	for key, mTestSet := range mapQuestionRecords {
		// fmt.Printf("Processing Question ID, %+v +%v \n", key, mTestSet)
		if err := updateMongo(collection, mTestSet.PaperID, mTestSet.ID, mTestSet); err != nil {
			fmt.Printf("Error in updating MongoDB for Question ID, %+v \n", key)
			continue
		}
	}

	// var wg sync.WaitGroup
	//
	// for j := oldQuestionIDStart; j <= oldQuestionIDEnd; j++ {
	//	wg.Add(1)
	//	go func(j int64) {
	//		defer wg.Done()
	//
	//		questionRecords, exist := mapQuestionRecords[j]
	//		if !exist {
	//			fmt.Printf("questionRecords not found Question ID, %+v", j)
	//		} else {
	//			updateMongo(collection, j, questionRecords)
	//		}
	//	}(j)
	// }
	//
	// wg.Wait()
	fmt.Printf("Data backfill completed for  %+v %+v \n", oldQuestionIDStart, oldQuestionIDEnd)
	// }

	// fmt.Println("Data backfill completed successfully!")
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
