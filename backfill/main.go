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
	collection := client.Database("qb").Collection("questions")
	return client, collection, nil
}

func fetchQuestionContent(db *sql.DB, oldQuestionIDStart, oldQuestionIDEnd int64) (map[int64][]MQuestionContent, error) {
	query := `
		select 
			qc.learning_objective,
			qc.mismatched,
			qc.display_answer,
			qc.display_options,
			qc.language,
			qc.qns_id
		 FROM question_content qc
		 LEFT JOIN questions q on q.id=qc.qns_id
		 WHERE q.id BETWEEN ? AND ?;  
	`

	rows, err := db.Query(query, oldQuestionIDStart, oldQuestionIDEnd)

	if err != nil {
		fmt.Printf("error in question content query %+v", err)
		return nil, err
	}
	defer rows.Close()

	records := make(map[int64][]MQuestionContent, 0)
	for rows.Next() {

		var rec MQuestionContent
		err := rows.Scan(&rec.LearningObjective, &rec.Mismatched, &rec.DisplayAnswer, &rec.DisplayOptions, &rec.Language, &rec.QnsID)
		if err != nil {
			fmt.Printf("error in question content %+v", err)
			return nil, err
		}
		records[rec.QnsID] = append(records[rec.QnsID], rec)
	}
	return records, nil
}

func fetchQuestionHashtags(db *sql.DB, oldQuestionIDStart, oldQuestionIDEnd int64) (map[int64][]MHashTags, error) {
	query := `
		select
    		h.hashtag,
    		h.description,
    		hr.question_id
		FROM question_hashtags h
		RIGHT JOIN question_hashtags_relation hr on hr.hashtag = h.id
		WHERE hr.question_id BETWEEN ? AND ?; 
	`

	rows, err := db.Query(query, oldQuestionIDStart, oldQuestionIDEnd)

	if err != nil {
		fmt.Printf("error in question hashtags query %+v", err)
		return nil, err
	}
	defer rows.Close()

	records := make(map[int64][]MHashTags, 0)

	for rows.Next() {
		var rec MHashTags
		err := rows.Scan(&rec.HashTag, &rec.Description, &rec.QuestionID)
		if err != nil {
			fmt.Printf("error in question hashtags %+v", err)
			return nil, err
		}
		records[rec.QuestionID] = append(records[rec.QuestionID], rec)
	}

	return records, nil
}

// Fetch records from MySQL in batches
func fetchQuestionRecords(db *sql.DB, oldQuestionIDStart, oldQuestionIDEnd int64) (map[int64]MQuestion, error) {

	query := `
       select
	    q.id,
	    q.video_solution_papercode,
	    q.source_material,
	    q.center_info_id,
	    q.dirty_level,
	    q.is_practice,
	    q.faculty_by,
	    q.extra_info,
	    q.duplicacy_status,
	    c.name,
	    qt.is_single_correct
	FROM questions q
	LEFT JOIN center_info c ON c.id = q.center_info_id
	LEFT JOIN question_type_info qt on qt.id = q.type
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
		err := rows.Scan(&rec.ID, &rec.VideoSolutionPaperCode, &rec.SourceMaterial, &rec.CenterInfoID, &rec.DirtyLevel,
			&rec.IsPractice, &rec.FacultyBy, &rec.ExtraInfo, &rec.DuplicacyStatus, &rec.CenterName, &rec.IsSingleCorrect)
		if err != nil {
			fmt.Printf("error in fetching question records %+v", err)
			return nil, err
		}
		records[int64(rec.ID)] = rec
	}
	return records, nil
}

// Fetch document from MongoDB and update it
func updateMongo(collection *mongo.Collection, oldQuestionID int64, questionContent []MQuestionContent, hashTags []MHashTags, question MQuestion) error {
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

	var hashTagsList []HashTags
	for _, hashtag := range hashTags {
		hashTagsList = append(hashTagsList, HashTags{
			HashTagID:   hashtag.HashTag,
			Description: convertString(hashtag.Description),
		})
	}

	var updatedDocs []mongo.WriteModel

	for _, ques := range resultList {
		ques.VideoSolutionPaperCode = convertString(question.VideoSolutionPaperCode)

		source := convertString(question.SourceMaterial)
		sourceVal, exist := QuestionSourceMap[source]
		if exist {
			ques.Source = sourceVal
		} else {
			ques.Source = 3
		}

		ques.SourceCenter = convertString(question.CenterName)
		ques.DirtyLevel = convertInt32(question.DirtyLevel)
		ques.IsPractice = convertInt32(question.IsPractice)
		ques.FacultyBy = convertString(question.FacultyBy)
		ques.ExtraInfo = convertString(question.ExtraInfo)

		dupStatus := convertString(question.DuplicacyStatus)
		val, exist := DuplicacyStatusMap[dupStatus]
		if exist {
			ques.DuplicacyStatus = val
		} else {
			ques.DuplicacyStatus = 0
		}
		ques.ExtraInfo = convertString(question.ExtraInfo)
		ques.IsSingleCorrect = convertInt32(question.IsSingleCorrect)

		ques.HashTags = hashTagsList

		var contentList []Content

		for _, content := range ques.Content {
			pos := -1
			for i, qnsContent := range questionContent {
				if content.Language == qnsContent.Language {
					pos = i
					break
				}
			}
			if pos != -1 {
				boolValue, _ := strconv.ParseBool(questionContent[pos].Mismatched)
				content.Mismatched = boolValue
				content.LearningObjectives = []string{convertString(questionContent[pos].LearningObjective)}
				content.DisplayAnswer = convertString(questionContent[pos].DisplayAnswer)
				content.DisplayOptions = convertString(questionContent[pos].DisplayOptions)
			}
			contentList = append(contentList, content)
		}
		ques.Content = contentList

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
		mapQuestionContent, _ := fetchQuestionContent(db, oldQuestionIDStart, oldQuestionIDEnd)
		mapQuestionHashtags, _ := fetchQuestionHashtags(db, oldQuestionIDStart, oldQuestionIDEnd)
		mapQuestionRecords, _ := fetchQuestionRecords(db, oldQuestionIDStart, oldQuestionIDEnd)

		var wg sync.WaitGroup

		for j := oldQuestionIDStart; j <= oldQuestionIDEnd; j++ {
			wg.Add(1)
			go func(j int64) {
				defer wg.Done()

				questionContent, exist := mapQuestionContent[j]
				if !exist {
					fmt.Printf("questionContent not found Question ID, %+v", j)
				}

				questionHashTags, exist := mapQuestionHashtags[j]
				if !exist {
					fmt.Printf("questionHashTags not found Question ID, %+v", j)
				}

				questionRecords, exist := mapQuestionRecords[j]
				if !exist {
					fmt.Printf("questionRecords not found Question ID, %+v", j)
				}

				updateMongo(collection, j, questionContent, questionHashTags, questionRecords)
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

var DuplicacyStatusMap = map[string]int32{
	"":                          0,
	"bin_of_questions":          1,
	"marked_dirty_by_retagging": 2,
	"repeated_by_retagging":     3,
	"ported-from-vendor":        4,
	"dirty":                     5,
	"fresh":                     6,
}

var QuestionSourceMap = map[string]int32{
	"":                                0,
	"race":                            1,
	"sheet":                           2,
	"test-paper":                      3,
	"sg":                              4,
	"practice-bank-pncf":              5,
	"Innovative":                      6,
	"work-from-home":                  7,
	"home-schooling":                  8,
	"mb":                              9,
	"adpl-test-paper":                 10,
	"star-iit":                        11,
	"ncert-medical":                   12,
	"external-material":               13,
	"Additional":                      14,
	"work-from-home-2":                15,
	"work-from-home-3":                16,
	"nats-ncert-accuracy-test-series": 17,
	"ja-books-questions":              18,
	"score-iit":                       19,
	"previous-year-papers":            20,
	"ncert-paragraph":                 21,
}
