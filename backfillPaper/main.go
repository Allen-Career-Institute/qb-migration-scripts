package main

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql" // Import the MySQL driver
	"go.mongodb.org/mongo-driver/bson"

	// "go.mongodb.org/mongo-driver/bson/primitive"
	"log"
	"os"
	"strconv"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	// "sync"
)

// MySQL and MongoDB Configurations
const (
	mysqlDSN    = "root:PCu2j8LNgQDLEdK@tcp(question-bank-php-ro.allen-internal-prod.in:3306)/question_pool"
	mongoURI    = "mongodb+srv://qb:EiGG1xOGtnulVkSA@learning-material-management-cluster-prod-cluster-pl-0.4dyev.mongodb.net"
	batchSize   = 1000
	workerCount = 5
)

func connectMySQL() (*sql.DB, error) {
	db, err := sql.Open("mysql", mysqlDSN)
	if err != nil {
		return nil, err
	}
	// db.SetMaxOpenConns(500)
	// db.SetMaxIdleConns(100)
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

func fetchSectionInfo(db *sql.DB, oldPaperId int64) (map[int64]SectionInfo, error) {
	query := `
		select
			qc.id,
			qc.parent_id,
			qc.sequence_id,
			qc.type,
			qc.omr_section,
			qc.marks_per_question,
			qc.neg_marks_per_question,
			qc.partial_marks_per_question,
			qc.partial_neg_marks_per_question
		 FROM section_info qc
		 WHERE qc.paper_id = ?
		 ORDER BY qc.id;
	`

	rows, err := db.Query(query, oldPaperId)

	if err != nil {
		fmt.Printf("error in fetchSectionInfo query %+v \n", err)
		return nil, err
	}
	defer rows.Close()

	records := make(map[int64]SectionInfo, 0)
	for rows.Next() {
		var rec SectionInfo
		err1 := rows.Scan(&rec.ID, &rec.ParentID, &rec.SequenceID, &rec.Type, &rec.OmrSection, &rec.MarksPerQuestion,
			&rec.NegMarksPerQuestion, &rec.PartialMarksPerQuestion, &rec.PartialNegMarksPerQuestion)
		if err1 != nil {
			fmt.Printf("error in fetchSectionInfo %+v \n", err1)
			return nil, err1
		}
		records[rec.ID] = rec
	}
	return records, nil
}

func fetchSharedPaperCenter(db *sql.DB, oldPaperId int64) ([]SharedPaperCenter, error) {
	query := `
		select
   		spc.center_id,
   		spc.from_center_id,
   		spc.shared_by,
        spc.shared_date,
        spc.status,
        spc.share_type
	FROM shared_paper_center spc
	WHERE spc.paper_id = ?;
	`

	rows, err := db.Query(query, oldPaperId)

	if err != nil {
		fmt.Printf("error in shared paper center query %d %+v", oldPaperId, err)
		return nil, err
	}
	defer rows.Close()

	records := make([]SharedPaperCenter, 0)

	for rows.Next() {
		var rec SharedPaperCenter
		err := rows.Scan(&rec.CenterId, &rec.FromCenterId, &rec.SharedBy, &rec.Date, &rec.Status, &rec.SharedType)
		if err != nil {
			fmt.Printf("error in shared paper center %d %+v", oldPaperId, err)
			return nil, err
		}
		records = append(records, rec)
	}
	return records, nil
}

// Fetch records from MySQL in batches
func fetchPaperRecords(db *sql.DB, oldPaperId int64) (*PaperInfo, error) {

	query := `
       select
	    p.id,
	    p.platform,
	    p.master_phase,
	    p.paper_phase,
	    p.layout,
	    p.paper_watermark,
	    p.paper_no,
	    p.test_no,
	    p.passkey,
	    p.type,
	    p.center_info_id,
	    c.name,
	    tm.name
	FROM paper_info p
	LEFT JOIN center_info c ON c.id = p.center_info_id
	LEFT JOIN paper_test_modes tm on tm.id = p.paper_test_modes_id
	WHERE p.id = ?;
    `

	rows, err := db.Query(query, oldPaperId)

	if err != nil {
		fmt.Printf("error in paper_info query %+v", err)
		return nil, err
	}
	defer rows.Close()

	// records := make(map[int64]PaperInfo, 0)
	var rec PaperInfo

	for rows.Next() {
		err := rows.Scan(&rec.ID, &rec.Platform, &rec.MasterPhase, &rec.PaperPhase, &rec.Layout,
			&rec.PaperWatermark, &rec.PaperNo, &rec.TestNo, &rec.Passkey, &rec.Method, &rec.CenterInfoID, &rec.CenterName, &rec.TestModeName)
		if err != nil {
			fmt.Printf("error in fetching paper_info records %+v", err)
			return nil, err
		}
		// records[rec.ID] = rec
	}
	return &rec, nil
}

func fetchFacultyForQuestions(db *sql.DB, oldPaperId int64) (map[int64]string, error) {

	query := `
       select
	    p.paper_id,
	    p.question_id,
	    p.faculty_id
	FROM paper_section_questions p
	WHERE p.paper_id = ?;
	`

	rows, err := db.Query(query, oldPaperId)

	if err != nil {
		fmt.Printf("error in paper_info query %+v", err)
		return nil, err
	}
	defer rows.Close()

	records := make(map[int64]string, 0)

	for rows.Next() {
		var rec PaperSectionQuestion
		err := rows.Scan(&rec.PaperID, &rec.QuestionID, &rec.FacultyID)
		if err != nil {
			fmt.Printf("error in fetching paper_info records %+v", err)
			return nil, err
		}
		if rec.QuestionID != nil {
			records[convertInt64(rec.QuestionID)] = convertString(rec.FacultyID)
		}
	}
	return records, nil
}

func FetchQuestionIDsByOldQuestionIDs(ctx context.Context, collection *mongo.Collection, oldQuestionIDs []int64) (map[int64]string, error) {
	result := make(map[int64]string)
	resultMutex := sync.Mutex{}

	// Use a semaphore to limit concurrent queries (e.g., 10 concurrent queries)
	semaphore := make(chan struct{}, 10)
	errChan := make(chan error, len(oldQuestionIDs))
	var wg sync.WaitGroup

	for _, oldQuestionID := range oldQuestionIDs {
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			semaphore <- struct{}{}        // Acquire semaphore
			defer func() { <-semaphore }() // Release semaphore

			// Query for each oldQuestionId, sort by version desc, limit to 1
			filter := bson.M{"oldQuestionId": id}
			projection := bson.M{"questionId": 1, "oldQuestionId": 1}
			opts := options.FindOne().
				SetProjection(projection).
				SetSort(bson.M{"version": -1}) // Get latest version first

			var doc struct {
				QuestionID    string `bson:"questionId"`
				OldQuestionID int64  `bson:"oldQuestionId"`
			}

			err := collection.FindOne(ctx, filter, opts).Decode(&doc)
			if err != nil {
				if err == mongo.ErrNoDocuments {
					// Skip if no document found for this oldQuestionId
					return
				}
				errChan <- err
				return
			}

			resultMutex.Lock()
			result[doc.OldQuestionID] = doc.QuestionID
			resultMutex.Unlock()
		}(oldQuestionID)
	}

	wg.Wait()
	close(errChan)

	// Check if any errors occurred
	if len(errChan) > 0 {
		return nil, <-errChan
	}

	return result, nil
}

// Fetch document from MongoDB and update it
func updateMongo(collection *mongo.Collection, oldPaperId int64, paperInfo PaperInfo, newQuestionToFacultyID map[string]string, sharedList []SharedPaperCenter, namespaceToSectionInfoMap map[string]SectionInfo, isDryRun bool) error {
	ctx := context.TODO()

	// Find document in MongoDB
	filter := bson.M{"oldPaperId": oldPaperId} // Assuming MySQL ID matches MongoDB document _id
	cursor, err := collection.Find(ctx, filter)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			fmt.Printf("No document found for oldPaperId: %d \n", oldPaperId)
			return nil
		}
		fmt.Printf(" updateMongo |  oldPaperId: %d, err %+v \n", oldPaperId, err)
		return err
	}

	if cursor == nil || cursor.RemainingBatchLength() == 0 {
		fmt.Println("cursor is 0 for oldPaperId ", oldPaperId)
		return nil
	}

	var resultList []QuestionSetDocument

	for cursor.Next(ctx) {
		questionDoc := &NewQuestionSetDocument{}
		if err = cursor.Decode(&questionDoc); err != nil {
			fmt.Printf("Error decoding document err: %+v \n", err)
			continue
		}

		resultList = append(resultList, ConvertNewToOld(*questionDoc))
	}

	fmt.Printf("oldPaperId ID %+v result List %+v \n", oldPaperId, len(resultList))

	var sharedInfoList []SharedInfo
	for _, sharedInfo := range sharedList {
		center := ""
		centerID := convertInt64(sharedInfo.CenterId)
		centerVal, exist := CentreIDMapProd[centerID]
		if exist {
			center = centerVal
		}

		fromCenter := ""
		fromCenterID := convertInt64(sharedInfo.FromCenterId)
		fromCenterVal, exist := CentreIDMapProd[fromCenterID]
		if exist {
			fromCenter = fromCenterVal
		}

		date, err := time.Parse("2006-01-02 15:04:05", convertString(sharedInfo.Date)) // Adjust the layout to match your date format
		if err != nil {
			fmt.Printf("Error parsing date: %v\n", err)
			continue
		}
		timestamp := date.Unix()

		var sharedType int32
		sharedStatusVal, exist := SharedTypeMap[convertString(sharedInfo.SharedType)]
		if exist {
			sharedType = sharedStatusVal
		} else {
			sharedType = 0
		}

		status := convertInt32(sharedInfo.Status)
		if status != 1 {
			status = 0
		}

		sharedInfoList = append(sharedInfoList, SharedInfo{
			SharedBy:     convertString(sharedInfo.SharedBy),
			CenterId:     center,
			FromCenterId: fromCenter,
			Date:         timestamp,
			Status:       status,
			SharedType:   sharedType,
		})
	}

	var updatedDocs []mongo.WriteModel

	for _, paper := range resultList {

		paper.Platform = convertString(paperInfo.Platform)
		paper.MasterPhase = convertString(paperInfo.MasterPhase)
		paper.PaperPhase = convertString(paperInfo.PaperPhase)
		paper.Watermark = convertString(paperInfo.PaperWatermark)
		paper.PaperNo = convertInt32(paperInfo.PaperNo)
		paper.TestNo = convertInt32(paperInfo.TestNo)
		paper.PassKey = convertString(paperInfo.Passkey)
		paper.Method = convertString(paperInfo.Method)
		paper.CenterName = convertString(paperInfo.CenterName)
		paper.TestMode = convertString(paperInfo.TestModeName)

		layout := convertString(paperInfo.Layout)
		sourceVal, exist := LayoutMap[layout]
		if exist {
			paper.Layout = sourceVal
		} else {
			paper.Layout = 0
		}

		centerID := convertInt64(paperInfo.CenterInfoID)
		centerVal, exist := CentreIDMapProd[centerID]
		if exist {
			paper.CenterId = centerVal
		} else {
			paper.CenterId = ""
		}

		paper.SharedInfo = sharedInfoList

		paper.FillFacultyIds(newQuestionToFacultyID, namespaceToSectionInfoMap)

		bsonDoc := ConvertStructToBsonInterface(paper)
		updatedDocs = append(updatedDocs, mongo.NewReplaceOneModel().SetFilter(bson.M{"_id": paper.QuestionSetID}).SetReplacement(bsonDoc))
	}

	fmt.Printf("records update for oldPaperId %+v, length %+v \n ", oldPaperId, len(updatedDocs))

	if isDryRun {
		fmt.Printf("=== DRY RUN MODE - NO ACTUAL CHANGES WILL BE MADE FOR oldPaperId %d ===", oldPaperId)
		return nil
	}

	result, err := collection.BulkWrite(ctx, updatedDocs)
	if err != nil {
		fmt.Printf("Failed to update oldPaperId %d document Error: %v\n", oldPaperId, err)
	}
	fmt.Printf(" Document Update Result oldPaperId %d MatchedCount:%d ModifiedCount:%d \n", oldPaperId, result.MatchedCount, result.ModifiedCount)
	return nil
}

func main() {

	args := os.Args

	// Check if parameters are passed
	if len(args) < 4 {
		fmt.Println("Usage: ./main <start_id> <end_id> <batch_size> [dry-run]")
		fmt.Println("Add 'dry-run' as 4th parameter to simulate without database updates")
		return
	}
	param1, err := strconv.ParseInt(args[1], 10, 64) // First parameter
	param2, err := strconv.ParseInt(args[2], 10, 64) // Second parameter
	param3, err := strconv.ParseInt(args[3], 10, 64) // Third parameter

	// Check for dry run mode
	isDryRun := len(args) > 4 && args[4] == "dry-run"

	// Print parameters
	fmt.Println("Start OldQuestionID Parameter 1:", param1)
	fmt.Println("End OldQuestionID Parameter 2:", param2)
	fmt.Println("BatchSize Parameter 3: ", param3)
	if isDryRun {
		fmt.Println("=== DRY RUN MODE - NO DATABASE UPDATES WILL BE MADE ===")
	}

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

	paperCollection := client.Database("qb").Collection("questionSets")

	// for newID, facultyID := range newQuestionIDToFacultyIDMap {
	// 	fmt.Printf("New Question ID: %s, Faculty ID: %s\n", newID, facultyID)
	// }

	for i := param1; i <= param2; i = i + param3 {
		oldPaperIDStart := i
		oldPaperIDEnd := i + param3 - 1

		var wg sync.WaitGroup

		for j := oldPaperIDStart; j <= oldPaperIDEnd; j++ {
			wg.Add(1)
			go func(j int64) {
				defer wg.Done()

				sharedPaperCenter, err1 := fetchSharedPaperCenter(db, j)
				if err1 != nil {
					fmt.Printf("Error fetching fetchSharedPaperCenter IDs: %d %+v \n", j, err1)
				}
				oldToFacultyIDMap, err2 := fetchFacultyForQuestions(db, j)
				if err2 != nil {
					fmt.Printf("Error fetching fetchFacultyForQuestions IDs: %d %+v \n", j, err2)
				}

				keys := make([]int64, 0, len(oldToFacultyIDMap))
				for key := range oldToFacultyIDMap {
					keys = append(keys, key)
				}
				oldToNewQuestionIDMap, err3 := FetchQuestionIDsByOldQuestionIDs(context.Background(), collection, keys)
				if err3 != nil {
					fmt.Printf("Error fetching FetchQuestionIDsByOldQuestionIDs IDs:%d %+v \n", j, err3)
				}
				newQuestionIDToFacultyIDMap := make(map[string]string)
				for oldID, newID := range oldToNewQuestionIDMap {
					if facultyID, exists := oldToFacultyIDMap[oldID]; exists {
						// fmt.Println(facultyID)
						newQuestionIDToFacultyIDMap[newID] = facultyID
					}
				}
				paperInfo, err4 := fetchPaperRecords(db, j)
				if err4 != nil {
					fmt.Printf("Error fetching fetchPaperRecords IDs:%d %+v \n", j, err4)
				}
				sectionIDToSectionInfoMap, err5 := fetchSectionInfo(db, j)
				if err5 != nil {
					fmt.Printf("Error fetching fetchSectionInfo IDs:%d %+v \n", j, err5)
				}
				var sectionList []SectionInfo
				for _, value := range sectionIDToSectionInfoMap {
					sectionList = append(sectionList, value)
				}

				sectionIDToNamespaceMap := generateHierarchy(sectionList)

				namespaceToSectionInfoMap := make(map[string]SectionInfo)
				for key, value := range sectionIDToNamespaceMap {
					namespaceToSectionInfoMap[value] = sectionIDToSectionInfoMap[key]
				}

				updateMongo(paperCollection, j, *paperInfo, newQuestionIDToFacultyIDMap, sharedPaperCenter, namespaceToSectionInfoMap, isDryRun)

			}(j)
		}

		wg.Wait()
		fmt.Printf("Data backfill completed for  %+v %+v \n", oldPaperIDStart, oldPaperIDEnd)
	}

	if isDryRun {
		fmt.Println("=== DRY RUN COMPLETED - NO ACTUAL CHANGES WERE MADE ===")
	} else {
		fmt.Println("Data backfill completed successfully!")
	}
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

var LayoutMap = map[string]int32{
	"":       0,
	"single": 1,
	"multi":  2,
}

var SharedTypeMap = map[string]int32{
	"":       0,
	"user":   1,
	"center": 2,
}

var SectionTypeMap = map[string]int32{
	"":              0,
	"exercise":      1,
	"beginners_box": 2,
	"default":       3,
}

var CentreIDMapProd = map[int64]string{
	1:  "fa_xFoPmDT8HQJEOVoaYMLao",       // Kota
	4:  "fa_JpapKNtNv8KmNQguuKzcS",       // Jaipur
	5:  "fa_5bisOGusZBQU5T7JeDClL",       // CHANDIGARH
	6:  "fa_b7RASkljhNKRMwqHgyZqW",       // AHMEDABAD
	7:  "fa_GNecuMHJa8WqWZW1HVNzQ",       // Bangalore
	8:  "fa_N1q5vkUK4lhtq3Waeo4Xc",       // INDORE
	9:  "fa_7INaRfr6lRjgeI0DYIMos",       // MUMBAI
	10: "fa_wNWHgjh27xinnkQgYCcUq",       // SIKAR
	11: "fa_dTM28xiy5gLn8qfEFuFpQ",       // BHUBANESWAR
	12: "fa_ZldPDkCnb0Gd5IptNS3Ww",       // NAGPUR
	13: "fa_4XnUbXzQP3NwyGVQLyhoU",       // GUWAHATI
	14: "fa_STzSZKJiYMsMaGeF2cE1w",       // PUNE
	15: "fa_ZND28vyu597O1Rn1ur6tF",       // CHENNAI
	16: "fa_1VnCcfHBhiUmBsmpD9Hmo",       // KOCHI
	17: "fa_zCvBz94veZMeBdqyZV6xW",       // PUDUCHERRY
	18: "fa_TcN0X6aRFh3ewyobLX1Im",       // VADODARA
	19: "fa_HqxyCRIEg5VhCAdUZbOAk",       // SURAT
	20: "fa_SDpG3EhoatvksOP3EuIwc",       // RAJKOT
	21: "fa_3CPi4CkUFYPkblIfAMcwO",       // OVERSEAS
	22: "fa_jVmMqyCuG851hyvFIjfoP",       // BHILWARA
	23: "fa_KJdPGfHy8dp4f4qD05lZt",       // JAMMU
	24: "fa_yNrBViSDBxbEEUWtF0kH2",       // SRINAGAR
	25: "fa_6p9Zl4EAPM4280u9bvfZY",       // NANDED
	26: "fa_sIGWEtRjgyRE9mqLznPTp",       // RANCHI
	27: "fa_V9CKzsYM7PaygD38FdTJF",       // DEHRADUN
	28: "fa_Cu6vxx16eR5yFfsWqcEkt",       // DOHA
	30: "fa_O8rvB5cfa3zzV32p7PUJu",       // RAIPUR
	31: "facility_i72dRs7V6bscZL88LytGC", // Adpl
	32: "fa_LUsSAKvJX6o9aUcv8wrte",       // DURGAPUR
	33: "fa_pSqjO5V75X0uNW0R8uTwi",       // TIRUPATI
	34: "fa_QegcsZGGBLNpskrAhdk4b",       // PAAVAI
	35: "fa_B76iRFUWBmPSw1M9ucbc1",       // MYSURU
	36: "fa_EF9Q4LSnXGHUdK1twbQB0",       // MANGALURU
	40: "fa_dg6x3NTpi85zx0vppfs2q",       // BHOPAL
	41: "fa_H8T4XLAeyaXR98Tdr8wq3",       // UJJAIN
	42: "fa_kndxbhEyYKMxspymZ8jsQ",       // RAWATBHATA
	43: "fa_DAgUUunJZuyTlRfkiWG7O",       // GWALIOR
	44: "fa_9OG7nZ9AZuy0JCqqcsbnh",       // UDAIPUR
	45: "fa_VPuVoqenxS8FOJDGsV947",       // HISAR
	46: "fa_IOpdoY4ncv76tAR3SsT2q",       // SILIGURI
	48: "fa_LKyn0cMbwYmZ8xrQMuNQa",       // JODHPUR
	51: "fa_9Bj55lxKsK3AMVELruxs2",       // NASHIK
	52: "fa_di026G7uNKSxMHixfitnV",       // CHURU-TARANAGAR
	53: "fa_UZQ8qdR8sgF3Ec3UAdfDy",       // COIMBATORE
	57: "fa_lQ8ziWxkMkg6VIQgJ1k9C",       // DELHI
	58: "fa_9aG4sZJn8ePsCwe34QJhz",       // KOLKATA
	59: "fa_eI8DU91zi2GRAxPZq0dYG",       // BATHINDA
	60: "fa_VgiD695W5ncpzrVZKw5Hu",       // AMRITSAR
	61: "fa_ReRvoWfkAeL7DUQLAleE3",       // BILASPUR
	62: "fa_iW2cT0Ihz3v4NGrq0QYeF",       // DIBRUGARH
	65: "fa_oGyQsuHHvc6HaIhh9tCVy",       // ROHTAK
	66: "fa_nVROlbzb1gzaeBwsQ87hK",       // PATNA

}

var CentreIDMapStage = map[int64]string{
	1:  "fa_G9yc0Fl6VnzS8x9Sd7Fob",       // KOTA
	4:  "fa_aFeKnUZesYxhA0GGRMe5o",       // JAIPUR
	5:  "fa_qArrX2X2pHKWyzf6wVRO2",       // CHANDIGARH
	6:  "fa_sUl6pYlwSQzuueLS8nFwg",       // AHMEDABAD
	7:  "fa_vPrMOk5KHSdpGoDw82rbi",       // BENGALURU
	8:  "fa_IIOwtXXJQp0kFgyaZIS8q",       // INDORE
	9:  "fa_M5sfYXcRjXbbOyD7wKslh",       // MUMBAI
	10: "fa_iBnNkKjBTA3w6aM6fODmC",       // SIKAR
	11: "fa_NZdRmn1i4pl43DlUpwfW1",       // BHUBANESWAR
	12: "fa_iOnG8ExYdiTFBECLmyf2K",       // NAGPUR
	13: "fa_SHzltNXnfniY2A6vDXcyx",       // GUWAHATI
	14: "fa_e7WavdryHxW6M0tCWfuMp",       // PUNE
	15: "fa_UtXWmsrLKCRHJRqQjVetk",       // CHENNAI
	16: "fa_jt3LMbAZDp4OS24CdzmHn",       // KOCHI
	17: "fa_L0dyoYkmHpFNXf4xwbBs7",       // PUDUCHERRY
	18: "fa_xGvlG7bkVcYcKHTjq9JST",       // VADODARA
	19: "fa_D6yjmtgeFofQhK5kXQcrF",       // SURAT
	20: "fa_cIAOUWQRQ2GfzyEJPdVbG",       // RAJKOT
	21: "fa_nX9YnmiJU63U2IoLJASyN",       // OVERSEAS
	22: "fa_AcAtBhrMVktv7eF2WkPCZ",       // BHILWARA
	23: "fa_wNe7JHR4qGBQVY3xJs5R9",       // JAMMU
	24: "fa_75FDEOn4J0fmo5Cv15qAi",       // SRINAGAR
	25: "fa_tgT0BrUOVKyLErSDmYWWv",       // NANDED
	26: "fa_qWNv1uv6VCK5t4dQN5nkz",       // RANCHI
	27: "fa_eDWtDUafulJsrjYmIW4eR",       // DEHRADUN
	28: "fa_G39AcAhvUZCg7jc2wcA2W",       // DOHA
	30: "fa_4heolVE0N1XCbFLRBScOy",       // RAIPUR
	31: "facility_j1IwGFcbbdl3LSALAsGIX", // ADPL
	32: "fa_Y279yl21y1O7Kj55brdIB",       // DURGAPUR
	33: "fa_YpVqZtAHQkmY2NjAsSt9m",       // TIRUPATI
	34: "fa_sPuAIrZWuixmzMIAxijL4",       // PAAVAI
	35: "fa_aXAM8HO0gxSJhAwugZwWg",       // MYSURU
	36: "fa_B76iRFUWBmPSw1M9ucbc1",       // MANGALURU
	40: "fa_iOnG8ExYdiTFBECLmyf2K",       // BHOPAL
	41: "fa_SzvUbLCI5OoJAZEEIXK8P",       // UJJAIN
	42: "fa_KYyYG7oizad1ShEVuM6Fk",       // RAWATBHATA
	43: "fa_IACuCF467QlXy2r2TORV0",       // GWALIOR
	44: "fa_jHutuhobBz7OWxLP4V6Za",       // UDAIPUR
	45: "fa_7IbGyYywxMmAeVuuvtV6S",       // HISAR
	46: "fa_FnOy321gH0WeuDNlM5sOv",       // SILIGURI
	48: "fa_dp1NnpSfWOHxQGgzz3Sxk",       // JODHPUR
	51: "fa_zdo4ZUNTaZgdKHcanHifX",       // NASHIK
	52: "fa_YyQOHvys3ecxr7qUqqK71",       // CHURU-TARANAGAR
	53: "fa_nr2HApoF1L5nOFrW0jaBZ",       // COIMBATORE
	57: "fa_t2s0B5H35nhSX5JEhShsQ",       // DELHI
	58: "fa_NfkUbYKrHUj7sjCaxpSON",       // KOLKATA
	59: "fa_ClQvx1p1tWR7QJD4Da4wq",       // BATHINDA
	60: "fa_vhOdT5D9NiQKo8zTwxT8G",       // AMRITSAR
	61: "fa_qArrX2X2pHKWyzf6wVRO2",       // BILASPUR
	62: "fa_M1pqD8PJUNUZwFzpQZ6UY",       // DIBRUGARH
	65: "fa_qRkMtcf6W7Ep8NFVOA7dK",       // ROHTAK
	66: "fa_1XyzA3w5RH0rWE67oRzmD",       // PATNA
}

func (qsd *QuestionSetDocument) FillFacultyIds(facultyMap map[string]string, nodeMap map[string]SectionInfo) {
	for i := range qsd.Questions {
		if facultyId, ok := facultyMap[qsd.Questions[i].QuestionID]; ok {
			qsd.Questions[i].FacultyId = facultyId
		}
	}
	for i := range qsd.QuestionSetSections {
		qsd.QuestionSetSections[i].fillFacultyIdsInSection(facultyMap, nodeMap)
	}
}

func (qss *QuestionSetSection) fillFacultyIdsInSection(facultyMap map[string]string, nodeMap map[string]SectionInfo) {
	if sectionValue, ok := nodeMap[qss.Namespace]; ok {
		qss.OmrSection = convertString(sectionValue.OmrSection)
		sectionType, exist := SectionTypeMap[convertString(sectionValue.Type)]
		if exist {
			qss.Type = sectionType
		} else {
			qss.Type = 0
		}

		qss.ParentSectionID = convertInt64(sectionValue.ParentID)
		qss.SectionID = sectionValue.ID

		qss.MarkingSchemePerQuestion = MarkingSchemePerQuestion{
			NegMarks:            sectionValue.NegMarksPerQuestion,
			CorrectMarks:        sectionValue.MarksPerQuestion,
			PartialNegMarks:     sectionValue.PartialNegMarksPerQuestion,
			PartialCorrectMarks: sectionValue.PartialMarksPerQuestion,
		}
	}

	for i := range qss.Questions {
		if facultyId, ok := facultyMap[qss.Questions[i].QuestionID]; ok {
			qss.Questions[i].FacultyId = facultyId
		}
	}
	for i := range qss.Subsections {
		qss.Subsections[i].fillFacultyIdsInSection(facultyMap, nodeMap)
	}
}

func generateHierarchy(nodes []SectionInfo) map[int64]string {
	hierarchy := make(map[int64]string)        // Stores ID -> Hierarchical Number mapping
	parentMap := make(map[int64][]SectionInfo) // Maps ParentID -> List of child nodes

	// Step 1: Group nodes by ParentID
	for _, node := range nodes {
		parentMap[convertInt64(node.ParentID)] = append(parentMap[convertInt64(node.ParentID)], node)
	}

	// Step 2: Sort children within each parent group by ID to ensure consistent ordering
	for parentID := range parentMap {
		children := parentMap[parentID]
		// Sort by ID to ensure consistent processing order
		sort.Slice(children, func(i, j int) bool {
			return children[i].ID < children[j].ID
		})
		parentMap[parentID] = children
	}

	// Step 3: Process hierarchy recursively
	var assignNumbering func(parentID int64, prefix string)

	assignNumbering = func(parentID int64, prefix string) {
		children, exists := parentMap[parentID]
		if !exists {
			return
		}

		// Handle NULL sequence_ids by assigning incremental values
		nullSequenceCounter := int64(1)
		usedSequences := make(map[int64]bool)

		// First pass: collect all non-NULL sequence IDs
		for _, child := range children {
			if child.SequenceID != nil {
				seq := convertInt64(child.SequenceID)
				if seq > 0 {
					usedSequences[seq] = true
				}
			}
		}

		for _, child := range children {
			var sequenceID int64

			if child.SequenceID == nil {
				// Assign next available sequence number for NULL values
				for usedSequences[nullSequenceCounter] {
					nullSequenceCounter++
				}
				sequenceID = nullSequenceCounter
				usedSequences[nullSequenceCounter] = true
				nullSequenceCounter++
			} else {
				sequenceID = convertInt64(child.SequenceID)
				if sequenceID == 0 { // Handle case where convertInt64 returns 0
					for usedSequences[nullSequenceCounter] {
						nullSequenceCounter++
					}
					sequenceID = nullSequenceCounter
					usedSequences[nullSequenceCounter] = true
					nullSequenceCounter++
				}
			}

			// Create new number by appending SequenceID
			newNumber := prefix
			if prefix == "" {
				newNumber += strconv.FormatInt(sequenceID, 10) // Root level
			} else {
				newNumber += "." + strconv.FormatInt(sequenceID, 10) // Child level
			}

			// Store the computed numbering
			hierarchy[child.ID] = newNumber

			// Recursively process child nodes
			assignNumbering(child.ID, newNumber)
		}
	}

	// Step 4: Start numbering for root nodes (parent_id == 0)
	assignNumbering(0, "")

	return hierarchy
}
