package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	MONGO_URI      = ""
	MONGO_DB       = "qb"
	QUESTIONS_COLL = "questions"
	SOLUTIONS_COLL = "questionSolutions"

	OS_ENDPOINT   = "https://parmanu-opensearch.allen-stage.in"
	OS_USERNAME   = "admin"
	OS_PASSWORD   = "Y5aT8gpe6P051vjeL67F"
	OS_INDEX_NAME = "questions_index"

	WORKERS_DEFAULT   = 20
	BULK_SIZE_DEFAULT = 100
)

// Connect to MongoDB
func connectMongo() (*mongo.Client, *mongo.Collection, *mongo.Collection, error) {
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(MONGO_URI))
	if err != nil {
		return nil, nil, nil, err
	}
	db := client.Database(MONGO_DB)
	questionsCollection := db.Collection(QUESTIONS_COLL)
	solutionsCollection := db.Collection(SOLUTIONS_COLL)
	return client, questionsCollection, solutionsCollection, nil
}

type QuestionDoc struct {
	QuestionID       string `bson:"questionId,omitempty"`
	OldQuestionID    int64  `bson:"oldQuestionId,omitempty"`
	Version          int64  `bson:"version,omitempty"`
	Status           int32  `bson:"status,omitempty"`
	Type             int32  `bson:"type,omitempty"`
	QnsLevel         int32  `bson:"qnsLevel,omitempty"`
	Session          int64  `bson:"session,omitempty"`
	Source           int32  `bson:"source,omitempty"`
	SourceCenter     string `bson:"sourceCenter,omitempty"`
	UniqueIdentifier string `bson:"uniqueIdentifier,omitempty"`
	Content          []struct {
		Language        int32 `bson:"language,omitempty"`
		QuestionNature  int32 `bson:"questionNature,omitempty"`
		HasTextSolution bool  `bson:"hasTextSolution,omitempty"`
	} `bson:"content,omitempty"`
	TaxonomyData []TaxonomyData `bson:"taxonomyData,omitempty"`
	OldTags      []Tag          `bson:"oldTags,omitempty"`
	CustomTags   []CustomTag    `bson:"customTags,omitempty"`
	HashTags     []HashTags     `bson:"hashTags,omitempty"`
	CreatedAt    int64          `bson:"createdAt"`
	UpdatedAt    int64          `bson:"updatedAt"`
}

type CustomTag struct {
	TagName string `bson:"tag_name,omitempty"`
	Value   string `bson:"value,omitempty"`
	TagType string `bson:"tag_type,omitempty"`
}

type TaxonomyData struct {
	TaxonomyId   string `bson:"taxonomyId,omitempty"`
	ClassId      string `bson:"classId,omitempty"`
	SubjectId    string `bson:"subjectId,omitempty"`
	TopicId      string `bson:"topicId,omitempty"`
	SubTopicId   string `bson:"subtopicId,omitempty"`
	SuperTopicId string `bson:"supertopicId,omitempty"`
	ConceptId    string `bson:"conceptId,omitempty"`
}

type HashTags struct {
	HashTagID   string `bson:"hashTagId,omitempty"`
	Description string `bson:"description,omitempty"`
}

type TextSolutionDocument struct {
	Language int32  `bson:"language,omitempty"`
	Text     string `bson:"text,omitempty"`
}

type VideoSolutionDocument struct {
	VTag  string `bson:"vTag,omitempty"`
	VTag2 string `bson:"vTag2,omitempty"`
}

type QuestionSolutionDoc struct {
	OldQuestionID  int64                    `bson:"oldQuestionId,omitempty"`
	TextSolutions  []*TextSolutionDocument  `bson:"textSolutions,omitempty"`
	VideoSolutions []*VideoSolutionDocument `bson:"videoSolutions,omitempty"`
}

// Align with backfill/question.go Tag definition
// name/value pairs e.g., streamName, className, subjectName, topicName, subTopicName, taxonomyId
type Tag struct {
	Name  string `bson:"name" json:"name"`
	Value string `bson:"value" json:"value"`
}

type BackfillConfig struct {
	MongoURI     string
	MongoDB      string
	QuestionsCol string
	SolutionsCol string
	IndexName    string
	StartID      int64
	EndID        int64
	ChunkSize    int64
	Workers      int
	BulkSize     int
	OSEndpoint   string
	OSUser       string
	OSPass       string
	OutFile      string
}

func main() {
	args := os.Args
	if len(args) < 4 {
		fmt.Println("Usage: ./opensearch-backfill <startOldQuestionId> <endOldQuestionId> <batchSize>")
		return
	}
	startID, _ := strconv.ParseInt(args[1], 10, 64)
	endID, _ := strconv.ParseInt(args[2], 10, 64)
	batchSize, _ := strconv.ParseInt(args[3], 10, 64)

	fmt.Println("Start OldQuestionID Parameter 1:", startID)
	fmt.Println("End OldQuestionID Parameter 2:", endID)
	fmt.Println("BatchSize Parameter 3:", batchSize)

	cfg := BackfillConfig{
		MongoURI:     MONGO_URI,
		MongoDB:      MONGO_DB,
		QuestionsCol: QUESTIONS_COLL,
		SolutionsCol: SOLUTIONS_COLL,
		IndexName:    OS_INDEX_NAME,
		StartID:      startID,
		EndID:        endID,
		ChunkSize:    batchSize,
		Workers:      int(batchSize), // batchSize = number of GoRoutines
		BulkSize:     BULK_SIZE_DEFAULT,
		OSEndpoint:   OS_ENDPOINT,
		OSUser:       OS_USERNAME,
		OSPass:       OS_PASSWORD,
		OutFile:      "",
	}

	ctx := context.Background()

	// Connect to MongoDB
	client, questions, solutions, err := connectMongo()
	if err != nil {
		log.Fatal("MongoDB Connection Error:", err)
		fmt.Println("MongoDB Connection Error:", err)
		return
	}
	defer client.Disconnect(ctx)

	out := make(chan map[string]interface{}, cfg.Workers*cfg.BulkSize)

	// bulk writer
	var writerWg sync.WaitGroup
	writerWg.Add(1)
	go func() {
		defer writerWg.Done()
		bulkIndex(ctx, cfg, out)
	}()

	// Process in batches like backfill/main.go
	for i := startID; i <= endID; i = i + batchSize {
		oldQuestionIDStart := i
		oldQuestionIDEnd := i + batchSize - 1

		var wg sync.WaitGroup

		for j := oldQuestionIDStart; j <= oldQuestionIDEnd; j++ {
			wg.Add(1)
			go func(j int64) {
				defer wg.Done()
				processQuestion(ctx, questions, solutions, j, out)
			}(j)
		}

		wg.Wait()
		fmt.Printf("Data backfill completed for %d %d \n", oldQuestionIDStart, oldQuestionIDEnd)
	}

	close(out)
	writerWg.Wait()

	fmt.Println("Data backfill completed successfully!")
}

func processQuestion(ctx context.Context, questions, solutions *mongo.Collection, oldQuestionID int64, out chan<- map[string]interface{}) {
	// Find the latest version of the question
	filter := bson.M{"oldQuestionId": oldQuestionID}
	opts := options.FindOne().SetSort(bson.D{{Key: "version", Value: -1}})

	var questionDoc QuestionDoc
	err := questions.FindOne(ctx, filter, opts).Decode(&questionDoc)
	if err != nil {
		if err != mongo.ErrNoDocuments {
			log.Printf("Error finding question %d: %v", oldQuestionID, err)
		}
		return
	}

	// Fetch solution
	solution := fetchSolution(ctx, solutions, oldQuestionID)

	// Build OpenSearch document
	doc := buildOSDoc(questionDoc, solution)
	out <- doc
}

func fetchSolution(ctx context.Context, solutions *mongo.Collection, oldQID int64) *QuestionSolutionDoc {
	filter := bson.M{"oldQuestionId": oldQID}
	find := options.FindOne().SetSort(bson.D{{Key: "versionId", Value: -1}})
	var sol QuestionSolutionDoc
	res := solutions.FindOne(ctx, filter, find)
	if res.Err() != nil {
		return nil
	}
	if err := res.Decode(&sol); err != nil {
		return nil
	}
	return &sol
}

func buildOSDoc(q QuestionDoc, sol *QuestionSolutionDoc) map[string]interface{} {
	languages := make([]int32, 0, len(q.Content))
	natures := make([]int32, 0, len(q.Content))
	for _, c := range q.Content {
		languages = append(languages, c.Language)
		natures = append(natures, c.QuestionNature)
	}

	hasSolution := false
	vTagsSet := make(map[string]struct{})
	if sol != nil {
		for _, ts := range sol.TextSolutions {
			if strings.TrimSpace(ts.Text) != "" {
				hasSolution = true
				break
			}
		}
		for _, vs := range sol.VideoSolutions {
			if vs.VTag != "" {
				vTagsSet[vs.VTag] = struct{}{}
			}
			if vs.VTag2 != "" {
				vTagsSet[vs.VTag2] = struct{}{}
			}
		}
	}
	vTags := make([]string, 0, len(vTagsSet))
	for k := range vTagsSet {
		vTags = append(vTags, k)
	}

	return map[string]interface{}{
		"oldQuestionId":    q.OldQuestionID,
		"questionId":       q.QuestionID,
		"version":          q.Version,
		"status":           q.Status,
		"type":             q.Type,
		"qnsLevel":         q.QnsLevel,
		"streamName":       getOldTagValue(q.OldTags, "streamName"),
		"className":        getOldTagValue(q.OldTags, "className"),
		"subjectName":      getOldTagValue(q.OldTags, "subjectName"),
		"topicName":        getOldTagValue(q.OldTags, "topicName"),
		"subTopicName":     getOldTagValue(q.OldTags, "subTopicName"),
		"taxonomyId":       getOldTagValue(q.OldTags, "taxonomyId"),
		"session":          q.Session,
		"source":           q.Source,
		"sourceCenter":     q.SourceCenter,
		"uniqueIdentifier": q.UniqueIdentifier,
		"languages":        languages,
		"questionNatures":  natures,
		"taxonomyData":     q.TaxonomyData,
		"customTags":       q.CustomTags,
		"hashTags":         q.HashTags,
		"createdAt":        q.CreatedAt,
		"updatedAt":        q.UpdatedAt,
		"hasSolution":      hasSolution,
		"vTags":            vTags,
	}
}

// Helper to read values from oldTags list (case-insensitive on name)
func getOldTagValue(tags []Tag, key string) string {
	for _, t := range tags {
		if strings.EqualFold(t.Name, key) {
			return t.Value
		}
	}
	return ""
}

func bulkIndex(ctx context.Context, cfg BackfillConfig, in <-chan map[string]interface{}) {
	if cfg.OSEndpoint != "" && cfg.OSUser != "" {
		bulkIndexHTTP(ctx, cfg, in)
		return
	}
	if cfg.OutFile != "" {
		f, err := os.Create(cfg.OutFile)
		if err != nil {
			log.Fatalf("create out file: %v", err)
		}
		defer f.Close()
		w := bufio.NewWriter(f)
		defer w.Flush()
		for doc := range in {
			meta := map[string]map[string]interface{}{"index": {"_index": cfg.IndexName, "_id": fmt.Sprintf("%d", doc["oldQuestionId"])}}
			b1, _ := json.Marshal(meta)
			b2, _ := json.Marshal(doc)
			w.Write(b1)
			w.WriteByte('\n')
			w.Write(b2)
			w.WriteByte('\n')
		}
		return
	}
	count := 0
	for range in {
		count++
	}
	log.Printf("No OS_ENDPOINT/OS_BULK_OUT provided. Processed %d docs (dry run).", count)
}

func bulkIndexHTTP(ctx context.Context, cfg BackfillConfig, in <-chan map[string]interface{}) {
	client := &http.Client{Timeout: 60 * time.Second}
	buf := &bytes.Buffer{}
	enc := json.NewEncoder(buf)
	sent := 0
	flush := func() {
		if buf.Len() == 0 {
			return
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(cfg.OSEndpoint, "/")+"/_bulk", bytes.NewReader(buf.Bytes()))
		if err != nil {
			log.Printf("bulk req err: %v", err)
			buf.Reset()
			return
		}
		req.Header.Set("Content-Type", "application/x-ndjson")
		if cfg.OSUser != "" {
			req.SetBasicAuth(cfg.OSUser, cfg.OSPass)
		}
		resp, err := client.Do(req)
		if err != nil {
			log.Printf("bulk http err: %v", err)
			buf.Reset()
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 300 {
			log.Printf("bulk status=%d", resp.StatusCode)
		}
		buf.Reset()
	}
	for doc := range in {
		meta := map[string]map[string]interface{}{"index": {"_index": cfg.IndexName, "_id": fmt.Sprintf("%d", doc["oldQuestionId"])}}
		b1, _ := json.Marshal(meta)
		buf.Write(b1)
		buf.WriteByte('\n')
		if err := enc.Encode(doc); err != nil {
			log.Printf("encode doc err: %v", err)
			continue
		}
		sent++
		if sent%cfg.BulkSize == 0 {
			flush()
		}
	}
	flush()
	log.Printf("Bulk sent docs: %d", sent)
}
