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

	OS_ENDPOINT   = ""
	OS_USERNAME   = "admin"
	OS_PASSWORD   = "Y5aT8gpe6P051vjeL67F"
	OS_INDEX_NAME = "qb_index"

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
	HasVideoSolution bool   `bson:"hasVideoSolution,omitempty"`
	Content          []struct {
		Language        int32 `bson:"language,omitempty"`
		QuestionNature  int32 `bson:"questionNature,omitempty"`
		HasTextSolution bool  `bson:"hasTextSolution,omitempty"`
		QuestionStem    struct {
			Text string `bson:"text,omitempty"`
		} `bson:"questionStem,omitempty"`
		Options []struct {
			Text string `bson:"text,omitempty"`
		} `bson:"options,omitempty"`
		Answer string `bson:"answer,omitempty"`
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
	OldQuestionID           int64                    `bson:"oldQuestionId,omitempty"`
	TextSolutions           []*TextSolutionDocument  `bson:"textSolutions,omitempty"`
	VideoSolutions          []*VideoSolutionDocument `bson:"videoSolutions,omitempty"`
	StructuredTextSolutions []map[string]interface{} `bson:"structuredTextSolutions,omitempty"`
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

	// Build one OpenSearch document per language
	docs := buildOSDocs(questionDoc, solution)
	for _, d := range docs {
		out <- d
	}
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

func buildOSDocs(q QuestionDoc, sol *QuestionSolutionDoc) []map[string]interface{} {
	// Compute solution-related flags and vTags
	hasTextSolution := false
	hasBotSolution := false
	vTagsSet := make(map[string]struct{})
	if sol != nil {
		for _, ts := range sol.TextSolutions {
			if strings.TrimSpace(ts.Text) != "" {
				hasTextSolution = true
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
		if len(sol.StructuredTextSolutions) > 0 {
			hasBotSolution = true
		}
	}
	hasVTag := len(vTagsSet) > 0

	// Simple metadata extraction from old tags
	streamName := getOldTagValue(q.OldTags, "streamName")
	className := getOldTagValue(q.OldTags, "className")
	subjectName := getOldTagValue(q.OldTags, "subjectName")
	topicName := getOldTagValue(q.OldTags, "topicName")
	subTopicName := getOldTagValue(q.OldTags, "subTopicName")

	streams := make([]string, 0)
	if streamName != "" {
		streams = append(streams, streamName)
	}
	subjectNames := make([]string, 0)
	if subjectName != "" {
		subjectNames = append(subjectNames, subjectName)
	}
	topicNames := make([]string, 0)
	if topicName != "" {
		topicNames = append(topicNames, topicName)
	}
	subTopicNames := make([]string, 0)
	if subTopicName != "" {
		subTopicNames = append(subTopicNames, subTopicName)
	}

	docs := make([]map[string]interface{}, 0, len(q.Content))
	for _, c := range q.Content {
		options := make([]string, 0, len(c.Options))
		for _, op := range c.Options {
			text := strings.TrimSpace(op.Text)
			if text != "" {
				options = append(options, text)
			}
		}

		// Extract taxonomy IDs
		taxIDs := extractTaxonomyIDs(q.TaxonomyData)

		langStr := fmt.Sprintf("%d", c.Language)
		doc := map[string]interface{}{
			"_id":                fmt.Sprintf("%s_%s", q.QuestionID, langStr),
			"old_question_id":    q.OldQuestionID,
			"status":             q.Status,
			"stream":             streams,
			"subject":            subjectNames,
			"question":           c.QuestionStem.Text,
			"options":            options,
			"class":              className,
			"language":           c.Language,
			"has_text_solution":  c.HasTextSolution || hasTextSolution,
			"has_video_solution": q.HasVideoSolution,
			"has_vtag":           hasVTag,
			"has_bot_solution":   hasBotSolution,
			"source_root":        "allen",
			"question_id":        q.QuestionID,
			"current_version_id": q.Version,
			"answer":             c.Answer,
			"topic":              topicNames,
			"sub_topic":          subTopicNames,
			"difficulty_level":   q.QnsLevel,
			"question_type":      q.Type,
			"session":            q.Session,
			"source":             q.Source,
			"source_center":      q.SourceCenter,
			"unique_identifier":  q.UniqueIdentifier,
			"created_at":         q.CreatedAt,
			"updated_at":         q.UpdatedAt,
		}

		// customTags, hashTags formatting
		if ctags := formatCustomTags(q.CustomTags); len(ctags) > 0 {
			doc["custom_tags"] = ctags
		}
		if htags := formatHashTags(q.HashTags); len(htags) > 0 {
			doc["hashTags"] = htags
		}

		// taxonomy fields
		addTaxonomyFieldsToDocument(doc, taxIDs)

		docs = append(docs, doc)
	}
	return docs
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

// TaxonomyIDs holds de-duplicated taxonomy identifier combinations
type TaxonomyIDs struct {
	ClassTax    []string
	SubjectTax  []string
	TopicTax    []string
	SubtopicTax []string
	ConceptTax  []string
}

// extractTaxonomyIDs traverses TaxonomyData and creates formatted taxonomy ID combinations
func extractTaxonomyIDs(taxonomyData []TaxonomyData) *TaxonomyIDs {
	result := &TaxonomyIDs{
		ClassTax:    make([]string, 0),
		SubjectTax:  make([]string, 0),
		TopicTax:    make([]string, 0),
		SubtopicTax: make([]string, 0),
		ConceptTax:  make([]string, 0),
	}

	taxonomySets := map[string]map[string]struct{}{
		"class":    make(map[string]struct{}),
		"subject":  make(map[string]struct{}),
		"topic":    make(map[string]struct{}),
		"subtopic": make(map[string]struct{}),
		"concept":  make(map[string]struct{}),
	}

	for _, tax := range taxonomyData {
		processTaxonomyEntry(tax, taxonomySets)
	}

	result.ClassTax = convertSetToSlice(taxonomySets["class"])
	result.SubjectTax = convertSetToSlice(taxonomySets["subject"])
	result.TopicTax = convertSetToSlice(taxonomySets["topic"])
	result.SubtopicTax = convertSetToSlice(taxonomySets["subtopic"])
	result.ConceptTax = convertSetToSlice(taxonomySets["concept"])

	return result
}

func processTaxonomyEntry(tax TaxonomyData, sets map[string]map[string]struct{}) {
	if strings.TrimSpace(tax.TaxonomyId) == "" {
		return
	}
	addTaxonomyCombination(sets["class"], tax.TaxonomyId, tax.ClassId)
	addTaxonomyCombination(sets["subject"], tax.TaxonomyId, tax.SubjectId)
	addTaxonomyCombination(sets["topic"], tax.TaxonomyId, tax.TopicId)
	addTaxonomyCombination(sets["subtopic"], tax.TaxonomyId, tax.SubTopicId)
	addTaxonomyCombination(sets["concept"], tax.TaxonomyId, tax.ConceptId)
}

func addTaxonomyCombination(set map[string]struct{}, taxonomyId, fieldId string) {
	if strings.TrimSpace(fieldId) == "" {
		return
	}
	combination := taxonomyId + "_" + fieldId
	set[combination] = struct{}{}
}

// convertSetToSlice converts a set map to a slice of keys
func convertSetToSlice(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	return out
}

// addTaxonomyFieldsToDocument adds taxonomy fields only if they have values
func addTaxonomyFieldsToDocument(documentMap map[string]interface{}, ids *TaxonomyIDs) {
	if ids == nil {
		return
	}
	if len(ids.ClassTax) > 0 {
		documentMap["class_tax"] = ids.ClassTax
	}
	if len(ids.SubjectTax) > 0 {
		documentMap["subject_tax"] = ids.SubjectTax
	}
	if len(ids.TopicTax) > 0 {
		documentMap["topic_tax"] = ids.TopicTax
	}
	if len(ids.SubtopicTax) > 0 {
		documentMap["sub_topic_tax"] = ids.SubtopicTax
	}
	if len(ids.ConceptTax) > 0 {
		documentMap["concept_tax"] = ids.ConceptTax
	}
}

// formatCustomTags converts CustomTag structs to tagName|value strings
func formatCustomTags(customTags []CustomTag) []string {
	if len(customTags) == 0 {
		return nil
	}
	formatted := make([]string, 0, len(customTags))
	for _, tag := range customTags {
		if strings.TrimSpace(tag.TagName) != "" && strings.TrimSpace(tag.Value) != "" {
			formatted = append(formatted, tag.TagName+"|"+tag.Value)
		}
	}
	return formatted
}

// formatHashTags converts HashTags structs to id|description strings
func formatHashTags(hashTags []HashTags) []string {
	if len(hashTags) == 0 {
		return nil
	}
	formatted := make([]string, 0, len(hashTags))
	for _, tag := range hashTags {
		if strings.TrimSpace(tag.HashTagID) != "" && strings.TrimSpace(tag.Description) != "" {
			formatted = append(formatted, tag.HashTagID+"|"+tag.Description)
		}
	}
	return formatted
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
			docID := ""
			if v, ok := doc["_id"]; ok {
				docID = fmt.Sprintf("%v", v)
				delete(doc, "_id")
			} else if v, ok := doc["oldQuestionId"]; ok {
				docID = fmt.Sprintf("%v", v)
			}
			meta := map[string]map[string]interface{}{"index": {"_index": cfg.IndexName, "_id": docID}}
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
		docID := ""
		if v, ok := doc["_id"]; ok {
			docID = fmt.Sprintf("%v", v)
			delete(doc, "_id")
		} else if v, ok := doc["oldQuestionId"]; ok {
			docID = fmt.Sprintf("%v", v)
		}
		meta := map[string]map[string]interface{}{"index": {"_index": cfg.IndexName, "_id": docID}}
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
