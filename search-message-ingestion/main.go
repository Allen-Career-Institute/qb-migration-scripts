package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type SqsMessage struct {
	ClientId           string `json:"clientId"`
	EntityType         string `json:"entityType"`
	UniqueIdentifier   string `json:"uniqueIdentifier"`
	OperationType      string `json:"operationType"`
	OperationTimestamp int64  `json:"operationTimestamp"`
	MessageSentAt      int64  `json:"messageSentAt"`
}

const (
	constantClientID   = "qb_client"
	constantEntityType = "questions"
	constantRegion     = "ap-south-1"
	constantQueueName  = "ap-south-1-staging-qb-search-queue"

	mongoURI      = ""
	mongoDB       = "qb"
	questionsColl = "questions"
)

type minimalContent struct {
	Language int32 `bson:"language,omitempty"`
}

type minimalQuestion struct {
	QuestionID string           `bson:"questionId,omitempty"`
	Content    []minimalContent `bson:"content,omitempty"`
}

func main() {
	var (
		startID   int64
		endID     int64
		batchSize int64
		workers   int
		opType    string
	)

	flag.Int64Var(&startID, "start", 1, "Start oldQuestionId")
	flag.Int64Var(&endID, "end", 5900000, "End oldQuestionId (inclusive)")
	flag.Int64Var(&batchSize, "batch-size", 200, "Batch size for processing")
	flag.IntVar(&workers, "workers", 50, "Number of concurrent workers per batch")
	flag.StringVar(&opType, "operation-type", "UPSERT", "Operation type")
	flag.Parse()

	fmt.Printf("Start OldQuestionID: %d\n", startID)
	fmt.Printf("End OldQuestionID: %d\n", endID)
	fmt.Printf("Batch Size: %d\n", batchSize)
	fmt.Printf("Workers per batch: %d\n", workers)

	ctx := context.Background()

	// AWS SQS client
	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(constantRegion))
	if err != nil {
		log.Fatalf("failed to load AWS config: %v", err)
	}
	sqsClient := sqs.NewFromConfig(awsCfg)
	outURL, err := sqsClient.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(constantQueueName)})
	if err != nil {
		log.Fatalf("failed to resolve queue URL for %s: %v", constantQueueName, err)
	}
	queueURL := aws.ToString(outURL.QueueUrl)

	// Mongo client
	mongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Fatalf("mongo connect error: %v", err)
	}
	defer mongoClient.Disconnect(ctx)
	qCol := mongoClient.Database(mongoDB).Collection(questionsColl)

	// Process in batches with 50 worker goroutines
	for i := startID; i <= endID; i = i + batchSize {
		oldQuestionIDStart := i
		oldQuestionIDEnd := i + batchSize - 1
		if oldQuestionIDEnd > endID {
			oldQuestionIDEnd = endID
		}

		// Create channel for this batch
		ids := make(chan int64, workers*4) // Buffer for workers
		var wg sync.WaitGroup

		// Start worker goroutines for this batch
		wg.Add(workers)
		for w := 0; w < workers; w++ {
			go func() {
				defer wg.Done()
				for oid := range ids {
					processOne(ctx, qCol, sqsClient, queueURL, oid, opType)
				}
			}()
		}

		// Send IDs to workers
		for j := oldQuestionIDStart; j <= oldQuestionIDEnd; j++ {
			ids <- j
		}
		close(ids)

		wg.Wait()
		fmt.Printf("SQS message ingestion completed for %d %d\n", oldQuestionIDStart, oldQuestionIDEnd)
	}

	fmt.Println("Completed sending SQS messages for range")
}

func processOne(ctx context.Context, qCol *mongo.Collection, sqsClient *sqs.Client, queueURL string, oldQuestionID int64, opType string) {
	// Find latest version with projection of questionId and content.language
	filter := bson.M{"oldQuestionId": oldQuestionID}
	findOpts := options.FindOne().SetSort(bson.D{{Key: "version", Value: -1}})
	findOpts.SetProjection(bson.M{
		"questionId":       1,
		"content.language": 1,
	})
	var m minimalQuestion
	res := qCol.FindOne(ctx, filter, findOpts)
	if res.Err() != nil {
		return // skip not found or errors silently
	}

	if err := res.Decode(&m); err != nil {
		return
	}
	if m.QuestionID == "" || len(m.Content) == 0 {
		return
	}

	now := time.Now().Unix()
	for _, c := range m.Content {
		uid := m.QuestionID + "_" + strconv.Itoa(int(c.Language))
		payload := &SqsMessage{
			ClientId:           constantClientID,
			EntityType:         constantEntityType,
			UniqueIdentifier:   uid,
			OperationType:      opType,
			OperationTimestamp: now,
			MessageSentAt:      now,
		}
		body, _ := json.Marshal(payload)
		_, err := sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
			QueueUrl:    aws.String(queueURL),
			MessageBody: aws.String(string(body)),
		})
		if err != nil {
			log.Printf("send msg failed oldQID=%d qid=%s lang=%d: %v", oldQuestionID, m.QuestionID, c.Language, err)
		}
	}
}
