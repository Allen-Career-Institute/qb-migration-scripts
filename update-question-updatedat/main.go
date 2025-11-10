package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	mongoURI      = ""
	mongoDB       = "qb"
	questionsColl = "questions"
	outputColl    = "new_question_v2"
)

type QuestionDoc struct {
	UpdatedAt int64 `bson:"updatedAt,omitempty"`
}

func main() {
	var startID int64
	var endID int64
	var workers int
	var batchSize int64

	flag.Int64Var(&startID, "start", 1, "Start oldQuestionId")
	flag.Int64Var(&endID, "end", 500000, "End oldQuestionId (inclusive)")
	flag.Int64Var(&batchSize, "batch-size", 1000, "Batch size of oldQuestionIds")
	flag.IntVar(&workers, "workers", 20, "Concurrent workers")
	flag.Parse()

	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Fatalf("mongo connect error: %v", err)
	}
	defer client.Disconnect(ctx)

	qCol := client.Database(mongoDB).Collection(questionsColl)
	outCol := client.Database(mongoDB).Collection(outputColl)

	// Ensure TTL index exists on new collection: expire documents 24 hours after their updatedAtTTL
	if err := ensureTTLIndex(ctx, outCol, 24*60*60); err != nil {
		log.Fatalf("failed to ensure TTL index: %v", err)
	}

	// Ensure unique index on oldQuestionId to keep one document per oldQuestionId
	if err := ensureUniqueOldQuestionID(ctx, outCol); err != nil {
		log.Fatalf("failed to ensure unique index on oldQuestionId: %v", err)
	}

	for i := startID; i <= endID; i += batchSize {
		start := i
		end := i + batchSize - 1
		if end > endID {
			end = endID
		}

		ids := make(chan int64, workers*4)
		var wg sync.WaitGroup

		wg.Add(workers)
		for w := 0; w < workers; w++ {
			// Each worker has its own rng to avoid contention
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(w)))
			go func(localRng *rand.Rand) {
				defer wg.Done()
				for oid := range ids {
					if err := processOne(ctx, qCol, outCol, oid, localRng); err != nil {
						log.Printf("oldQuestionId=%d error: %v", oid, err)
					}
				}
			}(rng)
		}

		for oid := start; oid <= end; oid++ {
			ids <- oid
		}
		close(ids)
		wg.Wait()

		fmt.Printf("Processed range %d-%d\n", start, end)
	}

	fmt.Println("Done")
}

func processOne(ctx context.Context, qCol *mongo.Collection, outCol *mongo.Collection, oldQuestionID int64, r *rand.Rand) error {
	filter := bson.M{"oldQuestionId": oldQuestionID}
	findOpts := options.FindOne().SetSort(bson.D{{Key: "version", Value: -1}})

	var fullDoc bson.M
	res := qCol.FindOne(ctx, filter, findOpts)
	if res.Err() != nil {
		if res.Err() == mongo.ErrNoDocuments {
			return nil
		}
		return res.Err()
	}
	if err := res.Decode(&fullDoc); err != nil {
		return err
	}

	// Random updatedAt within last 24 hours (milliseconds)
	nowMs := time.Now().UnixMilli()
	dayMs := int64(24 * time.Hour / time.Millisecond)
	randDelta := r.Int63n(dayMs + 1)
	newUpdatedAt := nowMs - randDelta

	// Update in source collection for this version document
	// Use _id equality to ensure only the latest-version doc we fetched is updated
	idVal, ok := fullDoc["_id"]
	if !ok {
		return fmt.Errorf("document missing _id for oldQuestionId=%d", oldQuestionID)
	}

	update := bson.M{"$set": bson.M{"updatedAt": newUpdatedAt}}
	if _, err := qCol.UpdateByID(ctx, idVal, update); err != nil {
		return err
	}

	// Prepare the out document
	delete(fullDoc, "_id")
	fullDoc["oldQuestionId"] = oldQuestionID
	fullDoc["updatedAt"] = newUpdatedAt // keep int64 ms
	// Mirror updatedAt into a BSON Date so TTL can be applied to it
	fullDoc["updatedAtTTL"] = time.UnixMilli(newUpdatedAt)

	// Overwrite by oldQuestionId using upsert
	upsert := true
	_, err := outCol.ReplaceOne(ctx, bson.M{"oldQuestionId": oldQuestionID}, fullDoc, &options.ReplaceOptions{Upsert: &upsert})
	if err != nil {
		return err
	}

	return nil
}

func ensureTTLIndex(ctx context.Context, col *mongo.Collection, expireAfterSeconds int32) error {
	idxView := col.Indexes()
	cur, err := idxView.List(ctx)
	if err != nil {
		return err
	}
	defer cur.Close(ctx)

	found := false
	needsUpdate := false
	for cur.Next(ctx) {
		var idx bson.M
		if err := cur.Decode(&idx); err != nil {
			return err
		}
		name, _ := idx["name"].(string)
		if name == "updatedAtTTL_1" {
			found = true
			var existing int32
			switch v := idx["expireAfterSeconds"].(type) {
			case int32:
				existing = v
			case int64:
				existing = int32(v)
			case float64:
				existing = int32(v)
			}
			if existing != expireAfterSeconds {
				needsUpdate = true
			}
			break
		}
	}
	if err := cur.Err(); err != nil {
		return err
	}

	if !found {
		opts := options.Index().SetExpireAfterSeconds(expireAfterSeconds)
		model := mongo.IndexModel{
			Keys:    bson.D{{Key: "updatedAtTTL", Value: 1}},
			Options: opts,
		}
		_, err := idxView.CreateOne(ctx, model)
		return err
	}

	if needsUpdate {
		cmd := bson.D{
			{Key: "collMod", Value: col.Name()},
			{Key: "index", Value: bson.D{
				{Key: "name", Value: "updatedAtTTL_1"},
				{Key: "expireAfterSeconds", Value: expireAfterSeconds},
			}},
		}
		if err := col.Database().RunCommand(ctx, cmd).Err(); err != nil {
			// Fallback: drop and recreate
			_, _ = idxView.DropOne(ctx, "updatedAtTTL_1")
			opts := options.Index().SetExpireAfterSeconds(expireAfterSeconds)
			model := mongo.IndexModel{
				Keys:    bson.D{{Key: "updatedAtTTL", Value: 1}},
				Options: opts,
			}
			if _, cerr := idxView.CreateOne(ctx, model); cerr != nil {
				return fmt.Errorf("failed to update TTL index via collMod: %v; recreate failed: %v", err, cerr)
			}
		}
	}

	return nil
}

// ensureUniqueOldQuestionID enforces a unique index on oldQuestionId in the new collection
func ensureUniqueOldQuestionID(ctx context.Context, col *mongo.Collection) error {
	idxView := col.Indexes()
	cur, err := idxView.List(ctx)
	if err != nil {
		return err
	}
	defer cur.Close(ctx)

	for cur.Next(ctx) {
		var idx bson.M
		if err := cur.Decode(&idx); err != nil {
			return err
		}
		if name, _ := idx["name"].(string); name == "oldQuestionId_1" {
			// already exists
			return nil
		}
	}
	if err := cur.Err(); err != nil {
		return err
	}

	unique := true
	model := mongo.IndexModel{
		Keys:    bson.D{{Key: "oldQuestionId", Value: 1}},
		Options: options.Index().SetUnique(unique),
	}
	_, err = idxView.CreateOne(ctx, model)
	return err
}
