package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	apiURL = ""
	// TODO: Add your authorization token here
	authToken = "your-auth-token-here"
)

type BackfillRequest struct {
	OldQuestionIDStart int64 `json:"old_question_id_start"`
	OldQuestionIDEnd   int64 `json:"old_question_id_end"`
	Workers            int   `json:"workers"`
	BatchSize          int   `json:"batch_size"`
}

type BackfillResponse struct {
	BackfillResults map[string]bool `json:"backfillResults"`
	OverallSuccess  bool            `json:"overallSuccess"`
	TotalProcessed  string          `json:"totalProcessed"`
	SuccessfulCount string          `json:"successfulCount"`
	FailedCount     string          `json:"failedCount"`
	ErrorMessage    string          `json:"errorMessage"`
}

func main() {
	var (
		startID       int64
		endID         int64
		rangeSize     int64
		concurrency   int
		apiWorkers    int
		apiBatchSize  int
		authTokenFlag string
	)

	flag.Int64Var(&startID, "start", 1, "Start oldQuestionId")
	flag.Int64Var(&endID, "end", 5900000, "End oldQuestionId (inclusive)")
	flag.Int64Var(&rangeSize, "range-size", 1000, "Size of each range per goroutine")
	flag.IntVar(&concurrency, "concurrency", 10, "Number of concurrent API calls")
	flag.IntVar(&apiWorkers, "api-workers", 50, "Workers parameter for API")
	flag.IntVar(&apiBatchSize, "api-batch-size", 100, "Batch size parameter for API")
	flag.StringVar(&authTokenFlag, "auth-token", "", "Authorization token for API")
	flag.Parse()

	if authTokenFlag == "" {
		log.Fatal("Authorization token is required. Use --auth-token flag")
	}

	// Create log file
	logFile, err := os.Create("logs.txt")
	if err != nil {
		log.Fatal("Error creating log file:", err)
	}
	defer logFile.Close()

	// Set log output to file
	log.SetOutput(logFile)
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// Log to both console and file
	fmt.Printf("Start OldQuestionID: %d\n", startID)
	log.Printf("Start OldQuestionID: %d", startID)
	fmt.Printf("End OldQuestionID: %d\n", endID)
	log.Printf("End OldQuestionID: %d", endID)
	fmt.Printf("Range size per goroutine: %d\n", rangeSize)
	log.Printf("Range size per goroutine: %d", rangeSize)
	fmt.Printf("Concurrent API calls: %d\n", concurrency)
	log.Printf("Concurrent API calls: %d", concurrency)
	fmt.Printf("API Workers: %d\n", apiWorkers)
	log.Printf("API Workers: %d", apiWorkers)
	fmt.Printf("API Batch Size: %d\n", apiBatchSize)
	log.Printf("API Batch Size: %d", apiBatchSize)

	ctx := context.Background()
	client := &http.Client{
		Timeout: 60 * time.Second,
	}

	// Initialize failed range tracker
	failedTracker := &FailedRangeTracker{}

	// Process the entire range in chunks
	for currentStart := startID; currentStart <= endID; {
		var wg sync.WaitGroup
		semaphore := make(chan struct{}, concurrency) // Limit concurrent goroutines

		batchStart := currentStart
		batchEnd := currentStart + int64(concurrency)*rangeSize - 1
		if batchEnd > endID {
			batchEnd = endID
		}

		fmt.Printf("\nProcessing batch: %d to %d\n", batchStart, batchEnd)
		log.Printf("Processing batch: %d to %d", batchStart, batchEnd)

		// Launch goroutines for this batch
		for i := 0; i < concurrency && currentStart <= endID; i++ {
			rangeStart := currentStart
			rangeEnd := currentStart + rangeSize - 1
			if rangeEnd > endID {
				rangeEnd = endID
			}

			wg.Add(1)
			go func(start, end int64) {
				defer wg.Done()
				semaphore <- struct{}{}        // Acquire semaphore
				defer func() { <-semaphore }() // Release semaphore

				failedIDs := callBackfillAPI(ctx, client, start, end, apiWorkers, apiBatchSize, authTokenFlag)
				if len(failedIDs) > 0 {
					fmt.Printf("GoRoutine [%d-%d]: Found %d failed IDs: %v\n", start, end, len(failedIDs), failedIDs)
					log.Printf("GoRoutine [%d-%d]: Found %d failed IDs: %v", start, end, len(failedIDs), failedIDs)
					// Track the entire range as failed for retry
					failedTracker.AddFailedRange(start, end)
				} else {
					fmt.Printf("GoRoutine [%d-%d]: All successful\n", start, end)
					log.Printf("GoRoutine [%d-%d]: All successful", start, end)
				}
			}(rangeStart, rangeEnd)

			currentStart = rangeEnd + 1
		}

		wg.Wait()
		fmt.Printf("Batch completed: %d to %d. Sleeping for 1 second...\n", batchStart, batchEnd)
		log.Printf("Batch completed: %d to %d. Sleeping for 1 second...", batchStart, batchEnd)
		time.Sleep(1 * time.Second)
	}

	fmt.Println("All batches completed!")
	log.Println("All batches completed!")

	// Write failed ranges to file for retry
	if err := failedTracker.WriteFailedRangesToFile("failed_ranges.txt"); err != nil {
		log.Printf("Error writing failed ranges to file: %v", err)
		fmt.Printf("Error writing failed ranges to file: %v\n", err)
	} else if len(failedTracker.FailedRanges) > 0 {
		fmt.Printf("Total failed ranges: %d (saved to failed_ranges.txt)\n", len(failedTracker.FailedRanges))
		log.Printf("Total failed ranges: %d (saved to failed_ranges.txt)", len(failedTracker.FailedRanges))
	}
}

func callBackfillAPI(ctx context.Context, client *http.Client, start, end int64, workers, batchSize int, authToken string) []string {
	req := BackfillRequest{
		OldQuestionIDStart: start,
		OldQuestionIDEnd:   end,
		Workers:            workers,
		BatchSize:          batchSize,
	}

	jsonData, err := json.Marshal(req)
	if err != nil {
		log.Printf("Error marshaling request for range %d-%d: %v", start, end, err)
		// Return all IDs in range as failed when request marshaling fails
		var failedIDs []string
		for id := start; id <= end; id++ {
			failedIDs = append(failedIDs, fmt.Sprintf("%d", id))
		}
		return failedIDs
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", apiURL, bytes.NewBuffer(jsonData))
	if err != nil {
		log.Printf("Error creating request for range %d-%d: %v", start, end, err)
		// Return all IDs in range as failed when request creation fails
		var failedIDs []string
		for id := start; id <= end; id++ {
			failedIDs = append(failedIDs, fmt.Sprintf("%d", id))
		}
		return failedIDs
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+authToken)

	resp, err := client.Do(httpReq)
	if err != nil {
		log.Printf("Error calling API for range %d-%d: %v", start, end, err)
		// Return all IDs in range as failed when API call fails
		var failedIDs []string
		for id := start; id <= end; id++ {
			failedIDs = append(failedIDs, fmt.Sprintf("%d", id))
		}
		return failedIDs
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Error reading response for range %d-%d: %v", start, end, err)
		// Return all IDs in range as failed when response reading fails
		var failedIDs []string
		for id := start; id <= end; id++ {
			failedIDs = append(failedIDs, fmt.Sprintf("%d", id))
		}
		return failedIDs
	}

	if resp.StatusCode != http.StatusOK {
		log.Printf("API returned status %d for range %d-%d: %s", resp.StatusCode, start, end, string(body))
		// Return all IDs in range as failed when API call fails
		var failedIDs []string
		for id := start; id <= end; id++ {
			failedIDs = append(failedIDs, fmt.Sprintf("%d", id))
		}
		return failedIDs
	}

	var response BackfillResponse
	if err := json.Unmarshal(body, &response); err != nil {
		log.Printf("Error unmarshaling response for range %d-%d: %v", start, end, err)
		// Return all IDs in range as failed when JSON parsing fails
		var failedIDs []string
		for id := start; id <= end; id++ {
			failedIDs = append(failedIDs, fmt.Sprintf("%d", id))
		}
		return failedIDs
	}

	// Extract failed IDs (where value is false)
	var failedIDs []string
	for id, success := range response.BackfillResults {
		if !success {
			failedIDs = append(failedIDs, id)
		}
	}

	// Log summary
	totalProcessed, _ := strconv.Atoi(response.TotalProcessed)
	successfulCount, _ := strconv.Atoi(response.SuccessfulCount)
	failedCount, _ := strconv.Atoi(response.FailedCount)

	fmt.Printf("Range [%d-%d]: Processed=%d, Success=%d, Failed=%d, Overall=%v\n",
		start, end, totalProcessed, successfulCount, failedCount, response.OverallSuccess)
	log.Printf("Range [%d-%d]: Processed=%d, Success=%d, Failed=%d, Overall=%v",
		start, end, totalProcessed, successfulCount, failedCount, response.OverallSuccess)

	if response.ErrorMessage != "" {
		log.Printf("API error for range %d-%d: %s", start, end, response.ErrorMessage)
	}

	return failedIDs
}

//go run . --start=1 --end=5000 --range-size=100 --concurrency=50 --api-workers=100 --api-batch-size=100 --auth-token "YOUR_BEARER_TOKEN"

//good conf -
// go run . --start=1 --end=100000 --range-size=100 --concurrency=20 --api-workers=20 --api-batch-size=5 --auth-token "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJhVVNzVzhHSTAzZHlRMEFJRlZuOTIiLCJkX3R5cGUiOiJ3ZWIiLCJkaWQiOiIzYjcwODQyZS02OGFlLTRlYTUtODhkZC1hZjBiZjQzMjU5NzQiLCJlX2lkIjoiMzcxMjUzODE1IiwiZXhwIjoxNzU4NTU2OTk0LCJpYXQiOiIyMDI1LTA5LTIyVDE1OjAzOjE0LjkwOTY2NzkzWiIsImlzcyI6ImF1dGhlbnRpY2F0aW9uLmFsbGVuLXN0YWdlIiwiaXN1IjoiZmFsc2UiLCJwdCI6IlNUVURFTlQiLCJzaWQiOiI3Y2ExYzc3My0wMGZjLTQ4ODAtYjZiNi04YzliOTNjNTNjMGEiLCJ0aWQiOiJhVVNzVzhHSTAzZHlRMEFJRlZuOTIiLCJ0eXBlIjoiYWNjZXNzIiwidWlkIjoiUmpsMDQ1VGZMWExNNVlhNU9XMDFBIn0.O64GjEJjwaIe2M0DjMkoOxvpvhzjEuXBuLvxnGc5qd8"


//go run . --start=1 --end=100000 --range-size=100 --concurrency=5 --api-workers=20 --api-batch-size=5 --auth-token "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJhVVNzVzhHSTAzZHlRMEFJRlZuOTIiLCJkX3R5cGUiOiJ3ZWIiLCJkaWQiOiIzYjcwODQyZS02OGFlLTRlYTUtODhkZC1hZjBiZjQzMjU5NzQiLCJlX2lkIjoiMzcxMjUzODE1IiwiZXhwIjoxNzU4NTU2OTk0LCJpYXQiOiIyMDI1LTA5LTIyVDE1OjAzOjE0LjkwOTY2NzkzWiIsImlzcyI6ImF1dGhlbnRpY2F0aW9uLmFsbGVuLXN0YWdlIiwiaXN1IjoiZmFsc2UiLCJwdCI6IlNUVURFTlQiLCJzaWQiOiI3Y2ExYzc3My0wMGZjLTQ4ODAtYjZiNi04YzliOTNjNTNjMGEiLCJ0aWQiOiJhVVNzVzhHSTAzZHlRMEFJRlZuOTIiLCJ0eXBlIjoiYWNjZXNzIiwidWlkIjoiUmpsMDQ1VGZMWExNNVlhNU9XMDFBIn0.O64GjEJjwaIe2M0DjMkoOxvpvhzjEuXBuLvxnGc5qd8"
