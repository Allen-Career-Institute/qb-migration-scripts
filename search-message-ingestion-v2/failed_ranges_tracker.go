package main

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
)

// FailedRangeTracker helps track and retry failed ranges
type FailedRangeTracker struct {
	FailedRanges []string
}

// AddFailedRange adds a range to the failed list
func (f *FailedRangeTracker) AddFailedRange(start, end int64) {
	f.FailedRanges = append(f.FailedRanges, fmt.Sprintf("%d-%d", start, end))
}

// WriteFailedRangesToFile writes failed ranges to a file for later retry
func (f *FailedRangeTracker) WriteFailedRangesToFile(filename string) error {
	if len(f.FailedRanges) == 0 {
		return nil
	}

	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	for _, r := range f.FailedRanges {
		_, err := file.WriteString(r + "\n")
		if err != nil {
			return err
		}
	}

	fmt.Printf("Written %d failed ranges to %s\n", len(f.FailedRanges), filename)
	return nil
}

// ReadFailedRangesFromFile reads failed ranges from a file
func ReadFailedRangesFromFile(filename string) ([]string, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	lines := strings.Split(string(data), "\n")
	var ranges []string
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" {
			ranges = append(ranges, line)
		}
	}

	return ranges, nil
}

// ParseRange parses a range string like "1000-1099" into start and end integers
func ParseRange(rangeStr string) (int64, int64, error) {
	parts := strings.Split(rangeStr, "-")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid range format: %s", rangeStr)
	}

	start, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid start value: %s", parts[0])
	}

	end, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid end value: %s", parts[1])
	}

	return start, end, nil
}

// ConsolidateRanges merges overlapping or adjacent ranges
func ConsolidateRanges(ranges []string) []string {
	if len(ranges) == 0 {
		return ranges
	}

	// Parse and sort ranges
	type rangeStruct struct {
		start, end int64
	}
	var parsedRanges []rangeStruct

	for _, r := range ranges {
		start, end, err := ParseRange(r)
		if err != nil {
			continue
		}
		parsedRanges = append(parsedRanges, rangeStruct{start, end})
	}

	sort.Slice(parsedRanges, func(i, j int) bool {
		return parsedRanges[i].start < parsedRanges[j].start
	})

	// Merge overlapping ranges
	var consolidated []rangeStruct
	for _, r := range parsedRanges {
		if len(consolidated) == 0 || consolidated[len(consolidated)-1].end < r.start-1 {
			consolidated = append(consolidated, r)
		} else {
			if r.end > consolidated[len(consolidated)-1].end {
				consolidated[len(consolidated)-1].end = r.end
			}
		}
	}

	// Convert back to strings
	var result []string
	for _, r := range consolidated {
		result = append(result, fmt.Sprintf("%d-%d", r.start, r.end))
	}

	return result
}
