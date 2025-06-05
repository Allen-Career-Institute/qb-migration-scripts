package main

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql" // Import the MySQL driver
	"html/template"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const mysqlDSN = ""

// RadioactiveDecay represents the structure of the JSON data
type RadioactiveDecay struct {
	ExplainTheProblem              string `json:"Explain the problem"`
	Concept                        string `json:"Concept"`
	Formula                        string `json:"Formula"`
	VisualAid                      string `json:"Visual Aid"`
	Calculation                    string `json:"Calculation"`
	Hints                          string `json:"Hints"`
	TipsAndTricks                  string `json:"Tips and Tricks"`
	CommonMistakes                 string `json:"Common Mistakes"`
	ExplanationForIncorrectAnswers string `json:"Explanation for Incorrect Answers"`
	TimeEstimate                   string `json:"Time Estimate"`
	DifficultyLevel                string `json:"Difficulty Level"`
}

func connectMySQL() (*sql.DB, error) {
	db, err := sql.Open("mysql", mysqlDSN)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	return db, nil
}

// // latexToImageURL converts a LaTeX string to an image URL using Codecogs API.
// func latexToImageURL(latex string) string {
// 	// Encode the LaTeX string for URL safety
// 	encodedLatex := url.QueryEscape(latex)
// 	fmt.Println(encodedLatex)
// 	// You can adjust the dpi (dots per inch) for better quality if needed, e.g., 300
// 	return fmt.Sprintf("https://latex.codecogs.com/png.latex?%s", encodedLatex)
// }

type LaTeXAPIResponse map[string]string

// latexToImageURL calls an API to convert LaTeX to an image URL.
func latexToImageURL(latex string) string {
	apiURL := "https://qb.allen.ac.in/equation-image" // **IMPORTANT: Replace with your actual API endpoint**

	// URL-encode the LaTeX code
	encodedLatex := url.QueryEscape(latex)

	// Create the request body
	requestBody := fmt.Sprintf("code=%s", encodedLatex)

	client := &http.Client{
		Timeout: 10 * time.Second, // Set a timeout for the API call
	}

	req, err := http.NewRequest("POST", apiURL, bytes.NewBufferString(requestBody))
	if err != nil {
		log.Printf("Error creating request for LaTeX '%s': %v", latex, err)
		return ""
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
	req.Header.Set("Cookie", "mjx.menu=renderer%3ACommonHTML%26%3Bscale%3A100; _fbp=fb.2.1737566098678.470122313194099259; _ga_NZ2TCWXZLL=GS1.1.1744454402.1.1.1744454490.0.0.0; _gcl_au=1.1.1000249648.1745448225; _ga=GA1.1.1972837165.1737566099; _uetvid=feaa4100178e11f0b3fc9f197001bb07; _ga_N4DFKF8RQD=GS2.3.s1747911013$o19$g0$t1747911013$j60$l0$h0$duSkUSueuITM_PtCTcUHMCBgecZ570D71MQ; _ga_38G8DGL4N3=GS2.1.s1747911013$o21$g1$t1747911115$j60$l0$h0$dhyskQ-hyQxKLrtDBBqYPGBtuQMBvKcVyWA; PHPSESSID=ki8pi99advhstfth2bvk98q9gj; ssnhash=Zis95%2FuF0FAG5tZSV1phBUl%2BrvF2z4zhAjjRq701tCPBydX%2B2j11Lh3gWny9vwbm; GoogleRefreshToken=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJhVVNzVzhHSTAzZHlRMEFJRlZuOTIiLCJkX3R5cGUiOiJ3ZWIiLCJkaWQiOiIyMWZhYzZmMy0zMmZkLTQ2YWYtYWNmNy1mNmZiZWIyNDY0ZmQiLCJlX2lkIjoiMzQ5MjQxNzE0IiwiZXhwIjoxNzUxNjEyNjQ1LCJpYXQiOiIyMDI1LTA2LTA0VDA3OjA0OjA1LjY0MTQ3OTgxMVoiLCJpc3MiOiJhdXRoZW50aWNhdGlvbi5hbGxlbi1wcm9kIiwiaXN1IjoiIiwicHQiOiJJTlRFUk5BTF9VU0VSIiwic2lkIjoiMzljOGVkZTEtYmU0ZC00NDQ0LTkzYmUtYzAzMjRjMWQxNWRlIiwidGlkIjoiYVVTc1c4R0kwM2R5UTBBSUZWbjkyIiwidHlwZSI6InJlZnJlc2giLCJ1aWQiOiJQMnFCMmFuYzVEUWhOTjlmQ002MVIifQ.MhTga_O6qrIighbGES3kUbo8ahxv5lgRsYFd4-nCxVQ; GoogleAccessToken=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJhVVNzVzhHSTAzZHlRMEFJRlZuOTIiLCJkX3R5cGUiOiJ3ZWIiLCJkaWQiOiIyMWZhYzZmMy0zMmZkLTQ2YWYtYWNmNy1mNmZiZWIyNDY0ZmQiLCJlX2lkIjoiMzQ5MjQxNzE0IiwiZXhwIjoxNzQ5MTE0MjQ1LCJpYXQiOiIyMDI1LTA2LTA0VDA3OjA0OjA1LjY0MTQ3OTgxMVoiLCJpc3MiOiJhdXRoZW50aWNhdGlvbi5hbGxlbi1wcm9kIiwiaXN1IjoiIiwicHQiOiJJTlRFUk5BTF9VU0VSIiwic2lkIjoiMzljOGVkZTEtYmU0ZC00NDQ0LTkzYmUtYzAzMjRjMWQxNWRlIiwidGlkIjoiYVVTc1c4R0kwM2R5UTBBSUZWbjkyIiwidHlwZSI6ImFjY2VzcyIsInVpZCI6IlAycUIyYW5jNURRaE5OOWZDTTYxUiJ9.O3z-N23PGw-bE2yei7u-6NTW-Gm79v6rzwbu8ply4os; qbUser=PHPSESSID%3Aki8pi99advhstfth2bvk98q9gj; mjx.menu=renderer%3ACommonHTML%26%3Bscale%3A100")

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Error making API call for LaTeX '%s': %v", latex, err)
		return ""
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		log.Printf("API call failed for LaTeX '%s' with status %d: %s", latex, resp.StatusCode, string(bodyBytes))
		return ""
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Error reading API response body for LaTeX '%s': %v", latex, err)
		return ""
	}

	var apiResponse LaTeXAPIResponse
	err = json.Unmarshal(body, &apiResponse)
	if err != nil {
		return ""
	}

	// Extract the image URL. Assuming "0" key always holds the URL.
	imageUrl, ok := apiResponse["0"]
	if !ok {
		return ""
	}

	newBaseURL := "https://d2lbh14zkcqlst.cloudfront.net/"
	oldBaseURL := "https://s3-ap-south-1.amazonaws.com/question-bank-allen/"

	if strings.HasPrefix(imageUrl, oldBaseURL) {
		return strings.Replace(imageUrl, oldBaseURL, newBaseURL, 1)
	} else {
		return imageUrl
	}

	log.Printf("API response for LaTeX '%s' contained no URLs.", latex)
	return ""
}

func getBotSolution(jsonData string) string {
	var decayData RadioactiveDecay
	err := json.Unmarshal([]byte(jsonData), &decayData)
	if err != nil {
		log.Fatalf("Error unmarshaling JSON: %v", err)
	}

	var sb strings.Builder

	// Add the "Generated by Allie" line at the beginning
	sb.WriteString("<p><b>Generated by Allie</b><br><br>")

	// Map for field-to-heading mappings
	fieldHeadings := map[string]string{
		"Explain the problem":               "Problem Statement",
		"Concept":                           "Underlying Concept",
		"Formula":                           "Relevant Formulas",
		"Calculation":                       "Step-by-Step Calculation",
		"Tips and Tricks":                   "Tips and Tricks",
		"Common Mistakes":                   "Common Mistakes",
		"Explanation for Incorrect Answers": "Why Other Options Are Incorrect?",
	}

	// Regular expressions to find LaTeX math
	// Inline math: \( ... \)
	inlineMathRegex := regexp.MustCompile(`\\\((.*?)\\\)`)
	// Display math: \[ ... \]
	displayMathRegex := regexp.MustCompile(`\\\[(.*?)\\\]`)

	// Function to process content for LaTeX and append to string builder
	processAndAppendField := func(heading, content string) {
		if content == "N/A" || content == "" {
			return // Skip if content is N/A or empty
		}
		// content = strings.ReplaceAll(content, "\n\n", "<br>")

		// Replace inline LaTeX
		content = inlineMathRegex.ReplaceAllStringFunc(content, func(match string) string {
			latexCode := strings.TrimPrefix(strings.TrimSuffix(match, "\\)"), "\\(")
			imageURL := latexToImageURL(latexCode)
			return fmt.Sprintf(`<img src="%s" alt="%s" style="vertical-align: middle;">`, imageURL, template.HTMLEscapeString(latexCode))
		})

		// Replace display LaTeX
		content = displayMathRegex.ReplaceAllStringFunc(content, func(match string) string {
			latexCode := strings.TrimPrefix(strings.TrimSuffix(match, "\\]"), "\\[")
			imageURL := latexToImageURL(latexCode)
			// Add a div for block-level equations to ensure they appear on their own line
			return fmt.Sprintf(`<img src="%s" alt="%s" style="display: block; margin: 10px auto;">`, imageURL, template.HTMLEscapeString(latexCode))
		})
		content = strings.ReplaceAll(content, "\n\n", "<br>")

		sb.WriteString(fmt.Sprintf("<b>%s:</b> %s<br><br>", heading, content))
	}

	processAndAppendField(fieldHeadings["Explain the problem"], decayData.ExplainTheProblem)
	processAndAppendField(fieldHeadings["Concept"], decayData.Concept)
	processAndAppendField(fieldHeadings["Formula"], decayData.Formula)
	processAndAppendField(fieldHeadings["Calculation"], decayData.Calculation)
	processAndAppendField(fieldHeadings["Tips and Tricks"], decayData.TipsAndTricks)
	processAndAppendField(fieldHeadings["Common Mistakes"], decayData.CommonMistakes)
	processAndAppendField(fieldHeadings["Explanation for Incorrect Answers"], decayData.ExplanationForIncorrectAnswers)

	sb.WriteString("</p>")
	// htmlOutput := template.HTML(sb.String())
	return sb.String()
}

func updateMySQL(db *sql.DB, oldQuestionID int64, solution string) {

	stmt, err := db.Prepare("UPDATE question_content SET solution = ? WHERE qns_id = ? AND language = 1")
	if err != nil {
		log.Fatalf("Error preparing update statement: %d %v", oldQuestionID, err)
		return
	}
	defer stmt.Close()

	result, err := stmt.Exec(solution, oldQuestionID)
	if err != nil {
		log.Fatalf("Error executing update statement: %d %v", oldQuestionID, err)
		return
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		log.Fatalf("Error getting rows affected: %d %v", oldQuestionID, err)
		return
	}

	fmt.Printf("Update successful! Rows affected: %d %d\n", oldQuestionID, rowsAffected)
}

func main() {

	file, err := os.Open("solutions.csv")
	if err != nil {
		log.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	// Create a new CSV reader
	reader := csv.NewReader(file)

	// Read all records
	records, err := reader.ReadAll()
	if err != nil {
		log.Fatalf("Failed to read CSV: %v", err)
	}

	db, err := connectMySQL()
	if err != nil {
		log.Fatal("MySQL Connection Error:", err)
		fmt.Println("MySQL Connection Error:", err)
		return
	}
	defer db.Close()

	for i, record := range records {
		if i == 0 {
			continue
		}

		oldQuestionID, err1 := strconv.ParseInt(record[0], 10, 64)
		if err1 != nil {
			fmt.Printf("Error occurred for  %+v \n", record[0])
		}
		fmt.Printf("Data Insertion started for: %+v \n", record[0])

		htmlOutput := getBotSolution(record[2])
		fmt.Println(htmlOutput)

		updateMySQL(db, oldQuestionID, htmlOutput)
		fmt.Printf("Data Insertion completed for  %+v \n", oldQuestionID)
	}

	fmt.Println("Task Completed")
}
