package main

type MHomework struct {
	ID         int64   `json:"id" bson:"id"`
	PaperID    int64   `json:"paper_id" bson:"oldPaperId"`
	QuestionID int64   `json:"question_id" bson:"questionId"`
	CenterName *string `json:"center_name" bson:"centerName"`
}
