package main

type MTestSet struct {
	ID        int64   `json:"id" bson:"setId"`
	PaperID   int64   `json:"paper_id" bson:"oldPaperId"`
	SetPdfUrl *string `json:"set_pdf_url" bson:"set_pdf_url"`
}
