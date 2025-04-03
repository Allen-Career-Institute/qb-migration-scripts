package main

type TestSet struct {
	ID            string        `bson:"_id,omitempty"`
	SetID         int64         `bson:"setId"`
	OldPaperID    int64         `bson:"oldPaperId"`
	PaperCode     string        `bson:"paperCode"`
	Label         string        `bson:"label"`
	IsOriginal    bool          `bson:"isOriginal"`
	AddedOn       int64         `bson:"addedOn"`
	LastUpdatedOn int64         `bson:"lastUpdatedOn"`
	SectionInfo   []*SetSection `bson:"setSections"`
	CreatedBy     string        `bson:"createdBy,omitempty"`
	UpdatedBy     string        `bson:"updatedBy,omitempty"`
	IsDeleted     bool          `bson:"isDeleted"`
	SetPdfUrl     string        `bson:"set_pdf_url,omitempty"`
}

type SetSection struct {
	Name            string        `bson:"name"`
	OriginalIndex   int32         `bson:"originalIndex"`
	HaveSubsections bool          `bson:"haveSubsections"`
	Questions       []SetQuestion `bson:"questions"`
	Subsections     []*SetSection `bson:"subsections"`
	IsRandomised    bool          `bson:"isRandomised"`
	OldNamespace    string        `bson:"oldNamespace,omitempty"`
	NewNamespace    string        `bson:"newNamespace,omitempty"`
}

type SetQuestion struct {
	OldQuestionID  int64  `bson:"oldQuestionId"`
	OptionSequence []int  `bson:"optionSequence"`
	Answer         string `bson:"answer"`
	IsRandomised   bool   `bson:"isRandomised"`
	SeqNo          int    `bson:"seqNo"`
	QuestionID     string `bson:"questionId,omitempty"`
}
