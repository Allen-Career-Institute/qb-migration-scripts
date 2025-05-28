package main

type HomeworkDocument struct {
	Id                string         `bson:"_id,omitempty"`
	Session           int64          `bson:"session"`
	OldQuestionId     int64          `bson:"oldQuestionId"`
	OldPaperId        int64          `bson:"oldPaperId"`
	SequenceNumber    int64          `bson:"sequenceNumber"`
	MaterialNumber    string         `bson:"materialNumber,omitempty"`
	SubsectionName    string         `bson:"subsectionName,omitempty"`
	CenterId          string         `bson:"centerId"`
	MaterialType      string         `bson:"materialType,omitempty"`
	QuestionId        string         `bson:"questionId"`
	PaperId           string         `bson:"paperId"`
	PaperName         string         `bson:"paperName"`
	SectionName       string         `bson:"sectionName,omitempty"`
	ModuleIdentifier  string         `bson:"moduleIdentifier,omitempty"`
	TaxonomyData      []TaxonomyData `bson:"taxonomyData"`
	ModuleIdentifier2 string         `bson:"moduleIdentifier2,omitempty"`
	IsDeleted         bool           `bson:"isDeleted"`
	PaperCode         string         `bson:"paper_code"`
	CreatedBy         string         `bson:"createdBy,omitempty"`
	UpdatedBy         string         `bson:"updatedBy,omitempty"`
	CreatedAt         int64          `bson:"createdAt,omitempty"`
	UpdatedAt         int64          `bson:"updatedAt,omitempty"`
	CenterName        string         `bson:"centerName,omitempty"`
}

type TaxonomyData struct {
	TaxonomyId   string  `bson:"taxonomyId,omitempty"`
	ClassId      string  `bson:"classId,omitempty"`
	SubjectId    string  `bson:"subjectId,omitempty"`
	TopicId      string  `bson:"topicId,omitempty"`
	SubTopicId   string  `bson:"subtopicId,omitempty"`
	SuperTopicId *string `bson:"supertopicId,omitempty"`
}
