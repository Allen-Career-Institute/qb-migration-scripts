package main

type NewQuestionSetDocument struct {
	QuestionSetID       string               `bson:"_id"`
	OldPaperID          int64                `bson:"oldPaperId,omitempty"`
	TenantID            string               `bson:"tenantId,omitempty"`
	Name                string               `bson:"name"`
	Stream              string               `bson:"stream"`
	Session             string               `bson:"session"`
	Phase               string               `bson:"phase"`
	Status              int32                `bson:"status"`
	PaperCode           string               `bson:"paperCode"`
	UniqueIdentifier    string               `bson:"uniqueIdentifier"`
	MaxMarks            float32              `bson:"maxMarks"`
	TotalTime           Time                 `bson:"totalTime"`
	TestType            string               `bson:"testType"`
	NumberOfQuestions   int32                `bson:"numberOfQuestions"`
	Instructions        map[string]string    `bson:"instructions"`
	TestDate            int64                `bson:"testDate,omitempty"`
	LastUpdatedAt       int64                `bson:"lastUpdatedAt,omitempty"`
	LastUpdatedBy       string               `bson:"lastUpdatedBy"`
	ApprovedBy          string               `bson:"approvedBy"`
	CreatedAt           int64                `bson:"createdAt,omitempty"`
	CreatedBy           string               `bson:"createdBy"`
	PrimaryLanguage     string               `bson:"primaryLanguage"`
	Program             string               `bson:"program"`
	IsMultilingual      bool                 `bson:"isMultilingual"`
	Languages           []string             `bson:"languages"`
	Pattern             string               `bson:"pattern"`
	Tags                []Tag                `bson:"tags,omitempty"`
	Questions           []Question           `bson:"questions"`
	QuestionSetSections []QuestionSetSection `bson:"paperSections"`
	TaxonomyId          string               `bson:"taxonomyId"`
	Course              string               `bson:"course,omitempty"`
	Target              string               `bson:"target,omitempty"`
	Lock                bool                 `bson:"lock,omitempty"`
	PaperProgramsId     int64                `bson:"paperProgramId,omitempty"`
	PaperPdfUrls        string               `bson:"paper_pdf_urls,omitempty"`
	UpdatedBy           string               `bson:"updatedBy,omitempty"`
	UpdatedAt           int64                `bson:"updatedAt,omitempty"`
	Platform            string               `bson:"platform,omitempty"`
	PaperPhase          string               `bson:"paperPhase,omitempty"`
	MasterPhase         string               `bson:"masterPhase,omitempty"`
	Layout              int32                `bson:"layout,omitempty"`
	Watermark           string               `bson:"watermark,omitempty"`
	PaperNo             int32                `bson:"paperNo,omitempty"`
	TestNo              int32                `bson:"testNo,omitempty"`
	TestMode            string               `bson:"testMode,omitempty"`
	CenterId            string               `bson:"centerId,omitempty"`
	CenterName          string               `bson:"centerName,omitempty"`
	PassKey             string               `bson:"passKey,omitempty"`
	Method              string               `bson:"method,omitempty"`
}

type QuestionSetDocument struct {
	QuestionSetID       string               `bson:"_id"`
	OldPaperID          int64                `bson:"oldPaperId,omitempty"`
	TenantID            string               `bson:"tenantId,omitempty"`
	Name                string               `bson:"name"`
	Stream              string               `bson:"stream"`
	Session             string               `bson:"session"`
	Phase               string               `bson:"phase"`
	Status              int32                `bson:"status"`
	PaperCode           string               `bson:"paperCode"`
	UniqueIdentifier    string               `bson:"uniqueIdentifier"`
	MaxMarks            float32              `bson:"maxMarks"`
	TotalTime           Time                 `bson:"totalTime"`
	TestType            string               `bson:"testType"`
	NumberOfQuestions   int32                `bson:"numberOfQuestions"`
	Instructions        map[string]string    `bson:"instructions"`
	TestDate            int64                `bson:"testDate,omitempty"`
	LastUpdatedAt       int64                `bson:"lastUpdatedAt,omitempty"`
	LastUpdatedBy       string               `bson:"lastUpdatedBy"`
	ApprovedBy          string               `bson:"approvedBy"`
	CreatedAt           int64                `bson:"createdAt,omitempty"`
	CreatedBy           string               `bson:"createdBy"`
	PrimaryLanguage     string               `bson:"primaryLanguage"`
	Program             string               `bson:"program"`
	IsMultilingual      bool                 `bson:"isMultilingual"`
	Languages           []string             `bson:"languages"`
	Pattern             string               `bson:"pattern"`
	Tags                []Tag                `bson:"tags,omitempty"`
	Questions           []Question           `bson:"questions"`
	QuestionSetSections []QuestionSetSection `bson:"paperSections"`
	TaxonomyId          string               `bson:"taxonomyId"`
	Course              string               `bson:"course,omitempty"`
	Target              string               `bson:"target,omitempty"`
	Lock                bool                 `bson:"lock,omitempty"`
	PaperProgramsId     int64                `bson:"paperProgramId,omitempty"`
	PaperPdfUrls        string               `bson:"paper_pdf_urls,omitempty"`
	UpdatedBy           string               `bson:"updatedBy,omitempty"`
	UpdatedAt           int64                `bson:"updatedAt,omitempty"`
	Platform            string               `bson:"platform,omitempty"`
	PaperPhase          string               `bson:"paperPhase,omitempty"`
	MasterPhase         string               `bson:"masterPhase,omitempty"`
	Layout              int32                `bson:"layout,omitempty"`
	Watermark           string               `bson:"watermark,omitempty"`
	PaperNo             int32                `bson:"paperNo,omitempty"`
	TestNo              int32                `bson:"testNo,omitempty"`
	TestMode            string               `bson:"testMode,omitempty"`
	CenterId            string               `bson:"centerId,omitempty"`
	CenterName          string               `bson:"centerName,omitempty"`
	PassKey             string               `bson:"passKey,omitempty"`
	SharedInfo          []SharedInfo         `bson:"sharedInfo,omitempty"`
	Method              string               `bson:"method,omitempty"`
}

type SharedInfo struct {
	SharedBy     string `bson:"sharedBy,omitempty"`
	CenterId     string `bson:"centerId,omitempty"`
	FromCenterId string `bson:"fromCenterId,omitempty"`
	Date         int64  `bson:"date,omitempty"`
	Status       int32  `bson:"status,omitempty"`
	SharedType   int32  `bson:"sharedType,omitempty"`
}

type Time struct {
	Unit  string `bson:"unit"`
	Value int32  `bson:"value"`
}

type QuestionSetSection struct {
	Name                          string                   `bson:"name"`
	TopicList                     []string                 `bson:"topicList"`
	Instructions                  map[string]string        `bson:"instructions"`
	HaveSubsections               bool                     `bson:"haveSubsections"`
	SequenceID                    int32                    `bson:"sequenceId"`
	ParentSequenceID              int32                    `bson:"parentSequenceId,omitempty"`
	MaxMarks                      float64                  `bson:"maxMarks"`
	NumberOfQuestions             int32                    `bson:"numberOfQuestions"`
	Subsections                   []QuestionSetSection     `bson:"subsections"`
	Questions                     []Question               `bson:"questions"`
	ParentSectionID               int64                    `bson:"parentSectionId,omitempty"`
	SectionID                     int64                    `bson:"sectionId,omitempty"`
	Tags                          []Tag                    `bson:"tags,omitempty"`
	Namespace                     string                   `bson:"namespace"`
	TotalQuestions                int32                    `bson:"totalQuestions"`
	TotalAttemptableQuestions     int32                    `bson:"totalAttemptableQuestions"`
	TotalMarks                    float64                  `bson:"totalMarks"`
	TotalAttemptableQuestionMarks float64                  `bson:"totalAttemptableQuestionMarks"`
	Subject                       string                   `bson:"subject"`
	Type                          int32                    `bson:"type,omitempty"`
	OmrSection                    string                   `bson:"omrSection,omitempty"`
	MarkingSchemePerQuestion      MarkingSchemePerQuestion `bson:"markingSchemePerQuestion,omitempty"`
}

type Question struct {
	QuestionID                  string                   `bson:"id"`
	UniqueIdentifier            string                   `bson:"uniqueIdentifier"`
	Version                     int64                    `bson:"version,omitempty"`
	TaggingStatus               string                   `bson:"taggingStatus"`
	PartialMarkingType          string                   `bson:"partialMarkingType"`
	Marks                       float32                  `bson:"marks"`
	NegMarks                    float32                  `bson:"negMarks"`
	PartialCorrectMarkingScheme PartialCorrectionMarking `bson:"partialCorrectMarkingScheme"`
	SequenceNo                  int32                    `bson:"sequenceNo"`
	FacultyId                   string                   `bson:"facultyId"`
	GroupId                     string                   `bson:"groupId,omitempty"`
	GroupVersion                string                   `bson:"groupVersion,omitempty"`
}

type PartialCorrectionMarking struct {
	OptionSequenceID int32   `bson:"optionSequenceId"`
	CorrectMarks     float32 `bson:"correctMarks"`
	NegativeMarks    float32 `bson:"negativeMarks"`
}

type MarkingSchemePerQuestion struct {
	NegMarks            float64 `bson:"negMarks"`
	CorrectMarks        float64 `bson:"correctMarks"`
	PartialNegMarks     float64 `bson:"partialNegMarks"`
	PartialCorrectMarks float64 `bson:"partialCorrectMarks"`
}

type Tag struct {
	Name  string `bson:"name,omitempty"`
	Value string `bson:"value,omitempty"`
}

func ConvertNewToOld(newDoc NewQuestionSetDocument) QuestionSetDocument {
	questionDoc := QuestionSetDocument{
		QuestionSetID:       newDoc.QuestionSetID,
		OldPaperID:          newDoc.OldPaperID,
		TenantID:            newDoc.TenantID,
		Name:                newDoc.Name,
		Stream:              newDoc.Stream,
		Session:             newDoc.Session,
		Phase:               newDoc.Phase,
		Status:              newDoc.Status,
		PaperCode:           newDoc.PaperCode,
		UniqueIdentifier:    newDoc.UniqueIdentifier,
		MaxMarks:            newDoc.MaxMarks,
		TotalTime:           newDoc.TotalTime,
		TestType:            newDoc.TestType,
		NumberOfQuestions:   newDoc.NumberOfQuestions,
		Instructions:        newDoc.Instructions,
		TestDate:            newDoc.TestDate,
		LastUpdatedAt:       newDoc.LastUpdatedAt,
		LastUpdatedBy:       newDoc.LastUpdatedBy,
		ApprovedBy:          newDoc.ApprovedBy,
		CreatedAt:           newDoc.CreatedAt,
		CreatedBy:           newDoc.CreatedBy,
		PrimaryLanguage:     newDoc.PrimaryLanguage,
		Program:             newDoc.Program,
		IsMultilingual:      newDoc.IsMultilingual,
		Languages:           newDoc.Languages,
		Pattern:             newDoc.Pattern,
		Tags:                newDoc.Tags,
		Questions:           newDoc.Questions,
		QuestionSetSections: newDoc.QuestionSetSections,
		TaxonomyId:          newDoc.TaxonomyId,
		Course:              newDoc.Course,
		Target:              newDoc.Target,
		Lock:                newDoc.Lock,
		PaperProgramsId:     newDoc.PaperProgramsId,
		PaperPdfUrls:        newDoc.PaperPdfUrls,
		UpdatedBy:           newDoc.UpdatedBy,
		UpdatedAt:           newDoc.UpdatedAt,
		Platform:            newDoc.Platform,
		PaperPhase:          newDoc.PaperPhase,
		MasterPhase:         newDoc.MasterPhase,
		Layout:              newDoc.Layout,
		Watermark:           newDoc.Watermark,
		PaperNo:             newDoc.PaperNo,
		TestNo:              newDoc.TestNo,
		TestMode:            newDoc.TestMode,
		CenterId:            newDoc.CenterId,
		CenterName:          newDoc.CenterName,
		PassKey:             newDoc.PassKey,
		Method:              newDoc.Method,
	}

	return questionDoc
}
