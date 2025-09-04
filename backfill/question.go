package main

type QuestionStem struct {
	Text  string  `bson:"text,omitempty"`
	Media []Media `bson:"media,omitempty"`
}

type Option struct {
	Text        string  `bson:"text,omitempty"`
	Explanation string  `bson:"explanation,omitempty"`
	SequenceID  int32   `bson:"sequenceId"`
	Media       []Media `bson:"media,omitempty"`
}

type Media struct {
	MediaID     string `json:"media_id" bson:"mediaId,omitempty"`
	MediaPath   string `json:"media_path" bson:"mediaPath,omitempty"`
	MediaType   string `json:"media_type" bson:"mediaType,omitempty"`
	LatexSource string `json:"latex_source" bson:"latexSource,omitempty"`
}

type Content struct {
	Language           int32        `bson:"language,omitempty"`
	Answer             string       `bson:"answer,omitempty"`
	QuestionNature     int32        `bson:"questionNature,omitempty"`
	IsGroupContent     bool         `bson:"isGroupContent,omitempty"`
	GroupID            string       `bson:"groupId,omitempty"`
	QuestionStem       QuestionStem `bson:"questionStem,omitempty"`
	Options            []Option     `bson:"options,omitempty"`
	MatchOptions       *MatchOption `bson:"matchOptions,omitempty"`
	DifficultyLevel    int32        `bson:"difficultyLevel,omitempty"`
	CreatedBy          string       `bson:"createdBy,omitempty"`
	CreatedAt          int64        `bson:"createdAt,omitempty"`
	ModifiedBy         string       `bson:"modifiedBy,omitempty"`
	ModifiedAt         int64        `bson:"modifiedAt,omitempty"`
	EnteredBy          string       `bson:"enteredBy,omitempty"`
	ApprovedBy         string       `bson:"approvedBy,omitempty"`
	HasAnswer          bool         `bson:"hasAnswer,omitempty"`
	HasTextSolution    bool         `bson:"hasTextSolution,omitempty"`
	LearningObjectives []string     `bson:"learningObjectives,omitempty"`
	Mismatched         bool         `bson:"mismatched"`
	TextSolutionStatus int32        `bson:"textSolutionStatus,omitempty"`
	DisplayAnswer      string       `bson:"displayAnswer,omitempty"`
	DisplayOptions     string       `bson:"displayOptions,omitempty"`
	GroupVersion       string       `bson:"groupVersion,omitempty"`
}

type BookReference struct {
	BookID      string `bson:"bookId,omitempty"`
	SubjectName string `bson:"subjectName,omitempty"`
	Chapter     string `bson:"chapter,omitempty"` // to remove
	Pages       string `bson:"pages,omitempty"`   // to remove
	Text        string `bson:"text,omitempty"`    // to remove
}

type Duplication struct {
	QuestionID                  string  `bson:"questionId,omitempty"`
	PercentDuplication          float64 `bson:"percentDuplication,omitempty"`
	IsDuplicationConfirmed      bool    `bson:"isDuplicationConfirmed,omitempty"`
	DuplicationConfirmationTime int64   `bson:"duplicationConfirmationTimeStamp,omitempty"`
	DuplicationConfirmedBy      string  `bson:"duplicationConfirmedBy,omitempty"`
	questionDoc                 QuestionDocument
}

type QuestionDocument struct {
	ID                     string             `bson:"_id,omitempty"`
	TenantID               string             `bson:"tenantId,omitempty"`
	Nature                 int32              `bson:"nature,omitempty"`
	QuestionID             string             `bson:"questionId,omitempty"`
	OldQuestionID          int64              `bson:"oldQuestionId,omitempty"`
	Type                   int32              `bson:"type,omitempty"`
	Version                int64              `bson:"version,omitempty"`
	TaxonomyID             string             `bson:"taxonomyId,omitempty"`
	UniqueIdentifier       string             `bson:"uniqueIdentifier,omitempty"`
	VideoIdentifier        string             `bson:"videoIdentifier,omitempty"`
	Streams                []string           `bson:"streams,omitempty"`
	Content                []Content          `bson:"content,omitempty"`
	BookReferences         []BookReference    `bson:"bookReferences,omitempty"`
	Tags                   []Tag              `bson:"tags,omitempty"`
	OldTags                []Tag              `bson:"oldTags,omitempty"`
	Status                 int32              `bson:"status,omitempty"`
	IsTestEligible         bool               `bson:"isTestEligible,omitempty"`
	LearningObjectives     []string           `bson:"learningObjectives,omitempty"`
	Duplications           []Duplication      `bson:"duplications,omitempty"`
	CustomTags             []CustomTag        `bson:"customTags,omitempty"`
	TaxonomyData           []TaxonomyData     `bson:"taxonomyData,omitempty"`
	GroupContent           []ContentDoc       `bson:"groupContent,omitempty"`
	Session                int64              `bson:"session,omitempty"`
	QnsLevel               int32              `bson:"qnsLevel,omitempty"`
	VideoUploaded          int64              `bson:"videoUploaded,omitempty"`
	VideoSolutionPaperCode string             `bson:"videoSolutionPaperCode,omitempty"`
	Source                 int32              `bson:"source,omitempty"`
	SourceCenter           string             `bson:"sourceCenter,omitempty"`
	CreatedBy              string             `bson:"createdBy,omitempty"`
	UpdatedBy              string             `bson:"updatedBy,omitempty"`
	CreatedAt              int64              `bson:"createdAt"`
	UpdatedAt              int64              `bson:"updatedAt"`
	QuestionValidation     QuestionValidation `bson:"questionValidation,omitempty"`
	OldTaxonomyData        []OldTaxonomyData  `bson:"oldTaxonomyData,omitempty"`
	HasVideoSolution       bool               `bson:"hasVideoSolution,omitempty"`
	VideoSolutionStatus    int32              `bson:"videoSolutionStatus,omitempty"`
	IsPractice             int32              `bson:"isPractice,omitempty"`
	IsSingleCorrect        int32              `bson:"isSingleCorrect,omitempty"`
	FacultyBy              string             `bson:"facultyBy,omitempty"`
	DirtyLevel             int32              `bson:"dirtyLevel,omitempty"`
	HashTags               []HashTags         `bson:"hashTags,omitempty"`
	ExtraInfo              string             `bson:"extraInfo,omitempty"`
	DuplicacyStatus        int32              `bson:"duplicacyStatus,omitempty"`
	QuestionQualityStatus  QuestionQualityStatus `bson:"questionQualityStatus,omitempty"`
	FacultyName            string                `bson:"facultyName,omitempty"`
}

type QuestionQualityStatus int32

type HashTags struct {
	HashTagID   string `bson:"hashTagId,omitempty"`
	Description string `bson:"description,omitempty"`
}

type OldTaxonomyData struct {
	Stream   string `bson:"stream,omitempty"`
	Class    string `bson:"class,omitempty"`
	Subject  string `bson:"subject,omitempty"`
	Topic    string `bson:"topic,omitempty"`
	SubTopic string `bson:"subtopic,omitempty"`
	TaxonomyId string `bson:"taxonomyId,omitempty"`
}

type QuestionValidation struct {
	IsValid          bool              `bson:"isValid"`
	ValidationErrors []ValidationError `bson:"validationErrors"`
}

type ValidationError string

const (
	StreamNameInvalid         ValidationError = "StreamNameInvalid"
	ClassNameInvalid          ValidationError = "ClassNameInvalid"
	QuestionLevelInvalid      ValidationError = "QuestionLevelInvalid"
	QuestionTypeInvalid       ValidationError = "QuestionTypeInvalid"
	LanguageMismatched        ValidationError = "LanguageMismatched"
	DirtyLevelInvalid         ValidationError = "DirtyLevelInvalid"
	TaxonomyDataMissing       ValidationError = "TaxonomyDataMissing"
	TaxonomyDataFieldsMissing ValidationError = "TaxonomyDataFieldsMissing"
)

func (e ValidationError) String() string {
	switch e {
	case StreamNameInvalid:
		return "Stream name is not valid"
	case ClassNameInvalid:
		return "Class name is not valid"
	case QuestionLevelInvalid:
		return "Question level is not valid"
	case QuestionTypeInvalid:
		return "Type is not valid"
	case LanguageMismatched:
		return "Language mismatched"
	case DirtyLevelInvalid:
		return "Dirty level is invalid"
	case TaxonomyDataMissing:
		return "Taxonomy data is missing"
	case TaxonomyDataFieldsMissing:
		return "Missing required fields in taxonomy data"
	default:
		return "Unknown validation error"
	}
}

type QuestionUsageHistory struct {
	ID            string   `bson:"_id,omitempty"`
	OldQuestionID int64    `bson:"oldQuestionId,omitempty"`
	TestIDs       []string `bson:"testIds,omitempty"`
}

type Tag struct {
	Name  string `bson:"name,omitempty"`
	Value string `bson:"value,omitempty"`
}

type CustomTag struct {
	TagName string `bson:"tag_name,omitempty"`
	Value   string `bson:"value,omitempty"`
	TagType string `bson:"tag_type,omitempty"`
}

type TaxonomyData struct {
	TaxonomyId string `bson:"taxonomyId,omitempty"`
	ClassId    string `bson:"classId,omitempty"`
	SubjectId  string `bson:"subjectId,omitempty"`
	TopicId    string `bson:"topicId,omitempty"`
	SubTopicId string `bson:"subtopicId,omitempty"`
	SuperTopicId string `bson:"supertopicId,omitempty"`
}

type MatchOption struct {
	Row   string      `bson:"row,omitempty"`
	Cols  string      `bson:"cols,omitempty"`
	Label interface{} `bson:"label,omitempty"`
}

type QuestionData struct {
	QuestionID    string    `bson:"questionId,omitempty"`
	OldQuestionID int64     `bson:"oldQuestionId,omitempty"`
	Content       []Content `bson:"content,omitempty"`
	OldTags       []Tag     `bson:"oldTags,omitempty"`
	QnsLevel      int32     `bson:"qnsLevel,omitempty"`
}

type VideoContent struct {
	QuestionID         string `bson:"questionId,omitempty"`
	OldQuestionID      int64  `bson:"oldQuestionId,omitempty"`
	LearningMaterialId string `bson:"learning_material_id,omitempty"`
}

type GroupedQuestion struct {
	ID  string            `bson:"_id"`
	Doc *QuestionDocument `bson:"doc"`
}

type ContentDoc struct {
	LanguageId int64  `bson:"languageId"`
	Language   string `bson:"language"`
	Text       string `bson:"text"`
}


