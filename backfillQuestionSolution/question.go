package main

type Media struct {
	MediaID     string `json:"media_id" bson:"mediaId,omitempty"`
	MediaPath   string `json:"media_path" bson:"mediaPath,omitempty"`
	MediaType   string `json:"media_type" bson:"mediaType,omitempty"`
	LatexSource string `json:"latex_source" bson:"latexSource,omitempty"`
}

type TextSolutionDocument struct {
	Language   int32   `json:"language" bson:"language,omitempty"`
	Text       string  `json:"text" bson:"text,omitempty"`
	Media      []Media `json:"media" bson:"media,omitempty"`
	ModifiedBy string  `json:"modified_by" bson:"modifiedBy,omitempty"`
	ModifiedAt int64   `json:"modified_at" bson:"modifiedAt,omitempty"`
	EnteredBy  string  `json:"entered_by" bson:"enteredBy,omitempty"`
	ApprovedBy string  `json:"approved_by" bson:"approvedBy,omitempty"`
	CreatedBy  string  `json:"created_by" bson:"createdBy,omitempty"`
	CreatedAt  int64   `json:"created_at" bson:"createdAt,omitempty"`
	Status     int32   `json:"status" bson:"status,omitempty"`
}

type VideoSolutionDocument struct {
	VTag       string `json:"v_tag" bson:"vTag,omitempty"`
	Language   int32  `json:"language" bson:"language,omitempty"`
	ContentID  string `json:"content_id" bson:"contentId,omitempty"`
	VideoPath  string `json:"video_path" bson:"videoPath,omitempty"`
	ModifiedBy string `json:"modified_by" bson:"modifiedBy,omitempty"`
	ModifiedAt int64  `json:"modified_at" bson:"modifiedAt,omitempty"`
	EnteredBy  string `json:"entered_by" bson:"enteredBy,omitempty"`
	ApprovedBy string `json:"approved_by" bson:"approvedBy,omitempty"`
	CreatedBy  string `json:"created_by" bson:"createdBy,omitempty"`
	CreatedAt  int64  `json:"created_at" bson:"createdAt,omitempty"`
	Status     int32  `json:"status" bson:"status,omitempty"`
	VTag2      string `json:"v_tag2" bson:"vTag2,omitempty"`
	VTagType   int32  `json:"v_tag_type" bson:"vTagType,omitempty"`
}

type QuestionSolution struct {
	ID             string                   `json:"_id" bson:"_id,omitempty"`
	QuestionID     string                   `json:"question_id" bson:"questionId,omitempty"`
	VersionID      int64                    `json:"version_id" bson:"versionId,omitempty"`
	OldQuestionID  int64                    `json:"old_question_id" bson:"oldQuestionId,omitempty"`
	TextSolutions  []*TextSolutionDocument  `json:"text_solutions" bson:"textSolutions,omitempty"`
	VideoSolutions []*VideoSolutionDocument `json:"video_solutions" bson:"videoSolutions,omitempty"`
	CreatedBy      string                   `bson:"createdBy,omitempty"`
	UpdatedBy      string                   `bson:"updatedBy,omitempty"`
	CreatedAt      int64                    `bson:"createdAt,omitempty"`
	UpdatedAt      int64                    `bson:"updatedAt,omitempty"`
}
