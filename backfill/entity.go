package main

type MQuestion struct {
	ID                     int     `json:"id"`
	VideoSolutionPaperCode *string `json:"video_solution_papercode,omitempty"`
	SourceMaterial         *string `json:"source_material"`
	CenterInfoID           *int64  `json:"center_info_id,omitempty"`
	DirtyLevel             *int32  `json:"dirty_level"`
	IsPractice             *int32  `json:"is_practice,omitempty"`
	FacultyBy              *string `json:"faculty_by,omitempty"`
	ExtraInfo              *string `json:"extra_info,omitempty"`
	DuplicacyStatus        *string `json:"duplicacy_status,omitempty"`
	CenterName             *string `json:"name,omitempty"`
	IsSingleCorrect        *int32  `json:"is_single_correct,omitempty"`
}

type MQuestionContent struct {
	LearningObjective *string `json:"learning_objective"`
	DisplayOptions    *string `json:"display_options"`
	DisplayAnswer     *string `json:"display_answer"`
	Mismatched        string  `json:"mismatched"`
	QnsID             int64   `json:"qns_id"`
	Language          int32   `json:"language"`
}

type MHashTags struct {
	HashTag     string  `json:"hashtag"`
	Description *string `json:"description"`
	QuestionID  int64   `json:"question_id"`
}
