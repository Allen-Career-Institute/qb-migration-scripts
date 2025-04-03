package main

type PaperInfo struct {
	ID             int64   `json:"id"`
	Platform       *string `json:"platform"`
	MasterPhase    *string `json:"master_phase"`
	PaperPhase     *string `json:"paper_phase"`
	Layout         *string `json:"layout"`
	PaperWatermark *string `json:"paper_watermark"`
	PaperNo        *int32  `json:"paper_no"`
	TestNo         *int32  `json:"test_no"`
	Passkey        *string `json:"passkey"`
	Method         *string `json:"type"`
	CenterInfoID   *int64  `json:"center_info_id"`
	CenterName     *string `json:"center_name"`
	TestModeName   *string `json:"test_mode_name"`
}
type SectionInfo struct {
	ID                         int64   `json:"id"`
	ParentID                   *int64  `json:"parent_id"`
	SequenceID                 *int64  `json:"sequence_id"`
	Type                       *string `json:"type"`
	OmrSection                 *string `json:"omr_section"`
	MarksPerQuestion           float64 `json:"marks_per_question"`
	NegMarksPerQuestion        float64 `json:"neg_marks_per_question"`
	PartialMarksPerQuestion    float64 `json:"partial_marks_per_question"`
	PartialNegMarksPerQuestion float64 `json:"partial_neg_marks_per_question"`
}

type PaperSectionQuestion struct {
	PaperID    int64   `json:"paper_id"`
	QuestionID *int64  `json:"question_id"`
	FacultyID  *string `json:"faculty_id"`
}

type SharedPaperCenter struct {
	SharedBy     *string `json:"shared_by"`
	CenterId     *int64  `json:"center_id"`
	FromCenterId *int64  `json:"from_center_id"`
	Date         *string `json:"date"`
	Status       *int32  `json:"status"`
	SharedType   *string `json:"shared_type"`
}
