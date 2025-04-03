package main

type MQuestion struct {
	ID       int     `json:"id"`
	VTag     *string `json:"vtag,omitempty"`
	VTagType *string `json:"vtag_type,omitempty"`
}
