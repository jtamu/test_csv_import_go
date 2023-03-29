package importstatus

import "time"

type ImportDetail struct {
	ID             int `gorm:"primaryKey"`
	ImportStatusID int
	RowNumber      *int
	Detail         string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

func NewImportDetail(rowNumber *int, detail string) *ImportDetail {
	return &ImportDetail{
		RowNumber: rowNumber,
		Detail:    detail,
	}
}
