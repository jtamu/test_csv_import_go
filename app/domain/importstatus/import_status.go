package importstatus

import (
	"my-s3-function-go/config"
	"time"
)

type ImportStatus struct {
	ID             int `gorm:"primaryKey"`
	FileName       string
	FilePath       string
	RecordCount    int
	ProcessedCount int
	Status         string
	CreatedAt      time.Time
	UpdatedAt      time.Time

	Details []*ImportDetail
}

func (i *ImportStatus) Processing() {
	i.Status = config.PROCESSING
}

func (i *ImportStatus) Failed(err error) {
	i.Status = config.FAILED
	i.Details = append(i.Details, NewImportDetail(nil, err.Error()))
}
