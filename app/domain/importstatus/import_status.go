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

func (i *ImportStatus) AppendDetail(row int, msg string) {
	i.Details = append(i.Details, NewImportDetail(&row, msg))
}

func (i *ImportStatus) Finished() {
	i.Status = config.FINISHED
}

func (i *ImportStatus) SetRecordCount(count int) {
	i.RecordCount = count
}

func (i *ImportStatus) IncrementProcessedCount() {
	i.ProcessedCount += 1
}
