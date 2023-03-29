package importstatus

import (
	"fmt"
	"my-s3-function-go/config"
	"time"
)

var status = config.Cfg.ImportStatus

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

func (i *ImportStatus) ShouldBePending() error {
	if i.Status == status.Pending {
		return nil
	}
	return fmt.Errorf("%sは取り込み待ちステータスではありません", i.FileName)
}

func (i *ImportStatus) Processing() {
	i.Status = status.Processing
}

func (i *ImportStatus) Failed(err error) {
	i.Status = status.Failed
	i.Details = append(i.Details, NewImportDetail(nil, err.Error()))
}

func (i *ImportStatus) AppendDetail(row int, msg string) {
	i.Details = append(i.Details, NewImportDetail(&row, msg))
}

func (i *ImportStatus) Finished() {
	i.Status = status.Finished
}

func (i *ImportStatus) SetRecordCount(count int) {
	i.RecordCount = count
}

func (i *ImportStatus) IncrementProcessedCount() {
	i.ProcessedCount += 1
}
