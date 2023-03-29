package importstatus

import "time"

type ImportStatus struct {
	ID             int `gorm:"primaryKey"`
	FileName       string
	FilePath       string
	RecordCount    int
	ProcessedCount int
	Status         string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}
