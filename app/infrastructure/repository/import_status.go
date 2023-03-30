package repository

import "my-s3-function-go/app/domain/importstatus"

type ImportStatusRepository struct {
	baseRepository *BaseRepository
}

func NewImportStatusRepository() *ImportStatusRepository {
	return &ImportStatusRepository{baseRepository: baseRepository}
}

func (i *ImportStatusRepository) GetOneByFilePath(filePath string) (*importstatus.ImportStatus, error) {
	importstatus := importstatus.ImportStatus{}
	if err := i.baseRepository.db.Where("file_path = ?", filePath).Preload("Details").First(&importstatus).Error; err != nil {
		return nil, err
	}
	return &importstatus, nil
}

func (i *ImportStatusRepository) Save(importStatus *importstatus.ImportStatus) error {
	return i.baseRepository.Save(importStatus)
}
