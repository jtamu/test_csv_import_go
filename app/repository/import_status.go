package repository

import "my-s3-function-go/app/domain/importstatus"

type ImportStatusRepository struct {
	baseRepository *BaseRepository
}

func NewImportStatusRepository(baseRepository *BaseRepository) *ImportStatusRepository {
	return &ImportStatusRepository{baseRepository: baseRepository}
}

func (i *ImportStatusRepository) GetOneByFilePath(filePath string) (*importstatus.ImportStatus, error) {
	importstatus := importstatus.ImportStatus{}
	if err := i.baseRepository.db.Where("file_path = ?", filePath).First(&importstatus).Error; err != nil {
		return nil, err
	}
	return &importstatus, nil
}
