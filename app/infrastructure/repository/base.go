package repository

import (
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type BaseRepository struct {
	db *gorm.DB
}

func NewBaseRepository(db *gorm.DB) *BaseRepository {
	return &BaseRepository{db: db}
}

func (b *BaseRepository) Save(obj interface{}) error {
	return b.db.Clauses(clause.OnConflict{
		UpdateAll: true,
	}).Create(obj).Error
}
