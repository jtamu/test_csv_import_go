package repository

import (
	"fmt"
	"os"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var baseRepository *BaseRepository

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

func init() {
	USER := os.Getenv("user")
	PASS := os.Getenv("password")
	HOST := os.Getenv("host")
	PORT := os.Getenv("port")
	DBNAME := os.Getenv("dbname")
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8&parseTime=true", USER, PASS, HOST, PORT, DBNAME)
	var err error
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		panic("failed to connect database")
	}
	baseRepository = NewBaseRepository(db)
}
