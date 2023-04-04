package repository

import (
	"fmt"
	"my-s3-function-go/config"
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
	cfg := config.Cfg
	user := os.Getenv(cfg.DB.User)
	pass := os.Getenv(cfg.DB.Password)
	host := os.Getenv(cfg.DB.Host)
	port := os.Getenv(cfg.DB.Port)
	dbname := os.Getenv(cfg.DB.DBName)

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8&parseTime=true", user, pass, host, port, dbname)
	var err error
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		panic("failed to connect database")
	}
	baseRepository = NewBaseRepository(db)
}
