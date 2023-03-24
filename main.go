package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/go-playground/locales/ja"
	ut "github.com/go-playground/universal-translator"
	"github.com/jszwec/csvutil"
	"gopkg.in/go-playground/validator.v9"
	ja_translations "gopkg.in/go-playground/validator.v9/translations/ja"
)

var (
	uni      *ut.UniversalTranslator
	validate *validator.Validate
	trans    ut.Translator
	sess     *session.Session
	svc      *s3.S3
	db       *gorm.DB
)

const (
	PENDING    = "Pending"
	PROCESSING = "Processing"
	FINISHED   = "Finished"
	FAILED     = "Failed"
)

type User struct {
	ID    int    `csv:"id" jaFieldName:"ID" validate:"required"`
	Name  string `csv:"name" jaFieldName:"ユーザ名" validate:"required"`
	Email string `csv:"email" jaFieldName:"メールアドレス" validate:"required"`
}

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

type ImportDetail struct {
	ID             int `gorm:"primaryKey"`
	ImportStatusID int
	RowNumber      int
	Detail         string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

func dbInit() {
	USER := os.Getenv("user")
	PASS := os.Getenv("password")
	HOST := os.Getenv("host")
	PORT := os.Getenv("port")
	DBNAME := os.Getenv("dbname")
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8&parseTime=true", USER, PASS, HOST, PORT, DBNAME)
	var err error
	db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		panic("failed to connect database")
	}
}

func Init() {
	dbInit()

	// セッション
	sess = session.Must(session.NewSession(&aws.Config{
		Region: aws.String("ap-northeast-1"),
	}))

	// S3クライアント
	svc = s3.New(sess)

	ja := ja.New()
	uni = ut.New(ja, ja)
	t, _ := uni.GetTranslator("ja")
	trans = t
	validate = validator.New()
	validate.RegisterTagNameFunc(func(fld reflect.StructField) string {
		fieldName := fld.Tag.Get("jaFieldName")
		if fieldName == "-" {
			return ""
		}
		return fieldName
	})
	ja_translations.RegisterDefaultTranslations(validate, trans)
}

func s3lambda(ctx context.Context, event events.S3Event) (interface{}, error) {
	for _, record := range event.Records {
		// recordの中にイベント発生させたS3のBucket名やKeyが入っている
		bucket := record.S3.Bucket.Name
		key := record.S3.Object.Key

		importStatus := ImportStatus{}
		if err := db.Where("file_path = ?", key).First(&importStatus).Error; err != nil {
			return nil, err
		}
		importStatus.Status = PROCESSING
		if err := db.Clauses(clause.OnConflict{
			UpdateAll: true,
		}).Create(&importStatus).Error; err != nil {
			return nil, err
		}

		obj, err := svc.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			return nil, err
		}
		csv, err := io.ReadAll(obj.Body)
		if err != nil {
			return nil, err
		}
		var users []User
		err = csvutil.Unmarshal(csv, &users)
		if err != nil {
			return nil, err
		}

		importStatus.RecordCount = len(users)
		if err := db.Clauses(clause.OnConflict{
			UpdateAll: true,
		}).Create(&importStatus).Error; err != nil {
			return nil, err
		}

		for i, user := range users {
			if err := validate.Struct(user); err != nil {
				importDetail := ImportDetail{
					ImportStatusID: importStatus.ID,
					RowNumber:      i + 1,
					Detail:         strings.Join(GetErrorMessages(err), ","),
				}
				if err := db.Clauses(clause.OnConflict{
					UpdateAll: true,
				}).Create(&importDetail).Error; err != nil {
					return nil, err
				}
			}

			importStatus.ProcessedCount = i + 1
			if err := db.Clauses(clause.OnConflict{
				UpdateAll: true,
			}).Create(&importStatus).Error; err != nil {
				return nil, err
			}
		}

		importStatus.Status = FINISHED
		if err := db.Clauses(clause.OnConflict{
			UpdateAll: true,
		}).Create(&importStatus).Error; err != nil {
			return nil, err
		}
	}
	resp := &struct {
		StatusCode uint `json:"statusCode"`
	}{StatusCode: 200}
	return resp, nil
}

func GetErrorMessages(err error) []string {
	if err == nil {
		return []string{}
	}
	var messages []string
	for _, m := range err.(validator.ValidationErrors).Translate(trans) {
		messages = append(messages, m)
	}
	return messages
}

func main() {
	Init()
	lambda.Start(s3lambda)
}
