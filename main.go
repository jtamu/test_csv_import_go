package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
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
	"github.com/aws/aws-sdk-go/service/sqs"
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
	sqsSvc   *sqs.SQS
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

type BaseRepository struct {
	db *gorm.DB
}

func (b *BaseRepository) Save(obj interface{}) error {
	return b.db.Clauses(clause.OnConflict{
		UpdateAll: true,
	}).Create(obj).Error
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

	// SQSのクライアントを作成
	sqsSvc = sqs.New(sess)

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

func s3lambda(ctx context.Context, sqsEvent events.SQSEvent) (interface{}, error) {
	baseRepository := &BaseRepository{
		db: db,
	}

	ch := make([]chan error, len(sqsEvent.Records))
	for i, _ := range ch {
		ch[i] = make(chan error)
	}

	for i, record := range sqsEvent.Records {
		go processEventRecord(record, baseRepository, ch[i])
	}

	for i, _ := range ch {
		if err := <-ch[i]; err != nil {
			log.Printf("%+v\n", err)
		}
	}

	resp := &struct {
		StatusCode uint `json:"statusCode"`
	}{StatusCode: 200}
	return resp, nil
}

func processEventRecord(record events.SQSMessage, baseRepository *BaseRepository, ch chan<- error) {
	b := []byte(record.Body)
	s3Object := events.S3EventRecord{}
	if err := json.Unmarshal(b, &s3Object); err != nil {
		ch <- err
		return
	}
	log.Printf("%+v\n", s3Object)

	bucket := s3Object.S3.Bucket.Name
	key := s3Object.S3.Object.Key

	var importStatus ImportStatus
	if err := db.Where("file_path = ?", key).First(&importStatus).Error; err != nil {
		ch <- err
		return
	}
	importStatus.Status = PROCESSING
	if err := baseRepository.Save(&importStatus); err != nil {
		ch <- err
		return
	}

	obj, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		ch <- err
		return
	}
	csv, err := io.ReadAll(obj.Body)
	if err != nil {
		ch <- err
		return
	}
	if err := importCSV(csv, &importStatus, baseRepository); err != nil {
		ch <- err
		return
	}

	importStatus.Status = FINISHED
	if err := baseRepository.Save(&importStatus); err != nil {
		ch <- err
		return
	}
	ch <- nil
}

func importCSV(csv []byte, importStatus *ImportStatus, baseRepository *BaseRepository) error {
	var users []User
	err := csvutil.Unmarshal(csv, &users)
	if err != nil {
		return err
	}

	importStatus.RecordCount = len(users)
	if err := baseRepository.Save(&importStatus); err != nil {
		return err
	}

	for i, user := range users {
		if err := validate.Struct(user); err != nil {
			importDetail := ImportDetail{
				ImportStatusID: importStatus.ID,
				RowNumber:      i + 1,
				Detail:         strings.Join(GetErrorMessages(err), ","),
			}
			if err := baseRepository.Save(&importDetail); err != nil {
				return err
			}
		} else {
			// バリデーションOKの場合
			// メッセージをJSON化
			msgJson, err := json.Marshal(user)
			if err != nil {
				return err
			}

			// 環境変数からキューURLを取得
			queueURL := os.Getenv("QUEUE_URL")
			// SQSに送信 (sqsSvc, queueURLは準備のときに作成したもの)
			if _, err := sqsSvc.SendMessage(&sqs.SendMessageInput{
				MessageBody: aws.String(string(msgJson)),
				QueueUrl:    &queueURL,
			}); err != nil {
				return err
			}
		}

		importStatus.ProcessedCount = i + 1
		if err := baseRepository.Save(&importStatus); err != nil {
			return err
		}
	}
	return nil
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
