package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"reflect"
	"strings"

	"my-s3-function-go/app/domain/importstatus"
	"my-s3-function-go/app/repository"
	"my-s3-function-go/config"

	_ "github.com/go-sql-driver/mysql"
	"github.com/saintfish/chardet"
	"golang.org/x/text/encoding/japanese"
	"golang.org/x/text/transform"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/jszwec/csvutil"
)

var (
	sess           *session.Session
	svc            *s3.S3
	db             *gorm.DB
	sqsSvc         *sqs.SQS
	baseRepository *repository.BaseRepository
)

const (
	PENDING    = config.PENDING
	PROCESSING = config.PROCESSING
	FINISHED   = config.FINISHED
	FAILED     = config.FAILED
)

type User struct {
	ID    int    `csv:"id" jaFieldName:"ID" validate:"required"`
	Name  string `csv:"name" jaFieldName:"ユーザ名" validate:"required"`
	Email string `csv:"email" jaFieldName:"メールアドレス" validate:"required,email-unique"`
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
	baseRepository = repository.NewBaseRepository(db)
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

	userRepository := repository.NewUserRepository(baseRepository)
	emails, err := userRepository.GetAllEmails()
	if err != nil {
		log.Printf("%+v\n", err)
		return
	}
	config.InitValidator(emails)
}

func s3lambda(ctx context.Context, sqsEvent events.SQSEvent) (interface{}, error) {
	importStatusRepository := repository.NewImportStatusRepository(baseRepository)

	ch := make([]chan error, len(sqsEvent.Records))
	for i, _ := range ch {
		ch[i] = make(chan error)
	}

	for i, record := range sqsEvent.Records {
		// NOTE: 先にループが回ってからgoroutineにスイッチするので、直接recordを渡してしまうと全てのgoroutineに最後のrecordのみが渡されてしまう
		go func(msg events.SQSMessage, chl chan<- error) {
			if err := processEventRecord(msg, baseRepository, importStatusRepository); err != nil {
				chl <- err
				return
			}
			chl <- nil
		}(record, ch[i])
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

func processEventRecord(record events.SQSMessage, baseRepository *repository.BaseRepository, importStatusRepository *repository.ImportStatusRepository) error {
	b := []byte(record.Body)
	s3Object := events.S3EventRecord{}
	if err := json.Unmarshal(b, &s3Object); err != nil {
		return err
	}
	log.Printf("%+v\n", s3Object)

	bucket := s3Object.S3.Bucket.Name
	key := s3Object.S3.Object.Key

	importStatus, err := importStatusRepository.GetOneByFilePath(key)
	if err != nil {
		return err
	}

	importStatus.Processing()
	if err := importStatusRepository.Save(importStatus); err != nil {
		return err
	}

	obj, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return err
	}
	csv, err := io.ReadAll(obj.Body)
	if err != nil {
		return err
	}
	csv, err = convertToUTF8(csv)
	if err != nil {
		importStatus.Failed(err)
		if err := importStatusRepository.Save(importStatus); err != nil {
			return err
		}
		return err
	}

	if err := validateHeader(csv, importStatus, baseRepository); err != nil {
		var invalidHeaderError *InvalidHeaderError
		if errors.As(err, &invalidHeaderError) {
			importStatus.Failed(err)
			if err := importStatusRepository.Save(importStatus); err != nil {
				return err
			}
		}
		return err
	}

	if err := importCSV(csv, importStatus, baseRepository, importStatusRepository); err != nil {
		return err
	}

	importStatus.Finished()
	if err := importStatusRepository.Save(importStatus); err != nil {
		return err
	}
	return nil
}

type InvalidHeaderError struct {
	notExistHeaders []string
}

func (i *InvalidHeaderError) Error() string {
	return fmt.Sprintf("CSVファイルのヘッダが欠損しています: %s", strings.Join(i.notExistHeaders, ","))
}

func NewInvalidHeaderError(notExistHeaders []string) *InvalidHeaderError {
	return &InvalidHeaderError{notExistHeaders: notExistHeaders}
}

func validateHeader(csv []byte, importStatus *importstatus.ImportStatus, baseRepository *repository.BaseRepository) error {
	scanner := bufio.NewScanner(bytes.NewBuffer(csv))
	for scanner.Scan() {
		unquoted := strings.ReplaceAll(scanner.Text(), "\"", "")
		headers := strings.Split(unquoted, ",")

		notExistHeaders := []string{}

		t := reflect.TypeOf(User{})
	L:
		for i := 0; i < t.NumField(); i++ {
			csvTag := t.Field(i).Tag.Get("csv")
			for _, header := range headers {
				if header == csvTag {
					continue L
				}
			}
			notExistHeaders = append(notExistHeaders, csvTag)
		}
		if len(notExistHeaders) > 0 {
			return NewInvalidHeaderError(notExistHeaders)
		}
		// ヘッダのみでいいので1行読み終わったら抜ける
		break
	}
	return nil
}

func importCSV(csv []byte, importStatus *importstatus.ImportStatus, baseRepository *repository.BaseRepository, importStatusRepository *repository.ImportStatusRepository) error {
	var users []*User
	err := csvutil.Unmarshal(csv, &users)
	if err != nil {
		return err
	}

	importStatus.SetRecordCount(len(users))
	if err := importStatusRepository.Save(importStatus); err != nil {
		return err
	}

	for i, user := range users {
		row := i + 1
		if err := importRow(user, row, importStatus, baseRepository, importStatusRepository); err != nil {
			return err
		}

		importStatus.IncrementProcessedCount()
		if err := importStatusRepository.Save(importStatus); err != nil {
			return err
		}
	}
	return nil
}

func importRow(user *User, row int, importStatus *importstatus.ImportStatus, baseRepository *repository.BaseRepository, importStatusRepository *repository.ImportStatusRepository) error {
	if err := config.ValidateStruct(user); err != nil {
		importStatus.AppendDetail(row, strings.Join(config.GetErrorMessages(err), ","))
		if err := importStatusRepository.Save(importStatus); err != nil {
			return err
		}
		return nil
	}

	// バリデーションOKの場合
	// 環境変数からキューURLを取得
	queueURL := os.Getenv("QUEUE_URL")

	if err := sendMessage(user, queueURL); err != nil {
		return err
	}
	return nil
}

func sendMessage(msg any, queueURL string) error {
	// メッセージをJSON化
	msgJson, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	// SQSに送信 (sqsSvc, queueURLは準備のときに作成したもの)
	if _, err := sqsSvc.SendMessage(&sqs.SendMessageInput{
		MessageBody: aws.String(string(msgJson)),
		QueueUrl:    &queueURL,
	}); err != nil {
		return err
	}
	return nil
}

func convertToUTF8(bytes []byte) ([]byte, error) {
	detector := chardet.NewTextDetector()
	result, err := detector.DetectBest(bytes)
	if err != nil {
		return nil, err
	}
	converted := []byte{}
	s := result.Charset
	switch {
	case s == "Shift_JIS" || s == "windows-1252":
		converted, err = io.ReadAll(transform.NewReader(strings.NewReader(string(bytes)), japanese.ShiftJIS.NewDecoder()))
		if err != nil {
			return nil, err
		}
	case s == "UTF-8" || strings.Contains(s, "ISO-8859"):
		converted = bytes
	default:
		return nil, fmt.Errorf("CSVファイルの文字コードが不正です")
	}
	return converted, nil
}

func main() {
	Init()
	lambda.Start(s3lambda)
}
