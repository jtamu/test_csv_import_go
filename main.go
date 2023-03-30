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
	userService "my-s3-function-go/app/domain/user/service"
	"my-s3-function-go/app/infrastructure/repository"
	"my-s3-function-go/config"

	_ "github.com/go-sql-driver/mysql"
	"github.com/saintfish/chardet"
	"golang.org/x/text/encoding/japanese"
	"golang.org/x/text/transform"
	"gopkg.in/go-playground/validator.v9"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jszwec/csvutil"
)

var (
	sess           *session.Session
	svc            *s3.S3
	db             *gorm.DB
	baseRepository *repository.BaseRepository
	validate       *validator.Validate
)

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

	userRepository := repository.NewUserRepository(baseRepository)
	emails, err := userRepository.GetAllEmails()
	if err != nil {
		log.Printf("%+v\n", err)
		return
	}
	validate = config.InitValidator(emails)
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
			if err := processEventRecord(msg, importStatusRepository); err != nil {
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

func processEventRecord(record events.SQSMessage, importStatusRepository *repository.ImportStatusRepository) error {
	b := []byte(record.Body)
	s3Object := events.S3EventRecord{}
	if err := json.Unmarshal(b, &s3Object); err != nil {
		return err
	}
	log.Printf("%+v\n", s3Object)

	bucket := s3Object.S3.Bucket.Name
	key := s3Object.S3.Object.Key

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

	if err := importCSV(csv, key, userService.ImportUser, importStatusRepository); err != nil {
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

func validateHeader[T any](csv []byte, importStatus *importstatus.ImportStatus) error {
	scanner := bufio.NewScanner(bytes.NewBuffer(csv))
	for scanner.Scan() {
		unquoted := strings.ReplaceAll(scanner.Text(), "\"", "")
		headers := strings.Split(unquoted, ",")

		notExistHeaders := []string{}

		model := new(T)
		t := reflect.TypeOf(*model)
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

func importCSV[T any](csv []byte, file_path string, importFunc func(*T) error, importStatusRepository *repository.ImportStatusRepository) error {
	importStatus, err := importStatusRepository.GetOneByFilePath(file_path)
	if err != nil {
		return err
	}
	if err := importStatus.ShouldBePending(); err != nil {
		return fmt.Errorf("%s。取り込み処理を終了します", err.Error())
	}

	importStatus.Processing()
	if err := importStatusRepository.Save(importStatus); err != nil {
		return err
	}

	csv, err = convertToUTF8(csv)
	if err != nil {
		var invalidFileFormatError *InvalidFileFormatError
		if errors.As(err, &invalidFileFormatError) {
			importStatus.Failed(err)
			if err := importStatusRepository.Save(importStatus); err != nil {
				return err
			}
		}
		return err
	}

	if err := validateHeader[T](csv, importStatus); err != nil {
		var invalidHeaderError *InvalidHeaderError
		if errors.As(err, &invalidHeaderError) {
			importStatus.Failed(err)
			if err := importStatusRepository.Save(importStatus); err != nil {
				return err
			}
		}
		return err
	}

	models := []*T{}
	if err := csvutil.Unmarshal(csv, &models); err != nil {
		return err
	}

	importStatus.SetRecordCount(len(models))
	if err := importStatusRepository.Save(importStatus); err != nil {
		return err
	}

	for i, model := range models {
		row := i + 1
		if err := importRow(model, importFunc, row, importStatus, importStatusRepository); err != nil {
			return err
		}

		importStatus.IncrementProcessedCount()
		if err := importStatusRepository.Save(importStatus); err != nil {
			return err
		}
	}

	importStatus.Finished()
	if err := importStatusRepository.Save(importStatus); err != nil {
		return err
	}
	return nil
}

func importRow[T any](model *T, importFunc func(*T) error, row int, importStatus *importstatus.ImportStatus, importStatusRepository *repository.ImportStatusRepository) error {
	if err := validate.Struct(model); err != nil {
		importStatus.AppendDetail(row, strings.Join(config.GetErrorMessages(err), ","))
		if err := importStatusRepository.Save(importStatus); err != nil {
			return err
		}
		return nil
	}

	if err := importFunc(model); err != nil {
		return err
	}
	return nil
}

type InvalidFileFormatError struct {
	msg string
}

func (i *InvalidFileFormatError) Error() string {
	return i.msg
}

func NewInvalidFileFormatError(msg string) *InvalidFileFormatError {
	return &InvalidFileFormatError{msg: msg}
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
		return nil, NewInvalidFileFormatError("CSVファイルの文字コードが不正です")
	}
	return converted, nil
}

func main() {
	Init()
	lambda.Start(s3lambda)
}
