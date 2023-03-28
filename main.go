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
	"github.com/saintfish/chardet"
	"golang.org/x/text/encoding/japanese"
	"golang.org/x/text/transform"
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
	Email string `csv:"email" jaFieldName:"メールアドレス" validate:"required,email-unique"`
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
	RowNumber      *int
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

	validate.RegisterValidation("email-unique", validateUniquenessOfEmail)
	validate.RegisterTranslation("email-unique", trans, func(ut ut.Translator) error {
		trans.Add("email-unique", "{0}が重複しています", false)
		return nil
	}, func(ut ut.Translator, fe validator.FieldError) string {
		msg, _ := trans.T(fe.Tag(), fe.Field())
		return msg
	})
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
		// NOTE: 先にループが回ってからgoroutineにスイッチするので、直接recordを渡してしまうと全てのgoroutineに最後のrecordのみが渡されてしまう
		go func(msg events.SQSMessage, chl chan<- error) {
			if err := processEventRecord(msg, baseRepository); err != nil {
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

func processEventRecord(record events.SQSMessage, baseRepository *BaseRepository) error {
	b := []byte(record.Body)
	s3Object := events.S3EventRecord{}
	if err := json.Unmarshal(b, &s3Object); err != nil {
		return err
	}
	log.Printf("%+v\n", s3Object)

	bucket := s3Object.S3.Bucket.Name
	key := s3Object.S3.Object.Key

	var importStatus ImportStatus
	if err := db.Where("file_path = ?", key).First(&importStatus).Error; err != nil {
		return err
	}
	importStatus.Status = PROCESSING
	if err := baseRepository.Save(&importStatus); err != nil {
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
		// TODO トランザクション
		importStatus.Status = FAILED
		if err := baseRepository.Save(&importStatus); err != nil {
			return err
		}
		importDetail := ImportDetail{
			ImportStatusID: importStatus.ID,
			RowNumber:      nil,
			Detail:         err.Error(),
		}
		if err := baseRepository.Save(&importDetail); err != nil {
			return err
		}
		return err
	}

	if err := importCSV(csv, &importStatus, baseRepository); err != nil {
		return err
	}

	importStatus.Status = FINISHED
	if err := baseRepository.Save(&importStatus); err != nil {
		return err
	}
	return nil
}

func importCSV(csv []byte, importStatus *ImportStatus, baseRepository *BaseRepository) error {
	var users []*User
	err := csvutil.Unmarshal(csv, &users)
	if err != nil {
		return err
	}

	importStatus.RecordCount = len(users)
	if err := baseRepository.Save(&importStatus); err != nil {
		return err
	}

	for i, user := range users {
		row := i + 1
		if err := importRow(user, row, importStatus, baseRepository); err != nil {
			return err
		}

		importStatus.ProcessedCount = row
		if err := baseRepository.Save(&importStatus); err != nil {
			return err
		}
	}
	return nil
}

func importRow(user *User, row int, importStatus *ImportStatus, baseRepository *BaseRepository) error {
	if err := validate.Struct(user); err != nil {
		importDetail := ImportDetail{
			ImportStatusID: importStatus.ID,
			RowNumber:      &row,
			Detail:         strings.Join(GetErrorMessages(err), ","),
		}
		if err := baseRepository.Save(&importDetail); err != nil {
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

func validateUniquenessOfEmail(fl validator.FieldLevel) bool {
	emails := []string{}
	if err := db.Model(&User{}).Pluck("email", &emails).Error; err != nil {
		panic(err)
	}
	for _, email := range emails {
		if fl.Field().String() == email {
			return false
		}
	}
	return true
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
	switch result.Charset {
	case "Shift_JIS", "windows-1252":
		converted, err = io.ReadAll(transform.NewReader(strings.NewReader(string(bytes)), japanese.ShiftJIS.NewDecoder()))
		if err != nil {
			return nil, err
		}
	case "UTF-8":
		converted = bytes
	default:
		return nil, fmt.Errorf("CSVファイルの文字コードが不正です")
	}
	return converted, nil
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
