package service

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"my-s3-function-go/app/domain/importstatus"
	userService "my-s3-function-go/app/domain/user/service"
	"my-s3-function-go/app/infrastructure/queue"
	"my-s3-function-go/app/infrastructure/repository"
	"my-s3-function-go/app/infrastructure/storage"
	"my-s3-function-go/config"
	"os"
	"reflect"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/jszwec/csvutil"
	"github.com/saintfish/chardet"
	"golang.org/x/text/encoding/japanese"
	"golang.org/x/text/transform"
	"gopkg.in/go-playground/validator.v9"
)

var validate *validator.Validate

func Init() {
	userRepository := repository.NewUserRepository()
	emails, err := userRepository.GetAllEmails()
	if err != nil {
		log.Printf("%+v\n", err)
		return
	}
	validate = config.InitValidator(emails)
}

func ProcessEventRecord(record events.SQSMessage) error {
	b := []byte(record.Body)
	s3Object := events.S3EventRecord{}
	if err := json.Unmarshal(b, &s3Object); err != nil {
		return err
	}
	log.Printf("%+v\n", s3Object)

	bucket := s3Object.S3.Bucket.Name
	key := s3Object.S3.Object.Key

	s3storage := storage.NewS3Storage(bucket)
	obj, err := s3storage.GetObject(key)
	if err != nil {
		return err
	}

	csv, err := io.ReadAll(obj)
	if err != nil {
		return err
	}

	userService := userService.NewUserService(queue.NewSQS(os.Getenv("QUEUE_URL")))
	if err := importCSV(csv, key, userService.ImportUser); err != nil {
		return err
	}
	return nil
}

func importCSV[T any](csv []byte, file_path string, importFunc func(*T) error) error {
	importStatusRepository := repository.NewImportStatusRepository()

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
		if err := importRow(model, importFunc); err != nil {
			var validationError *config.ValidationError
			if errors.As(err, &validationError) {
				importStatus.AppendDetail(row, err.Error())
				if err := importStatusRepository.Save(importStatus); err != nil {
					return err
				}
			} else {
				return err
			}
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

func importRow[T any](model *T, importFunc func(*T) error) error {
	if err := validate.Struct(model); err != nil {
		return config.NewValidationError(strings.Join(config.GetErrorMessages(err), ","))
	}

	if err := importFunc(model); err != nil {
		return err
	}
	return nil
}
