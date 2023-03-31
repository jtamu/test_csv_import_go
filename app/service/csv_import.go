package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	csvService "my-s3-function-go/app/domain/csv/service"
	userService "my-s3-function-go/app/domain/user/service"
	"my-s3-function-go/app/infrastructure/repository"
	"my-s3-function-go/app/infrastructure/storage"
	"my-s3-function-go/config"
	"my-s3-function-go/di"

	"github.com/aws/aws-lambda-go/events"
	"github.com/jszwec/csvutil"
)

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

	userService := userService.NewUserService(di.DIObj.GetUserQueue())
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

	csv, err = csvService.ConvertToUTF8(csv)
	if err != nil {
		var invalidFileFormatError *csvService.InvalidFileFormatError
		if errors.As(err, &invalidFileFormatError) {
			importStatus.Failed(err)
			if err := importStatusRepository.Save(importStatus); err != nil {
				return err
			}
		}
		return err
	}

	if err := csvService.ValidateHeader[T](csv, importStatus); err != nil {
		var invalidHeaderError *csvService.InvalidHeaderError
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

func importRow[T any](model *T, importFunc func(*T) error) error {
	validate := config.NewValidate()
	if err := validate.Struct(model); err != nil {
		return err
	}

	if err := importFunc(model); err != nil {
		return err
	}
	return nil
}
