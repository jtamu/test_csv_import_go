package service

import (
	"errors"
	"fmt"
	"io"
	"log"
	csvService "my-s3-function-go/app/domain/csv/service"
	userService "my-s3-function-go/app/domain/user/service"
	"my-s3-function-go/app/infrastructure/repository"
	"my-s3-function-go/config"
	"my-s3-function-go/di"
	"os"
	"path/filepath"

	"github.com/jszwec/csvutil"
)

var cfg = config.Cfg

func ProcessEventRecord(inputBaseUrl, inputFilePath string) error {
	diContainer := di.NewDIContainer()

	inputStorage := diContainer.GetStorage(inputBaseUrl)
	obj, err := inputStorage.GetObject(inputFilePath)
	if err != nil {
		return err
	}

	csv, err := io.ReadAll(obj)
	if err != nil {
		return err
	}

	importTarget := filepath.Dir(inputFilePath)
	switch importTarget {
	case "user":
		userQueue := diContainer.GetQueue(os.Getenv(cfg.Queue.UserQueue))
		userService := userService.NewUserService(userQueue)
		if err := importCSV(csv, inputFilePath, userService.ImportUser); err != nil {
			return err
		}
	default:
		log.Fatalf("invalid file path: %s", inputFilePath)
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
