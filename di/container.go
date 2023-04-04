package di

import (
	"my-s3-function-go/app/domain/queue"
	"my-s3-function-go/app/domain/storage"
	infra_queue "my-s3-function-go/app/infrastructure/queue"
	infra_storage "my-s3-function-go/app/infrastructure/storage"
	"os"
)

type DIContainer interface {
	GetStorage(arg string) storage.Storage
	GetQueue(arg string) queue.Queue
}

func NewDIContainer() DIContainer {
	if os.Getenv("env") != "local" {
		return &ProductionContainer{}
	}
	return nil
}

type ProductionContainer struct{}

func (p *ProductionContainer) GetStorage(arg string) storage.Storage {
	return infra_storage.NewS3Storage(arg)
}

func (p *ProductionContainer) GetQueue(arg string) queue.Queue {
	return infra_queue.NewSQS(arg)
}
