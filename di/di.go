package di

import (
	"my-s3-function-go/app/domain/queue"
	"my-s3-function-go/app/domain/storage"
	infra_queue "my-s3-function-go/app/infrastructure/queue"
	infra_storage "my-s3-function-go/app/infrastructure/storage"
	"os"
)

type DI struct {
	userQueue queue.Queue
}

var DIObj *DI

func init() {
	if os.Getenv("env") != "local" {
		DIObj = &DI{
			userQueue: infra_queue.NewSQS(os.Getenv("QUEUE_URL")),
		}
	}
}

func (di *DI) GetUserQueue() queue.Queue {
	return di.userQueue
}

func (di *DI) GetInputStorage(arg string) storage.Storage {
	if os.Getenv("env") != "local" {
		return infra_storage.NewS3Storage(arg)
	}
	return nil
}
