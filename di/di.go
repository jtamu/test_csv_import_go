package di

import (
	"my-s3-function-go/app/domain/queue"
	infra_queue "my-s3-function-go/app/infrastructure/queue"
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
