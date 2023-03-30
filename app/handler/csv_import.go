package handler

import (
	"context"
	"log"
	"my-s3-function-go/app/service"

	"github.com/aws/aws-lambda-go/events"
)

func S3lambda(ctx context.Context, sqsEvent events.SQSEvent) (interface{}, error) {
	ch := make([]chan error, len(sqsEvent.Records))
	for i := range ch {
		ch[i] = make(chan error)
	}

	for i, record := range sqsEvent.Records {
		// NOTE: 先にループが回ってからgoroutineにスイッチするので、直接recordを渡してしまうと全てのgoroutineに最後のrecordのみが渡されてしまう
		go func(msg events.SQSMessage, chl chan<- error) {
			if err := service.ProcessEventRecord(msg); err != nil {
				chl <- err
				return
			}
			chl <- nil
		}(record, ch[i])
	}

	for i := range ch {
		if err := <-ch[i]; err != nil {
			log.Printf("%+v\n", err)
		}
	}

	resp := &struct {
		StatusCode uint `json:"statusCode"`
	}{StatusCode: 200}
	return resp, nil
}
