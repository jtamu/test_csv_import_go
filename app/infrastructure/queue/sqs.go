package queue

import (
	"encoding/json"
	"my-s3-function-go/config"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

var sqsSvc *sqs.SQS

func init() {
	// SQSのクライアントを作成
	sqsSvc = sqs.New(config.AwsSess)
}

type SQS struct {
	url string
}

func NewSQS(url string) *SQS {
	return &SQS{
		url: url,
	}
}

func (s *SQS) SendMessage(message any) error {
	// メッセージをJSON化
	msgJson, err := json.Marshal(message)
	if err != nil {
		return err
	}

	if _, err := sqsSvc.SendMessage(&sqs.SendMessageInput{
		MessageBody: aws.String(string(msgJson)),
		QueueUrl:    &s.url,
	}); err != nil {
		return err
	}
	return nil
}
