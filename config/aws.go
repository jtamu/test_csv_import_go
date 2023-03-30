package config

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

var (
	AwsSess *session.Session
	SqsSvc  *sqs.SQS
)

func init() {
	// セッション
	AwsSess = session.Must(session.NewSession(&aws.Config{
		Region: aws.String("ap-northeast-1"),
	}))

	// SQSのクライアントを作成
	SqsSvc = sqs.New(AwsSess)
}
