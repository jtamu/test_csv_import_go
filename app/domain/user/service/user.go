package service

import (
	"encoding/json"
	"my-s3-function-go/app/domain/user"
	"my-s3-function-go/config"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func ImportUser(user *user.User) error {
	// 環境変数からキューURLを取得
	queueURL := os.Getenv("QUEUE_URL")

	if err := sendMessage(user, queueURL); err != nil {
		return err
	}
	return nil
}

func sendMessage(msg any, queueURL string) error {
	// メッセージをJSON化
	msgJson, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	// SQSに送信 (sqsSvc, queueURLは準備のときに作成したもの)
	if _, err := config.SqsSvc.SendMessage(&sqs.SendMessageInput{
		MessageBody: aws.String(string(msgJson)),
		QueueUrl:    &queueURL,
	}); err != nil {
		return err
	}
	return nil
}
