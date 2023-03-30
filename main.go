package main

import (
	"my-s3-function-go/app/handler"
	"my-s3-function-go/app/service"

	_ "github.com/go-sql-driver/mysql"

	"github.com/aws/aws-lambda-go/lambda"
)

func main() {
	service.Init()
	lambda.Start(handler.S3lambda)
}
