package main

import (
	"my-s3-function-go/app/handler"

	_ "github.com/go-sql-driver/mysql"

	"github.com/aws/aws-lambda-go/lambda"
)

func main() {
	handler.Init()
	lambda.Start(handler.S3lambda)
}
