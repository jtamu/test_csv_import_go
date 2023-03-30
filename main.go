package main

import (
	"my-s3-function-go/app/adapter"

	_ "github.com/go-sql-driver/mysql"

	"github.com/aws/aws-lambda-go/lambda"
)

func main() {
	adapter.Init()
	lambda.Start(adapter.S3lambda)
}
