package storage

import (
	"io"
	"my-s3-function-go/config"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

var s3Svc *s3.S3

func init() {
	// S3クライアント
	s3Svc = s3.New(config.AwsSess)
}

type S3Storage struct {
	svc    *s3.S3
	bucket string
}

func NewS3Storage(bucket string) *S3Storage {
	return &S3Storage{
		svc:    s3Svc,
		bucket: bucket,
	}
}

func (s *S3Storage) GetObject(key string) (io.Reader, error) {
	obj, err := s.svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	return obj.Body, nil
}
