package config

import (
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"gopkg.in/yaml.v2"
)

var (
	Cfg    Config
	SqsSvc *sqs.SQS
)

type Config struct {
	ImportStatus ImportStatus `yaml:"importStatus"`
}

type ImportStatus struct {
	Pending    string `yaml:"pending"`
	Processing string `yaml:"processing"`
	Finished   string `yaml:"finished"`
	Failed     string `yaml:"failed"`
}

func init() {
	// セッション
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String("ap-northeast-1"),
	}))

	// SQSのクライアントを作成
	SqsSvc = sqs.New(sess)

	f, err := os.Open("./setting.yml")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	if err := yaml.NewDecoder(f).Decode(&Cfg); err != nil {
		log.Fatal(err)
	}

	log.Printf("%+v\n", Cfg)
}
