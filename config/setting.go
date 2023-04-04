package config

import (
	"log"
	"os"

	"github.com/joho/godotenv"
	"gopkg.in/yaml.v2"
)

var Cfg Config

type Config struct {
	ImportStatus ImportStatus `yaml:"importStatus"`
	DB           DB           `yaml:"db"`
	Queue        Queue        `yaml:"queue"`
}

type ImportStatus struct {
	Pending    string `yaml:"pending"`
	Processing string `yaml:"processing"`
	Finished   string `yaml:"finished"`
	Failed     string `yaml:"failed"`
}

type DB struct {
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Host     string `yaml:"host"`
	Port     string `yaml:"port"`
	DBName   string `yaml:"dbname"`
}

type Queue struct {
	UserQueue string `yaml:"userQueue"`
}

func init() {
	if err := godotenv.Load(".env"); err != nil {
		log.Fatal("cannot load env file.")
	}

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
