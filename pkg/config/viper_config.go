package config

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

var (
	SERVICE_ENV  string
	SERVICE_NAME string
)

func Load(path string) {
	if err := godotenv.Load(path); err != nil {
		log.Fatalf("Error loading %v file: %v", path, err)
	}

	SERVICE_ENV = os.Getenv("SERVICE_ENV")
	SERVICE_NAME = os.Getenv("SERVICE_NAME")
}
