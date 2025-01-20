package logger

import (
	cfg "github.com/wiraphat-agoda/logging-svc/pkg/config"
	"go.uber.org/zap"
)

const (
	DEV  = "development"
	PROD = "production"
)

func New() *zap.Logger {
	return newFactory(cfg.SERVICE_ENV)
}

func newFactory(env string) *zap.Logger {
	switch env {
	case DEV:
		return zap.Must(zap.NewDevelopment())
	case PROD:
		return zap.Must(zap.NewProduction())
	default:
		return nil
	}
}
