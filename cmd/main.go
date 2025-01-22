package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	cfg "github.com/wiraphat-agoda/logging-svc/pkg/config"
	"github.com/wiraphat-agoda/logging-svc/pkg/kafka"
	"github.com/wiraphat-agoda/logging-svc/pkg/logger"
)

// LogMessage structure for product API logs
type LogMessage struct {
	Timestamp  time.Time `json:"timestamp"`
	Level      string    `json:"level"`
	Service    string    `json:"service"`
	Message    string    `json:"message"`
	RequestID  string    `json:"requestId,omitempty"`
	Method     string    `json:"method,omitempty"`
	Path       string    `json:"path,omitempty"`
	StatusCode int       `json:"statusCode,omitempty"`
	Error      string    `json:"error,omitempty"`
	Detail     string    `json:"detail,omitempty"`
}

func main() {
	cfg.Load("./.env")

	logger := logger.New()
	sugar := logger.Sugar()

	// Create consumer config
	config := &kafka.ConsumerConfig{
		Brokers:       []string{"localhost:9092"},
		Topic:         "product-logs", // topic สำหรับรับ log จาก product API
		GroupID:       "logging-service-group",
		InitialOffset: sarama.OffsetNewest,
		RetryInterval: 5 * time.Second,
		MaxRetries:    3,
	}

	// Create consumer
	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		sugar.Fatalw("Error creating consumer",
			"error", err,
		)
	}

	// Define message handler for processing logs
	handler := func(msg *sarama.ConsumerMessage) error {
		var logMsg LogMessage
		if err := json.Unmarshal(msg.Value, &logMsg); err != nil {
			sugar.Errorw("Failed to unmarshal log message",
				"error", err,
				"message", string(msg.Value),
			)
			return err
		}

		// Log message with appropriate level
		switch logMsg.Level {
		case "error":
			sugar.Errorw(logMsg.Message,
				"service", logMsg.Service,
				"request_id", logMsg.RequestID,
				"method", logMsg.Method,
				"path", logMsg.Path,
				"status_code", logMsg.StatusCode,
				"error", logMsg.Error,
			)
		case "warn":
			sugar.Warnw(logMsg.Message,
				"service", logMsg.Service,
				"request_id", logMsg.RequestID,
				"method", logMsg.Method,
				"path", logMsg.Path,
				"status_code", logMsg.StatusCode,
			)
		default:
			sugar.Infow(logMsg.Message,
				"service", logMsg.Service,
				"request_id", logMsg.RequestID,
				"method", logMsg.Method,
				"path", logMsg.Path,
				"status_code", logMsg.StatusCode,
				"detail", logMsg.Detail,
			)
		}

		return nil
	}

	// Start consuming
	sugar.Infow(fmt.Sprintf("Starting %v", cfg.SERVICE_NAME),
		"broker", config.Brokers[0],
		"topic", config.Topic,
		"group", config.GroupID,
	)

	if err := consumer.Start(handler); err != nil {
		sugar.Fatalw("Error starting consumer",
			"error", err,
		)
	}

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	// Gracefully stop the consumer
	if err := consumer.Stop(); err != nil {
		sugar.Errorw("Error stopping consumer",
			"error", err,
		)
	}

	sugar.Infof("Logging service stopped")
}
