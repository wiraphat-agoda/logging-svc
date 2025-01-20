package kafka

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

// ConsumerConfig holds the configuration for Kafka consumer
type ConsumerConfig struct {
	Brokers        []string        // Kafka broker addresses
	Topic          string          // Topic to consume
	GroupID        string          // Consumer group ID
	InitialOffset  int64           // Starting offset (sarama.OffsetNewest or sarama.OffsetOldest)
	RetryInterval  time.Duration   // Interval between retries
	MaxRetries     int             // Maximum number of retries
	SecurityConfig *SecurityConfig // Optional security configuration
}

// SecurityConfig holds security-related configurations
type SecurityConfig struct {
	Username   string
	Password   string
	TLSEnabled bool
}

// Consumer represents a Kafka consumer client
type Consumer struct {
	config     *ConsumerConfig
	client     sarama.ConsumerGroup
	logger     *log.Logger
	ctx        context.Context
	cancelFunc context.CancelFunc
	wg         sync.WaitGroup
	msgHandler MessageHandler
}

// MessageHandler is a function type that processes Kafka messages
type MessageHandler func(message *sarama.ConsumerMessage) error

// NewConsumer creates a new Kafka consumer instance
func NewConsumer(config *ConsumerConfig) (*Consumer, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}

	// Create Sarama config
	saramaConfig := sarama.NewConfig()
	saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	saramaConfig.Consumer.Offsets.Initial = config.InitialOffset
	saramaConfig.Consumer.Return.Errors = true

	// Configure security if provided
	if config.SecurityConfig != nil {
		saramaConfig.Net.SASL.Enable = true
		saramaConfig.Net.SASL.User = config.SecurityConfig.Username
		saramaConfig.Net.SASL.Password = config.SecurityConfig.Password
		saramaConfig.Net.TLS.Enable = config.SecurityConfig.TLSEnabled
	}

	// Create consumer group
	client, err := sarama.NewConsumerGroup(config.Brokers, config.GroupID, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer group: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Consumer{
		config:     config,
		client:     client,
		logger:     log.New(os.Stdout, "[kafka-consumer] ", log.LstdFlags),
		ctx:        ctx,
		cancelFunc: cancel,
	}, nil
}

// ConsumerGroupHandler implements sarama.ConsumerGroupHandler interface
type ConsumerGroupHandler struct {
	consumer *Consumer
}

func (h *ConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (h *ConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

// ConsumeClaim handles the consumption of messages from a given partition
func (h *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}

			// Process the message using the handler
			if err := h.consumer.msgHandler(message); err != nil {
				h.consumer.logger.Printf("Error processing message: %v", err)
				// Depending on your requirements, you might want to implement retry logic here
				continue
			}

			// Mark message as processed
			session.MarkMessage(message, "")

		case <-h.consumer.ctx.Done():
			return nil
		}
	}
}

// Start begins consuming messages with the provided message handler
func (c *Consumer) Start(handler MessageHandler) error {
	if handler == nil {
		return fmt.Errorf("message handler cannot be nil")
	}

	c.msgHandler = handler
	c.wg.Add(1)

	go func() {
		defer c.wg.Done()
		for {
			// Check if context was cancelled
			if c.ctx.Err() != nil {
				return
			}

			// Start consuming
			if err := c.client.Consume(c.ctx, []string{c.config.Topic}, &ConsumerGroupHandler{consumer: c}); err != nil {
				c.logger.Printf("Error from consumer: %v", err)

				// Implement retry logic
				select {
				case <-c.ctx.Done():
					return
				case <-time.After(c.config.RetryInterval):
					continue
				}
			}
		}
	}()

	c.logger.Printf("Consumer started. Topic: %s, Group: %s", c.config.Topic, c.config.GroupID)
	return nil
}

// Stop gracefully shuts down the consumer
func (c *Consumer) Stop() error {
	c.logger.Println("Stopping consumer...")
	c.cancelFunc()
	c.wg.Wait()

	if err := c.client.Close(); err != nil {
		return fmt.Errorf("error closing consumer: %w", err)
	}

	c.logger.Println("Consumer stopped successfully")
	return nil
}

// IsHealthy checks if the consumer is healthy
func (c *Consumer) IsHealthy() bool {
	// Implement health check logic
	// For example, check if the consumer group is still active
	return c.client != nil && c.ctx.Err() == nil
}
