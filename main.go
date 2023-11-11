package main

import (
	"github.com/IBM/sarama"
	"github/bulutcan99/kafka-pubsub/pkg/kafka"
	"github/bulutcan99/kafka-pubsub/pkg/message"
	"go.uber.org/zap"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	publisherConfig := kafka.PublisherConfig{
		Brokers: []string{"your-kafka-broker-address"},
	}
	publisher, err := kafka.NewPublisher(publisherConfig)
	if err != nil {
		zap.S().Fatalw("Failed to create Kafka publisher", "error", err)
	}

	subscriberConfig := kafka.SubscriberConfig{
		Brokers: []string{"your-kafka-broker-address"},
	}
	subscriber, err := kafka.NewSubscriber(subscriberConfig)
	if err != nil {
		zap.S().Fatalw("Failed to create Kafka subscriber", "error", err)
	}

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)

	err = subscriber.Subscribe("message-topic", func(msg *sarama.ConsumerMessage, userUUID string) {
		subscriber.HandleMessage(msg, userUUID)
	})
	if err != nil {
		zap.S().Fatalw("Failed to subscribe to Kafka topic", "error", err)
	}

	// Simulate sending a message every 5 seconds
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Create a sample message
			sampleMessage := &message.Message{
				Payload: "Hello, Kafka!",
			}

			// Publish the message
			err := publisher.Publish("message-topic", sampleMessage)
			if err != nil {
				zap.S().Errorw("Failed to publish message to Kafka", "error", err)
			}
		case <-interrupt:
			// Handle SIGINT (Ctrl+C) signal, close resources, and exit
			zap.S().Infow("Received interrupt signal, closing resources...")
			err := publisher.Close()
			if err != nil {
				zap.S().Errorw("Failed to close Kafka publisher", "error", err)
			}

			err = subscriber.Close()
			if err != nil {
				zap.S().Errorw("Failed to close Kafka subscriber", "error", err)
			}

			zap.S().Infow("Exiting...")
			return
		}
	}
}
