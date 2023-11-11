package kafka

import (
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/pkg/errors"
	"github/bulutcan99/kafka-pubsub/pkg/message"
	"go.uber.org/zap"
	"time"
)

type Publisher struct {
	config   PublisherConfig
	producer sarama.SyncProducer
	closed   chan struct{}
}

func NewPublisher(
	config PublisherConfig,
) (*Publisher, error) {
	config.setDefaults()
	if err := config.Validate(); err != nil {
		return nil, err
	}

	producer, err := sarama.NewSyncProducer(config.Brokers, config.OverwriteSaramaConfig)
	if err != nil {
		return nil, fmt.Errorf("cannot create Kafka producer: %w", err)
	}

	return &Publisher{
		config:   config,
		producer: producer,
		closed:   make(chan struct{}),
	}, nil
}

type PublisherConfig struct {
	Brokers []string

	// OverwriteSaramaConfig holds additional sarama settings.
	OverwriteSaramaConfig *sarama.Config
}

func (c *PublisherConfig) setDefaults() {
	if c.OverwriteSaramaConfig == nil {
		c.OverwriteSaramaConfig = DefaultSaramaSyncPublisherConfig()
	}
}

func (c PublisherConfig) Validate() error {
	if len(c.Brokers) == 0 {
		return fmt.Errorf("missing brokers")
	}

	return nil
}

func DefaultSaramaSyncPublisherConfig() *sarama.Config {
	config := sarama.NewConfig()

	config.Version = sarama.V3_6_0_0
	config.Producer.Retry.Max = 10
	config.Producer.Return.Successes = true
	config.Metadata.Retry.Backoff = time.Second * 2
	config.Producer.Compression = sarama.CompressionSnappy
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Flush.Frequency = 100 * time.Millisecond
	config.ClientID = "kafkaClient"

	return config
}

func (p *Publisher) Publish(topic string, messages ...*message.Message) error {
	if p.closed == nil {
		return errors.New("cannot publish on closed publisher")
	}

	producerMessages := make([]*sarama.ProducerMessage, len(messages))
	for i, msg := range messages {
		zap.S().Info("Publishing message to kafka")

		payloadBytes, err := json.Marshal(msg.Payload)
		if err != nil {
			return err
		}

		producerMsg := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(payloadBytes),
		}

		producerMessages[i] = producerMsg
		_, _, err = p.producer.SendMessage(producerMsg)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *Publisher) Close() error {
	if p.closed == nil {
		return nil
	}

	close(p.closed)
	if err := p.producer.Close(); err != nil {
		return errors.Wrap(err, "cannot close Kafka producer")
	}

	return nil
}
