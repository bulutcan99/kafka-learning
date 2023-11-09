package kafka

import (
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
	closed   bool
}

func NewPublisher(
	config PublisherConfig,
	logger zap.Logger,
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
	}, nil
}

type PublisherConfig struct {
	// Kafka brokers list.
	Brokers []string

	// Marshaler is used to marshal messages from Watermill format into Kafka format.
	Marshaler MarshalerMessage

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
	if c.Marshaler == nil {
		return fmt.Errorf("missing marshaler")
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

func (p *Publisher) Close() error {
	if p.closed {
		return nil
	}
	p.closed = true

	if err := p.producer.Close(); err != nil {
		return errors.Wrap(err, "cannot close Kafka producer")
	}

	return nil
}

func (p *Publisher) Publish(topic string, messages ...*message.Message) error {
	if p.closed {
		return errors.New("cannot publish on closed publisher")
	}

	producerMessages := make([]*sarama.ProducerMessage, len(messages))
	for i, msg := range messages {
		zap.S().Info("Publishing message to kafka")
		producerMsg, err := p.config.Marshaler.Marshal(topic, msg)
		if err != nil {
			return err
		}

		producerMessages[i] = producerMsg
	}

	for _, msg := range producerMessages {
		_, _, err := p.producer.SendMessage(msg)
		if err != nil {
			return err
		}
	}

	return nil
}
