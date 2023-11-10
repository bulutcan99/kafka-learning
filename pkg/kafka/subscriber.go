package kafka

import (
	"context"
	"fmt"
	"github.com/IBM/sarama"
	"github/bulutcan99/kafka-pubsub/pkg/message"
	"go.uber.org/zap"
	"sync"
	"time"
)

type Subscriber struct {
	config SubscriberConfig

	closing       chan struct{}
	subscribersWg sync.WaitGroup

	closed bool
}

func NewSubscriber(
	config SubscriberConfig,
) (*Subscriber, error) {
	config.setDefaults()

	if err := config.Validate(); err != nil {
		return nil, err
	}

	return &Subscriber{
		config: config,

		closing: make(chan struct{}),
	}, nil
}

type SubscriberConfig struct {
	// Kafka brokers list.
	Brokers []string

	// Unmarshaler is used to unmarshal messages from Kafka format into Watermill format.
	Unmarshaler UnmarshalerMessage

	// OverwriteSaramaConfig holds additional sarama settings.
	OverwriteSaramaConfig *sarama.Config

	// Kafka consumer group.
	// When empty, all messages from all partitions will be returned.
	ConsumerGroup string

	// How long after Nack message should be redelivered.
	NackResendSleep time.Duration

	// How long about unsuccessful reconnecting next reconnect will occur.
	ReconnectRetrySleep time.Duration

	InitializeTopicDetails *sarama.TopicDetail
}

func (c *SubscriberConfig) setDefaults() {
	if c.OverwriteSaramaConfig == nil {
		c.OverwriteSaramaConfig = DefaultSaramaSubscriberConfig()
	}
	if c.NackResendSleep == 0 {
		c.NackResendSleep = time.Millisecond * 100
	}
	if c.ReconnectRetrySleep == 0 {
		c.ReconnectRetrySleep = time.Second
	}
}

func (c SubscriberConfig) Validate() error {
	if len(c.Brokers) == 0 {
		return fmt.Errorf("missing brokers")
	}
	if c.Unmarshaler == nil {
		return fmt.Errorf("missing unmarshaler")
	}

	return nil
}

func DefaultSaramaSubscriberConfig() *sarama.Config {
	config := sarama.NewConfig()

	config.Version = sarama.V3_6_0_0
	config.Consumer.Return.Errors = true
	config.ClientID = "kafkaClient"

	return config
}

func (s *Subscriber) Subscribe(topic string) (<-chan *message.Message, error) {
	if s.closed {
		return nil, fmt.Errorf("subscriber closed")
	}

	s.subscribersWg.Add(1)
	msgChan := make(chan *message.Message)
	consumeClosed, err := s.consumeMessages(ctx, topic, output)
	if err != nil {
		s.subscribersWg.Done()
		return nil, err
	}

	go func() {
		// blocking, until s.closing is closed
		s.handleReconnects(ctx, topic, output, consumeClosed)
		close(msgChan)
		s.subscribersWg.Done()
	}()

	return msgChan, nil
}

func (s *Subscriber) consumeMessages(ctx context.Context, topic string, output chan *message.Message) (consumeMsgClosing chan struct{}, err error) {
	zap.S().Info("Consuming messages from kafka")
	client, err := sarama.NewClient(s.config.Brokers, s.config.OverwriteSaramaConfig)
	if err != nil {
		return nil, fmt.Errorf("cannot create kafka client: %w", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-s.closing:
			zap.S().Info("Closing kafka client")
			cancel()
		case <-ctx.Done():
			return
		}
	}()

	if s.config.ConsumerGroup == "" {
		consumeMsgClosing, err = s.consumeWithoutConsumerGroups(ctx, client, topic, output)
	} else {
		consumeMsgClosing, err = s.consumeGroupMessages(ctx, client, topic, output)
	}
	if err != nil {
		zap.S().Errorf("Error consuming messages from kafka: %s", err)
		cancel()
		return nil, err
	}

	go func() {
		<-consumeMsgClosing
		if err := client.Close(); err != nil {
			zap.S().Errorf("Cannot close client: %s", err)
		} else {
			zap.S().Error("Client closed")
		}
	}()

	return consumeMessagesClosed, nil
}
