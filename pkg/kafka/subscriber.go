package kafka

import (
	"fmt"
	"github.com/IBM/sarama"
	"github/bulutcan99/kafka-pubsub/pkg/message"
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

func (s *Subscriber) Subscribe(topic string) (<-chan *sarama.ConsumerMessage, error) {
	if s.closed {
		return nil, fmt.Errorf("subscriber closed")
	}

	s.subscribersWg.Add(1)
	msgChan := make(chan *message.Message)
	consumeClosed, err := s.consumeMessages(ctx, topic, output, logFields)
	if err != nil {
		s.subscribersWg.Done()
		return nil, err
	}

	go func() {
		// blocking, until s.closing is closed
		s.handleReconnects(ctx, topic, output, consumeClosed, logFields)
		close(output)
		s.subscribersWg.Done()
	}()

	return subscriber.messages, nil
}
