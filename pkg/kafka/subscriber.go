package kafka

import (
	"fmt"
	"github.com/IBM/sarama"
	"github.com/pkg/errors"
	"sync"
	"time"
)

type Subscriber struct {
	consumer      sarama.Consumer
	config        SubscriberConfig
	subscribersWg sync.WaitGroup
	closed        chan struct{}
}

func NewSubscriber(config SubscriberConfig) (*Subscriber, error) {
	config.setDefaults()

	if err := config.Validate(); err != nil {
		return nil, err
	}

	consumer, err := sarama.NewConsumer(config.Brokers, nil)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create Kafka consumer")
	}

	return &Subscriber{
		config:   config,
		consumer: consumer,
	}, nil
}

type SubscriberConfig struct {
	Brokers                []string
	OverwriteSaramaConfig  *sarama.Config
	NackResendSleep        time.Duration
	ReconnectRetrySleep    time.Duration
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

	return nil
}

func DefaultSaramaSubscriberConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Version = sarama.V3_6_0_0
	config.Consumer.Return.Errors = true
	config.ClientID = "kafkaClient"
	return config
}

func (s *Subscriber) Subscribe(topic string, handler func(msg *sarama.ConsumerMessage, userUUID string)) error {
	if s.closed == nil {
		return errors.New("cannot subscribe on closed subscriber")
	}

	userUUid := "user-uuid"
	partitions, err := s.consumer.Partitions(topic)
	if err != nil {
		return errors.Wrap(err, "cannot retrieve partitions for topic")
	}

	for _, partition := range partitions {
		pc, err := s.consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
		if err != nil {
			return errors.Wrap(err, "cannot consume partition")
		}

		go func(pc sarama.PartitionConsumer) {
			defer pc.Close()
			for {
				select {
				case msg := <-pc.Messages():
					handler(msg, userUUid)
				case <-s.closed:
					return
				}
			}
		}(pc)
	}

	return nil
}

func (s *Subscriber) HandleMessage(msg *sarama.ConsumerMessage, userUUID string) {
	payload := string(msg.Value)
	payloadWithUUID := fmt.Sprintf("%s - User UUID: %s", payload, userUUID)
	fmt.Printf("Received message: %s\n", payloadWithUUID)
}

func (s *Subscriber) Close() error {
	if s.closed == nil {
		return nil
	}
	close(s.closed)

	if err := s.consumer.Close(); err != nil {
		return errors.Wrap(err, "cannot close Kafka consumer")
	}

	return nil
}
