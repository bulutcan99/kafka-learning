package kafka

import (
	"github.com/IBM/sarama"
	"github/bulutcan99/kafka-pubsub/pkg/message"
)

const UUIDHeader = "uuid"

type MarshalerMessage interface {
	Marshal(topic string, msg *message.Message) (*sarama.ProducerMessage, error)
}

type UnmarshalerMessage interface {
	Unmarshal(*sarama.ConsumerMessage) (*message.Message, error)
}
