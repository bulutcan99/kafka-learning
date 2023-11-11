package message

import (
	"context"
	"github.com/google/uuid"
	"sync"
)

var closedchan = make(chan struct{})

// Payload is the Message's payload.
type Payload []byte

type ackType int

const (
	noAckSent ackType = iota
	ack
	nack
)

// Message is the basic transfer unit.
// Messages are emitted by Publishers and received by Subscribers.
type Message struct {
	// UUID can be empty.
	UUID    string
	Payload string
	// ack is closed, when acknowledge is received.
	ack chan struct{}
	// noACk is closed, when negative acknowledge is received.
	noAck       chan struct{}
	ackMutex    sync.Mutex
	ackSentType ackType
	ctx         context.Context
}

func NewMessage(payload string) *Message {
	return &Message{
		UUID:    uuid.New().String(),
		Payload: payload,
		ack:     make(chan struct{}),
		noAck:   make(chan struct{}),
	}
}

func (m *Message) Ack() bool {
	m.ackMutex.Lock()
	defer m.ackMutex.Unlock()

	if m.ackSentType == nack {
		return false
	}

	if m.ackSentType != noAckSent {
		return true
	}

	m.ackSentType = ack
	if m.ack == nil {
		m.ack = closedchan
	} else {
		close(m.ack)
	}

	return true
}

func (m *Message) Nack() bool {
	m.ackMutex.Lock()
	defer m.ackMutex.Unlock()
	if m.ackSentType == ack {
		return false
	}
	if m.ackSentType != noAckSent {
		return true
	}

	m.ackSentType = nack

	if m.noAck == nil {
		m.noAck = closedchan
	} else {
		close(m.noAck)
	}

	return true
}

// Acked returns channel which is closed when acknowledgement is sent.
//
// Usage:
//
//	select {
//	case <-message.Acked():
//		// ack received
//	case <-message.Nacked():
//		// nack received
//	}
func (m *Message) Acked() <-chan struct{} {
	return m.ack
}

// Nacked returns channel which is closed when negative acknowledgement is sent.
//
// Usage:
//
//	select {
//	case <-message.Acked():
//		// ack received
//	case <-message.Nacked():
//		// nack received
//	}
func (m *Message) Nacked() <-chan struct{} {
	return m.noAck
}
