package kafka

import (
	"fmt"
	"main/kafka/internal"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Producer interface {
	SendMessage(topic, key string, value []byte) error
	Close()
}

type producer struct {
	producer internal.Producer
}

func NewProducer(brokers string) *producer {
	wrapper := internal.New()
	return newKafkaProducer(brokers, wrapper)
}

func newKafkaProducer(brokers string, wrapper internal.Wrapper) *producer {
	return &producer{
		producer: wrapper.NewProducer(brokers),
	}
}

func (p *producer) SendMessage(topic, key string, value []byte) error {
	delivery_chan := make(chan kafka.Event, 1)
	err := p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic: &topic,
		},
		Key:   []byte(key),
		Value: value,
	},
		delivery_chan,
	)
	if err != nil {
		return err
	}
	e := <-delivery_chan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		fmt.Println(m.TopicPartition.Error)
	}
	return m.TopicPartition.Error
}

func (p *producer) Close() {
	p.producer.Close()
}
