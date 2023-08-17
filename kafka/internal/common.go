package internal

import (
	"fmt"
	"math"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Consumer interface {
	ReadMessage(timeout time.Duration) (*kafka.Message, error)
	CommitMessage(m *kafka.Message) ([]kafka.TopicPartition, error)
	Close() error
}

type Producer interface {
	Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error
	Close()
}

type Wrapper interface {
	NewConsumer(string, string, string) Consumer
	NewProducer(string) Producer
}

type kafkaWrapper struct {
	credentials kafkaCredentials
}

func New() *kafkaWrapper {
	credentials := getKafkaCredentials()
	return &kafkaWrapper{credentials}
}

func (k *kafkaWrapper) NewConsumer(brokers string, topic, groupId string) Consumer {
	consumer, e := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":               brokers,
		"group.id":                        groupId,
		"go.application.rebalance.enable": true,
		"isolation.level":                 "read_committed",
		"enable.auto.commit":              false,
	})

	if e != nil {
		panic(e)
	}

	if err := consumer.Subscribe(topic, nil); err != nil {
		panic(err)
	}
	return consumer
}

func (k *kafkaWrapper) NewProducer(brokers string) Producer {
	producer, e := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":   brokers,
		"acks":                "all",
		"delivery.timeout.ms": 120 * 1000,
		"max.in.flight":       5,
		"retries":             math.MaxInt32,
		"enable.idempotence":  true,
	})
	if e != nil {
		fmt.Println("error creating kafka producer")
		panic(e)
	}

	return producer
}

type kafkaCredentials struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

func getKafkaCredentials() kafkaCredentials {

	credentials := kafkaCredentials{}

	return credentials
}
