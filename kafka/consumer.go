package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"main/kafka/internal"
	"time"
)

type Consumer interface {
	ConsumeRecords(func([]byte) error)
	Close() error
}

type consumer struct {
	consumers []internal.Consumer
	poolSize  int
	closeChan chan struct{}
}

func NewConsumer(brokers string, topic, groupId string, poolSize int) *consumer {
	wrapper := internal.New()
	return newKafkaConsumer(brokers, topic, groupId, poolSize, wrapper)
}

func newKafkaConsumer(brokers string, topic, groupId string, poolSize int, wrapper internal.Wrapper) *consumer {
	consumers := make([]internal.Consumer, poolSize)

	for i := 0; i < poolSize; i++ {
		time.Sleep(time.Second * 5)
		consumers[i] = wrapper.NewConsumer(brokers, topic, groupId)
	}

	return &consumer{consumers, poolSize, make(chan struct{})}
}

// ConsumeRecords consumes records from a kafka topic, it receives a function to process the record.
// If the processing function is successful the offset of the message is commited
func (k *consumer) ConsumeRecords(processFunction func([]byte) error) {
	// Read([]byte("mensaje de prueba"))
	for i := 0; i < k.poolSize; i++ {
		go k.workProcessing(i, processFunction)
	}

	<-k.closeChan
}

type Ctx struct {
	Ctx context.Context
}

func (k *consumer) workProcessing(consumerId int, processFunction func([]byte) error) {
	consumer := k.consumers[consumerId]
	for {
		message, err := consumer.ReadMessage(-1)
		var parentCtx context.Context
		json.Unmarshal(message.Headers[0].Value, &parentCtx)

		if err != nil {
			continue
		}
		if err := processFunction(message.Value); err != nil {
			fmt.Println(err)
			continue
		}
		if _, err := consumer.CommitMessage(message); err != nil {
			continue
		}
	}
}

func (k *consumer) Close() error {
	k.closeChan <- struct{}{}
	for _, consumer := range k.consumers {
		err := consumer.Close()
		if err != nil {
			fmt.Println("can't close consumer channel")
		}
	}
	return nil
}
