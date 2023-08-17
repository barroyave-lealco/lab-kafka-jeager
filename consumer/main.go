package main

import (
	"context"
	"fmt"
	"main/jeager"
	kafkaPkg "main/kafka"

	"github.com/opentracing/opentracing-go"
)

func main() {
	brokers := "localhost:9093,localhost:9092,localhost:9091"
	_, closer, err := jeager.InitJaeger("http://localhost:14268/api/traces")
	if err != nil {
		fmt.Println(err)
	}
	defer closer.Close()

	handleProducerMessage(brokers)
	handleConsumerMessage(brokers)
	for {

	}
}

func handleProducerMessage(brokers string) {
	fmt.Println("Iniciando produtor")
	parentSpan := opentracing.GlobalTracer().StartSpan("processing-kafka-message")
	defer parentSpan.Finish()

	parentSpan.SetTag("send", "true")
	ctx := opentracing.ContextWithSpan(context.Background(), parentSpan)

	producer := kafkaPkg.NewProducer(brokers)

	sendMessageToKafka(ctx, producer, "topico1", []byte("Mensaje de prueba"))

}
func handleConsumerMessage(brokers string) {
	fmt.Println("Iniciando consumidor")
	consumer := kafkaPkg.NewConsumer(brokers, "topico1", "topico1", 1)
	defer consumer.Close()

	go consumer.ConsumeRecords(Read)
}

func Read(message []byte) error {
	fmt.Println("Leyendo mensaje")

	headers := map[string]string{
		"key1": "value1",
	}
	parentSpanContext, _ := opentracing.GlobalTracer().Extract(
		opentracing.TextMap,
		opentracing.TextMapCarrier(headers),
	)
	childSpan := opentracing.StartSpan(
		"processing-kafka-message",
		opentracing.ChildOf(parentSpanContext),
	)

	defer childSpan.Finish()
	return nil
}

func sendMessageToKafka(ctx context.Context, producer kafkaPkg.Producer, topic string, messageBody []byte) error {
	// Crear un nuevo span asociado con el contexto

	headers := map[string]string{
		"key1": "value1",
	}
	span, _ := opentracing.StartSpanFromContext(ctx, "sending-to-kafka")
	defer span.Finish()

	// Inyectar el contexto trazado en los headers del mensaje
	err := opentracing.GlobalTracer().Inject(
		span.Context(),
		opentracing.TextMap,
		opentracing.TextMapCarrier(headers),
	)
	if err != nil {
		return err
	}

	fmt.Println("Enviando mansaje")

	// producer.SendMessage(topic, "id", messageBody)
	return nil
}
