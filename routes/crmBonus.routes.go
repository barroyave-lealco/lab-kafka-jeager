package routes

import (
	"context"
	"fmt"

	kafkaPkg "main/kafka"

	"github.com/labstack/echo/v4"
	"github.com/opentracing/opentracing-go"
)

func CrmBonusRoutes(group *echo.Group) {

	group.GET("/", handle)

}

func handle(ctx echo.Context) error {
	handleProducerMessage(ctx)
	return nil
}

func handleProducerMessage(ctx echo.Context) {
	brokers := "localhost:9093,localhost:9092,localhost:9091"

	fmt.Println("Iniciando produtor")
	parentSpan := opentracing.GlobalTracer().StartSpan("processing-kafka-message")
	defer parentSpan.Finish()

	parentSpan.SetTag("send", "true")
	ctNewx := opentracing.ContextWithSpan(ctx.Request().Context(), parentSpan)

	producer := kafkaPkg.NewProducer(brokers)

	sendMessageToKafka(ctNewx, producer, "topico1", []byte("Mensaje de prueba"))

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

	producer.SendMessage(topic, "id", messageBody)
	return nil
}
