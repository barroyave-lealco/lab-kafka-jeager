package main

import (
	"fmt"
	"main/jeager"
	kafkaPkg "main/kafka"
	"os"

	"main/api"
	"main/routes"

	"github.com/opentracing/opentracing-go"
)

func server() {

	api.NewAPIRestServer(
		os.Getenv("API_REST_CRM_BONUS_PORT"),
		os.Getenv("API_REST_CRM_BONUS_GLOBAL_PREFIX"),
		routes.CrmBonusRoutes,
	)
}

func main() {
	brokers := "localhost:9093,localhost:9092,localhost:9091"
	_, closer, err := jeager.InitJaeger("http://localhost:14268/api/traces")
	if err != nil {
		fmt.Println(err)
	}
	defer closer.Close()

	handleConsumerMessage(brokers)

	server := routes.NewAPIRestServer("3001", "prueba", routes.CrmBonusRoutes)

	server.RunServer()
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
