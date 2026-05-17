package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"
	"os"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Estrutura esperada do JSON vindo do mqtt-sub
type CustomPayload struct {
	UserId     	string      	`json:"userId"`
	DeviceType 	string     		`json:"deviceType"`
	DeviceID  	string     		`json:"deviceId"`
	Payload   	interface{} 	`json:"payload"`
}

func main() {
	// Configurações via Variáveis de Ambiente (futuramente)
	influxURL := os.Getenv("INFLUX_URL")
	token := os.Getenv("INFLUX_TOKEN")
	org := os.Getenv("INFLUX_ORG")
	bucket := os.Getenv("INFLUX_BUCKET")
	rabbitURL := os.Getenv("RABBIT_URL")

	// Inicializa Cliente InfluxDB Cloud
	client := influxdb2.NewClient(influxURL, token)
	writeAPI := client.WriteAPIBlocking(org, bucket)
	defer client.Close()

	// Conexão RabbitMQ
	conn, err := amqp.Dial(rabbitURL)
	if err != nil {
		log.Fatal("Falha ao conectar no RabbitMQ:", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal("Falha ao abrir canal:", err)
	}
	defer ch.Close()

	msgs, err := ch.Consume(
		"fila_influx", // Nome da fila que o barramento de eventos deve enviar os dados
		"influx-connector",           // consumer
		true, false, false, false, nil,
	)

	fmt.Println("Conectado! Aguardando dados para o InfluxDB Cloud...")

	// Loop de Processamento
	for d := range msgs {
		var data CustomPayload
		err := json.Unmarshal(d.Body, &data)
		if err != nil {
			log.Printf("Erro ao decodificar JSON: %v", err)
			continue
		}

		fields := map[string]interface{}{}

		switch payload := data.Payload.(type) {

		// Caso seja JSON (map)
		case map[string]interface{}:
			for k, v := range payload {
				fields[k] = v
			}

		// Caso seja string (talvez JSON stringificado)
		case string:
			var parsed map[string]interface{}
			if err := json.Unmarshal([]byte(payload), &parsed); err == nil {
				for k, v := range parsed {
					fields[k] = v
				}
			} else {
				fields["value"] = payload
			}

		// Qualquer outro tipo
		default:
			fields["value"] = payload
		}


		// extraio o timestamp do payload e tiro ele dos fields (se existir), senão uso o timestamp atual
		timestamp := time.Now().Unix()

		if ts, ok := fields["timestamp"].(float64); ok {
			timestamp = int64(ts)
			delete(fields, "timestamp")
		}

		// CRIA POINT (nome da tabela)
		p := influxdb2.NewPoint(
			"telemetria",
			map[string]string{
				"deviceId":   data.DeviceID,
				"deviceType": data.DeviceType,
				"userId":  data.UserId,
			},
			fields,
			time.Unix(timestamp, 0),
		)

		// ESCREVE NO INFLUX
		err = writeAPI.WritePoint(context.Background(), p)
		if err != nil {
			log.Printf("Erro ao gravar no InfluxDB: %v", err)
			continue
		}

		fmt.Printf("Gravado | Sensor: %s | Fields: %v\n", data.DeviceID, fields)
	}
}