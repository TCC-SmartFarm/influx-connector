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
	UserId          string      `json:"userId"`
	ApplicationId   string      `json:"applicationId"`
	DeviceType      string     	`json:"deviceType"`
	DevAddr         string     	`json:"devAddr"`
	DevEUI        	string		`json:"devEUI"`
	Name            string		`json:"name"`
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

	_, err = ch.QueueDeclare(
		"fila_influx",
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,
	)
	if err != nil {
		log.Fatal("Falha ao declarar fila:", err)
	}

	msgs, err := ch.Consume(
		"fila_influx",
		"influx-connector",
		true, false, false, false, nil,
	)
	if err != nil {
		log.Fatal("Falha ao consumir fila:", err)
	}

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

		// Remove 'devEUI' dos fields para evitar conflito com a Tag
		delete(fields, "devEUI")

		log.Printf("Recebido para Influx UserId: %s | Sensor: %s | Fields: %v | DevEUI: %s\n", data.UserId, data.DevAddr, fields, data.DevEUI)

		// CRIA POINT (nome da tabela)
		p := influxdb2.NewPoint(
			"telemetria",
			map[string]string{
				"userId":     data.UserId,
				"devAddr":    data.DevAddr,
				"deviceType": data.DeviceType,
				"applicationId": data.ApplicationId,
				"devEUI":    data.DevEUI,
				"name":      data.Name,
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

		log.Printf("Gravado | timestamp: %d | Name: %s | Sensor: %s | Fields: %v | DevEUI: %s\n", timestamp, data.Name, data.DevAddr, fields, data.DevEUI)
	}
}