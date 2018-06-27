package main

import (
	"delayed-exchange/publisher/models"
	"log"

	"github.com/streadway/amqp"
)

var (
	connString = "amqp://guest:guest@localhost:5672/"
	connection *amqp.Connection
	channel    *amqp.Channel
	exchange   string
	queue      string
	routingKey string
	err        error
)

// Publica uma mensagem simples na Fila A para início do Fluxo de Consumo
func main() {
	exchange = "pocdelayed.work.exchange"
	queue = "pocdelayed.work.a.queue"
	routingKey = "work.a"

	// Cria a mensagem. A routingKey vai na mensagem para ser manipulada no consumer
	msg := new(models.Message)
	msg.ID = models.GetNewUUID()
	msg.Key = routingKey

	//Abre Conexão
	connection, err = amqp.Dial(connString)
	if err != nil {
		log.Printf("Erro ao abrir conexão com  RabbitMQ")
	}
	defer connection.Close()

	//Abre o Canal
	channel, err = connection.Channel()
	if err != nil {
		log.Printf("Erro ao abrir canal com  RabbitMQ")
	}
	defer channel.Close()

	//Declara a Exchange de Trabalho
	err = channel.ExchangeDeclare(exchange, "topic", true, false, false, false, nil)
	if err != nil {
		log.Printf("Erro ao declarar exchange work")
	}

	//Declara Fila Work A
	_, err = channel.QueueDeclare(queue, true, false, false, false, nil)
	if err != nil {
		log.Printf("Erro ao criar a fila " + queue)
	}

	//Faz o bind Exchange-Fila
	err = channel.QueueBind(queue, routingKey, exchange, false, nil)
	if err != nil {
		log.Printf("Erro ao fazer binding da fila " + queue + " com " + exchange)
	}

	//Cria mensagem e publica na fila A
	err = channel.Publish(
		exchange,
		routingKey,
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain, charset=UTF-8",
			Body:         models.ConvertMessageIntoByteArray(msg),
		})

	if err != nil {
		log.Printf("Erro ao publicar mensagem na %s", queue)
	}

	log.Printf("Mensagem ID: " + msg.ID + " - Key: " + msg.Key + " publicada na fila " + queue)
}
