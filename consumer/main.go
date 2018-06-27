package main

import (
	"delayed-exchange/consumer/models"
	"errors"
	"log"
	"os"
	"time"

	uuid "github.com/satori/go.uuid"
	"github.com/streadway/amqp"
)

var (
	connString   = "amqp://guest:guest@localhost:5672/"
	connection   *amqp.Connection
	channel      *amqp.Channel
	exchangeWork = "pocdelayed.work.exchange"
	exchangeWait = "pocdelayed.wait.exchange"
	waitQueue    = "pocdelayed.wait.queue"
	queue        string
	routingKey   string
	err          error
)

//Cria um consumidor com a lógica da fila de espera
func main() {

	//Recebe por parametro a letra da fila que irá consumir
	// a -> Consumir da Fila a
	// b -> Consumir da Fila b
	// c -> Consumir da Fila c
	queue, routingKey = getConsumerParams(os.Args[1])

	//Abre Conexão
	connection, err = amqp.Dial(connString)
	if err != nil {
		log.Printf("Erro ao abrir conexão com  RabbitMQ")
	}

	//Abre o Canal
	channel, err = connection.Channel()
	if err != nil {
		log.Printf("Erro ao abrir canal com  RabbitMQ")
	}

	//Declara a Exchange de Trabalho
	err = channel.ExchangeDeclare(exchangeWork, "topic", true, false, false, false, nil)
	if err != nil {
		log.Printf("Erro ao declarar exchange work")
	}

	//Declara a Fila Work de Consumo
	q, err := channel.QueueDeclare(queue, true, false, false, false, nil)
	if err != nil {
		log.Printf("Erro ao criar a fila %s", queue)
	}

	//Faz o bind Exchange-Fila
	err = channel.QueueBind(queue, routingKey, exchangeWork, false, nil)
	if err != nil {
		log.Printf("Erro ao fazer binding da fila %s com %s", queue, exchangeWork)
	}

	//Consume as mensagens da Fila para processamento
	msgs := getMessages(&q, channel)

	// Loop de consumo
	forever := make(chan bool)
	go func() {
		for msg := range msgs {

			// Traduz a Mensagem
			message := models.ConvertArrayByteToMessage(msg.Body)
			log.Printf(" [x] Consumer message %s from %s", message.ID, message.Key)

			// Muda a RoutingKey para outra fila
			message.Key = changeKey(message.Key)

			//ZZZZ.....
			time.Sleep(10000 * time.Millisecond)

			//Publica Mensagem na Fila de Espera
			err = PublisherMessageIntoQueueWait(message)

			if err != nil {
				log.Printf(" [!] Error to message %s from wait: %s", message.ID, err.Error())
				msg.Nack(true, true)
			} else {
				log.Printf(" [x] Publisher message %s from wait for 10s", message.ID)
				msg.Ack(true)
			}
		}
	}()
	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}

//PublisherMessageIntoQueueWait Publica as mensagens na fila de espera
func PublisherMessageIntoQueueWait(msg *models.Message) error {

	//Abre Conexão
	connection, err = amqp.Dial(connString)
	if err != nil {
		return errors.New("Erro ao abrir conexão com  RabbitMQ")
	}
	defer connection.Close()

	//Abre o Canal
	channel, err = connection.Channel()
	if err != nil {
		return errors.New("Erro ao abrir canal com  RabbitMQ")
	}
	defer channel.Close()

	//Declara Exchange de Espera. Ela também deverá ser topic para que possamos trabalhar
	//com a routingKey das filas de trabalho
	err = channel.ExchangeDeclare(exchangeWait, "topic", true, false, false, false, nil)
	if err != nil {
		return errors.New("Erro ao declarar exchange wait")
	}

	//Declara a Fila de Espera setando o dead-letter para Exchange de Trabalho
	args := make(amqp.Table)
	args["x-dead-letter-exchange"] = exchangeWork
	_, err = channel.QueueDeclare(waitQueue, true, false, false, false, args)
	if err != nil {
		return errors.New("Erro ao criar a fila " + waitQueue)
	}

	//Faz o bind Exchange-Fila. Utilizaremos o coringa work.* para que possamos
	//receber as mensagens oriundas de todas as filas de trabalho
	err = channel.QueueBind(waitQueue, "work.*", exchangeWait, false, nil)
	if err != nil {
		return errors.New("Erro ao fazer binding da fila " + waitQueue + " com " + exchangeWait)
	}

	//Cria mensagem e publica na fila de Espera. O atributo msg.Key tem a chave para a fila
	//de trabalho que a mensagem retornará após o fim do TTL (Expiration)
	err = channel.Publish(
		exchangeWait,
		msg.Key,
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain, charset=UTF-8",
			Body:         models.ConvertMessageIntoByteArray(msg),
			Expiration:   "15000",
		})

	if err != nil {
		return errors.New("Erro ao publicar mensagem na " + queue)
	}

	return nil
}

// Seta os atributos de fila de acordo com a letra passada por parametro no inicio da aplicação
func getConsumerParams(key string) (string, string) {
	switch key {
	case "a":
		return "pocdelayed.work.a.queue", "work.a"
	case "b":
		return "pocdelayed.work.b.queue", "work.b"
	case "c":
		return "pocdelayed.work.c.queue", "work.c"
	}
	return "", ""
}

//Consome as mensagens da fila
func getMessages(queue *amqp.Queue, channel *amqp.Channel) <-chan amqp.Delivery {
	consumerName, _ := uuid.NewV4()
	msgs, _ := channel.Consume(
		queue.Name,            // queue
		consumerName.String(), // consumer
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	return msgs
}

// Lógica de troca de filas
//	- Mensagens da fila A irão para a espera e serão republicadas na Fila B
//	- Mensagens da fila B irão para a espera e serão republicadas na Fila C
//	- Mensagens da fila C irão para a espera e serão republicadas na Fila A
func changeKey(key string) string {
	switch key {
	case "work.a":
		return "work.b"
	case "work.b":
		return "work.c"
	case "work.c":
		return "work.a"
	}
	return "work.a"
}
