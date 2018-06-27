package main

import (
	"log"

	"github.com/streadway/amqp"
)

var (
	connString       = "amqp://guest:guest@localhost:5672/" //String de Conexão com RabbitMQ
	connection       *amqp.Connection
	channel          *amqp.Channel
	nameExchangeWork = "pocdelayed.work.exchange" //Exchange de Principal
	nameExchangeWait = "pocdelayed.wait.exchange" //Exchange de Espera
	nameQueueA       = "pocdelayed.work.a.queue"  //Fila A
	nameQueueB       = "pocdelayed.work.b.queue"  //Fila B
	nameQueueC       = "pocdelayed.work.c.queue"  //Fila C
	nameQueueW       = "pocdelayed.wait.queue"    //Fila Espera
	routingKeyA      = "work.a"                   //Rota para Fila A
	routingKeyB      = "work.b"                   //Rota para Fila B
	routingKeyC      = "work.c"                   //Rota para Fila C
	routingKeyW      = "work.*"                   //Rota para a Fila de Espera
)

func main() {
	var err error

	//Abre Conexão com RabbitMQ
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

	//Cria Exchange de Trabalho para Filas
	err = channel.ExchangeDeclare(nameExchangeWork, "topic", true, false, false, false, nil)
	if err != nil {
		log.Printf("Erro ao declarar exchange work")
	}

	//Cria Exchange de Espera
	err = channel.ExchangeDeclare(nameExchangeWait, "topic", true, false, false, false, nil)
	if err != nil {
		log.Printf("Erro ao declarar exchange wait")
	}

	//Cria Fila Work A
	_, err = channel.QueueDeclare(nameQueueA, true, false, false, false, nil)
	if err != nil {
		log.Printf("Erro ao criar a fila " + nameQueueA)
	}
	//Faz o bind Exchange-Fila
	err = channel.QueueBind(nameQueueA, routingKeyA, nameExchangeWork, false, nil)
	if err != nil {
		log.Printf("Erro ao fazer binding da fila " + nameQueueA + " com " + nameExchangeWork)
	}

	//Cria Fila Work B
	_, err = channel.QueueDeclare(nameQueueB, true, false, false, false, nil)
	if err != nil {
		log.Printf("Erro ao criar a fila " + nameQueueB)
	}
	//Faz o bind Exchange-Fila
	err = channel.QueueBind(nameQueueB, routingKeyB, nameExchangeWork, false, nil)
	if err != nil {
		log.Printf("Erro ao fazer binding da fila " + nameQueueB + " com " + nameExchangeWork)
	}

	//Cria Fila Work C
	_, err = channel.QueueDeclare(nameQueueC, true, false, false, false, nil)
	if err != nil {
		log.Printf("Erro ao criar a fila " + nameQueueC)
	}
	//Faz o bind Exchange-Fila
	err = channel.QueueBind(nameQueueC, routingKeyC, nameExchangeWork, false, nil)
	if err != nil {
		log.Printf("Erro ao fazer binding da fila " + nameQueueC + " com " + nameExchangeWork)
	}

	// Cria Fila Wait com o argumento de dead-letter apontando para exchange de trabalho
	args := make(amqp.Table)
	args["x-dead-letter-exchange"] = nameExchangeWork
	_, err = channel.QueueDeclare(nameQueueW, true, false, false, false, args)
	if err != nil {
		log.Printf("Erro ao criar a fila " + nameQueueW)
	}
	// Faz o bind Exchange-Fila com a rota work.* para que todas as mensagens
	// vindas da Exchange de trabalho façam bind com a fila de espera
	err = channel.QueueBind(nameQueueW, routingKeyW, nameExchangeWait, false, nil)
	if err != nil {
		log.Printf("Erro ao fazer binding da fila " + nameQueueW + " com " + nameExchangeWait)
	}

	log.Printf("RabbitMQ configurado com Sucesso")
}
