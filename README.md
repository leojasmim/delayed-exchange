# delayed-exchange
Repositório de exemplo que aplicação GO que utiliza filas com penalidades de espera.

## Introdução
Neste _toy-problem_ temos uma exchange **pocdelayed.work.exchange** do tipo TOPIC com 3 filas de trabalho vinculadas por uma routingKey:
(pocdelayed.work.a.queue -> work.a; pocdelayed.work.b.queue -> work.b; pocdelayed.work.C.queue -> work.c).

Temos um exchange de penalidades **pocdelayed.wait.exchange** do tipo TOPIC que irá receber mensagens de todas as filas com uma determinado tempo de espera. Através da flag **x-dead-letter-exchange** configuramos que todas as mensagens na fila de espera retornão para uma determinada exchange, neste caso a de trabalho **pocdelayed.work.exchange** após a expiração do TTL

## Passo-a-passo
1. Execute o projeto em **delayed-exchange/config** para criação das estruturas de exchange e filas no RabbitMQ
2. Execute o projeto em **delayed-exchange/publisher** para publicar uma mensagem na fila A
3. Execute o projeto em **delayed-exchange/consumer** para consumir das 3 filas distintas: `go run main.go a` Para consumir da fila A;
`go run main.go b` Para consumir da fila B; `go run main.go c` Para consumir da fila C.
   
## Resultado
Após os passos 1 e 2 serem executados, quando ocorrer a execução dos consumidores o resultado esperado é que a mensagem siga o seguinte ciclo: **FILA A -> ESPERA -> FILA B -> ESPERA -> FILA C -> ESPERA**




