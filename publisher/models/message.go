package models

import (
	"encoding/json"

	uuid "github.com/satori/go.uuid"
)

//Message Modelo de mensagem da fila
type Message struct {
	ID  string
	Key string
}

//ConvertMessageIntoByteArray Converte mensagem para publicação
func ConvertMessageIntoByteArray(m *Message) []byte {
	a, _ := json.Marshal(m)
	return a
}

//ConvertArrayByteToMessage Converte publicação para mesagem
func ConvertArrayByteToMessage(msg []byte) *Message {
	m := new(Message)
	_ = json.Unmarshal(msg, m)
	return m
}

//GetNewUUID Gera um novo identificador
func GetNewUUID() string {
	key, err := uuid.NewV4()
	if err == nil {
		return key.String()
	}
	return "00000000-0000-0000-0000-000000000000"
}
