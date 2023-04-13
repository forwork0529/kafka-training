package client
import (
	"github.com/segmentio/kafka-go"
	"testing"
	"fmt"
)

func TestClient_sendMessages(t *testing.T) {
	// Инициализация клиента Kafka.
	kfk, err := New(
		[]string{"kafka:9093"},
		"test-topic",
		"test-consumer-group",
	)
	if err != nil {
		t.Fatal(err)
	}

	// Массив сообщений для отправки.
	messages := []kafka.Message{
		{
			Key:   []byte("Test Key"),
			Value: []byte("Test Value"),
		},
		{
			Key:   []byte("Hello"),
			Value: []byte("World"),
		}, 	
	}

	// Отправка сообщения.
	fmt.Println("try to send message..")
	err = kfk.sendMessages(messages)
	if err != nil {
		t.Fatal(err)
	}
}
