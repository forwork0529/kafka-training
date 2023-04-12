package client
import (
	"github.com/segmentio/kafka-go"
	"testing"
)

func TestClient_sendMessages(t *testing.T) {
	// Инициализация клиента Kafka.
	kfk, err := New(
		[]string{"localhost:9092"},
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
	}

	// Отправка сообщения.
	err = kfk.sendMessages(messages)
	if err != nil {
		t.Fatal(err)
	}
}