package client

import (
	"context"
	"errors"
	"github.com/segmentio/kafka-go"
)

// Client — клиент Kafka.
type Client struct {
	// Reader осуществляет операции чтения из топика.
	Reader *kafka.Reader

	// Writer осуществляет операции записи в топик.
	Writer *kafka.Writer
}

// New создаёт и инициализирует клиента Kafka.
// Функция-конструктор.
func New(brokers []string, topic string, groupId string) (*Client, error) {
	if len(brokers) == 0 || brokers[0] == "" || topic == "" || groupId == "" {
		return nil, errors.New("не указаны параметры подключения к Kafka")
	}

	c := Client{}

	// Инициализация компонента получения сообщений.
	c.Reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  groupId,
		MinBytes: 10e1,
		MaxBytes: 10e6,
	})

	// Инициализация компонента отправки сообщений.
	c.Writer = &kafka.Writer{
		Addr:     kafka.TCP(brokers[0]),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}

	return &c, nil
}

// sendMessages отправляет сообщения в Kafka.
func (c *Client) sendMessages(messages []kafka.Message) error {
	err := c.Writer.WriteMessages(context.Background(), messages...)
	return err
}

