package pubsub

import (
	"encoding/json"
	"fmt"
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	jsonData, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("Error while marhalling value: %v", err)
	}

	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body: jsonData,
	})
	if err != nil {
		return fmt.Errorf("Error while publishing context: %v", err)
	}
	return nil
}
