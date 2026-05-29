package pubsub

import (
	"fmt"
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

type SimpleQueueType int

const (
	SimpleQueueDurable SimpleQueueType = iota
	SimpleQueueTransient
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	channel, err := conn.Channel()
	if err != nil {
		return &amqp.Channel{}, amqp.Queue{}, fmt.Errorf("error while creating channel: %v", err)
	}

	queue, err := channel.QueueDeclare(queueName, queueType == SimpleQueueDurable, queueType == SimpleQueueTransient, queueType == SimpleQueueTransient, false, amqp.Table{
    	"x-dead-letter-exchange": routing.ExchangePerilDead,
	})
	if err != nil {
		return &amqp.Channel{}, amqp.Queue{}, fmt.Errorf("error while creating queue: %v", err)
	}

	err = channel.QueueBind(queueName, key, exchange, false, nil)
	if err != nil {
		return &amqp.Channel{}, amqp.Queue{}, fmt.Errorf("error while binding queue to channel: %v", err)
	}
	
	return channel, queue, nil
}

func SubscribeJSON[T any](
    conn *amqp.Connection,
    exchange,
    queueName,
    key string,
    queueType SimpleQueueType,
    handler func(T) AckType,
) error {
	channel, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return fmt.Errorf("errror while declaring and binding: %v", err)
	}

	consume, err := channel.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("error while getting new delivery chan: %v", err)
	}

	go func() error {
		for msg := range consume {
			var data T
			if err := json.Unmarshal(msg.Body, &data); err != nil {
				return fmt.Errorf("Error unmarshalling JSON: %v", err)
			}
			ackOrNack := handler(data)
			fmt.Printf("This was a %v\n", ackOrNack)
			switch ackOrNack{
			case Ack:
				msg.Ack(false)
			case NackRequeue:
				msg.Nack(false, true)
			case NackDiscard:
				msg.Nack(false, false)
			}
		}
		return nil
	} ()
	return nil
}