package pubsub

import (
	"fmt"
	"encoding/json"
	"encoding/gob"
	"bytes"

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
	simpleQueueType SimpleQueueType,
	handler func(T) AckType,
	) error {
	return subscribe[T](conn, exchange, queueName, key, simpleQueueType, handler, func(raw []byte) (T, error) {
		var data T
		if err := json.Unmarshal(raw, &data); err != nil {
			var zero T
			return zero, fmt.Errorf("Error unmarshalling JSON: %v", err)
		}
		return data, nil
	})
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simpleQueueType SimpleQueueType,
	handler func(T) AckType,
	) error {
	return subscribe[T](conn, exchange, queueName, key, simpleQueueType, handler, func(raw []byte) (T, error) {
		buffer := bytes.NewBuffer(raw)
		dec := gob.NewDecoder(buffer)
		var data T
		err := dec.Decode(&data)
		if err != nil {
			var zero T
			return zero, err
		}
		return data, nil
	})
}

func subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simpleQueueType SimpleQueueType,
	handler func(T) AckType,
	unmarshaller func([]byte) (T, error),
) error {
	channel, queue, err := DeclareAndBind(conn, exchange, queueName, key, simpleQueueType)
	if err != nil {
		return fmt.Errorf("errror while declaring and binding: %v", err)
	}

	err = channel.Qos(10, 0, false)
	if err != nil {
		return fmt.Errorf("error while limiting message fetching: %v", err)
	}

	consume, err := channel.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("error while getting new delivery chan: %v", err)
	}

	go func() {
		defer channel.Close()
		for msg := range consume {
			data, err := unmarshaller(msg.Body)
			if err != nil {
				fmt.Printf("Error unmarshalling: %v", err)
				continue
			}
			ackOrNack := handler(data)
			switch ackOrNack{
			case Ack:
				msg.Ack(false)
			case NackRequeue:
				msg.Nack(false, true)
			case NackDiscard:
				msg.Nack(false, false)
			}
		}
		return
	} ()
	return nil
}