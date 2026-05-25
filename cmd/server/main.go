package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/MarunDArbaumont/learn-pub-sub-starter/internal/pubsub"
	"github.com/MarunDArbaumont/learn-pub-sub-starter/internal/routing"

)

func main() {
	fmt.Println("Starting Peril server...")
	connectionString := "amqp://guest:guest@localhost:5672/"
	connection, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatalf("couldn't connect: %v", err)
	}
	defer connection.Close()

	connectionChan, err := connection.Channel()
	if err != nil {
		log.Fatalf("couldn't create channel from connection: %v", err)
	}

	err = pubsub.PublishJSON(connectionChan, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
		IsPaused: true,
	})

	fmt.Println("Successfully connected")
	
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("\nShutting program")
}
