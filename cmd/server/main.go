package main

import (
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"

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
	fmt.Println("Successfully connected")

	// _, _, err = pubsub.DeclareAndBind(
	// 	connection,
	// 	routing.ExchangePerilTopic,
	// 	"game_logs",
	// 	"game_logs.*",
	// 	pubsub.SimpleQueueDurable,
	// )
	// if err != nil {
	// 	log.Fatalf("something went wrong while declaring and binding: %v", err)
	// }

	err = pubsub.SubscribeGob(
		connection,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		routing.GameLogSlug + ".*",
		pubsub.SimpleQueueDurable,
		handlerGameLog(),
	)
	if err != nil {
		log.Fatalf("something went wrong while declaring and binding: %v", err)
	}

	gamelogic.PrintServerHelp()
	for ;; {
		words := gamelogic.GetInput()
		if len(words) < 1 {
			continue
		}
			switch words[0] {
			case "pause":
				fmt.Println("Sending pause message")
				err = pubsub.PublishJSON(connectionChan, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
					IsPaused: true,
				})
			case "resume":
				fmt.Println("Sending resume message")
				err = pubsub.PublishJSON(connectionChan, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
					IsPaused: false,
				})
			case "quit":
				fmt.Println("Exiting")
				return
			default:
				fmt.Println("Command not found")
		}
	}
}
