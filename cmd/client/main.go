package main

import (
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func main() {
	fmt.Println("Starting Peril client...")

	connectionString := "amqp://guest:guest@localhost:5672/"
	connection, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatalf("couldn't connect: %v", err)
	}
	defer connection.Close()

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("error while entering username: %v", err)
	}

	_, _, err = pubsub.DeclareAndBind(
		connection,
		routing.ExchangePerilDirect,
		routing.PauseKey + "." + username,
		routing.PauseKey,
		pubsub.SimpleQueueTransient,
	)
	if err != nil {
		log.Fatalf("something went wrong while declaring and binding: %v", err)
	}

	gameState := gamelogic.NewGameState(username)

	for ;; {
		words := gamelogic.GetInput()
		if len(words) < 1 {
			continue
		}
			switch words[0] {
			case "spawn":
				err = gameState.CommandSpawn(words)
				if err != nil {
					fmt.Printf("error while spawning unit: %v\n", err)
				}
			case "move":
				_, err := gameState.CommandMove(words)
				if err != nil {
					fmt.Printf("error while moving unit: %v\n", err)
				}
				fmt.Printf("%v moved successfully to %v\n", words[2], words[1])
			case "status":
				gameState.CommandStatus()
			case "help":
				gamelogic.PrintClientHelp()
			case "spam":
				fmt.Println("Spamming not allowed")
			case "quit":
				gamelogic.PrintQuit()
				return
			default:
				fmt.Println("Command not found")
		}
	}
}
