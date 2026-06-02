package main

import (
	"fmt"
	"log"
	"strconv"
	"time"

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

	connectionChan, err := connection.Channel()
	if err != nil {
		log.Fatalf("couldn't create channel from connection: %v", err)
	}
	fmt.Println("Successfully connected")

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("error while entering username: %v", err)
	}

	gameState := gamelogic.NewGameState(username)

	err = pubsub.SubscribeJSON(
		connection,
		routing.ExchangePerilDirect,
		routing.PauseKey + "." + username,
		routing.PauseKey,
		pubsub.SimpleQueueTransient,
		handlerPause(gameState),
	)
	if err != nil {
		log.Fatalf("something went wrong while declaring and binding: %v", err)
	}

	err = pubsub.SubscribeJSON(
		connection,
		routing.ExchangePerilTopic,
		routing.ArmyMovesPrefix + "." + username,
		routing.ArmyMovesPrefix + ".*",
		pubsub.SimpleQueueTransient,
		handlerMove(gameState, connectionChan),
	)
	if err != nil {
		log.Fatalf("something went wrong while declaring and binding: %v", err)
	}

	err = pubsub.SubscribeJSON(
		connection,
		routing.ExchangePerilTopic,
		"war",
		"war.#",
		pubsub.SimpleQueueDurable,
		handlerWar(gameState, connectionChan),
	)
	if err != nil {
		log.Fatalf("something went wrong while declaring and binding: %v", err)
	}

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
					continue
				}
			case "move":
				armyMove, err := gameState.CommandMove(words)
				err = pubsub.PublishJSON(
					connectionChan,
					routing.ExchangePerilTopic,
					"army_moves." + username,
					armyMove,
				)
				if err != nil {
					fmt.Printf("error while moving unit: %v\n", err)
					continue
				}
				fmt.Println("move published successfully")
			case "status":
				gameState.CommandStatus()
			case "help":
				gamelogic.PrintClientHelp()
			case "spam":
				if len(words) < 2 {
					fmt.Println("not enough arguments")
					continue
				}
				spams, err := strconv.Atoi(words[1])
				if err != nil {
					fmt.Printf("error while converting string: %v\n", err)
				}
				for i := 0; i < spams; i++ {
					maliciousMessage := gamelogic.GetMaliciousLog()
					err = pubsub.PublishGob(
						connectionChan,
						routing.ExchangePerilTopic,
						routing.GameLogSlug + "." + username,
						routing.GameLog{
							CurrentTime: time.Now(),
							Message: maliciousMessage,
							Username: username,
						},
					)
					if err != nil {
						fmt.Printf("error while publishing spam: %v", err)
					}
				}
			case "quit":
				gamelogic.PrintQuit()
				return
			default:
				fmt.Println("Command not found")
		}
	}
}
