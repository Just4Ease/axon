package main

import (
	"encoding/json"
	"fmt"
	"github.com/Just4Ease/axon"
	"github.com/Just4Ease/axon/stand"
	"log"
)

func main() {
	eventStore, err := stand.Init(axon.Options{
		ServiceName: "sample",
		Address:     "nats://0.0.0.0:4222",
		//CertContent:         "",
		//AuthenticationToken: "",
	}, "sendpack-node-a")

	if err != nil {
		log.Fatal(err)
	}

	go func() {
		var payload struct {
			Greeting string `json:"greeting"`
		}

		input := struct {
			Username string `json:"username"`
		}{
			Username: "Justice Nefe",
		}

		data, _ := json.Marshal(input)

		if err := eventStore.Request("callGreeting", data, &payload); err != nil {
			fmt.Print(err, " Err making request")
		}

		fmt.Print(payload, " - Result Payload from request")
	}()

	_ = eventStore.Reply("callGreeting", func(input []byte) ([]byte, error) {
		var payload struct {
			Username string `json:"username"`
		}
		_ = json.Unmarshal(input, &payload)

		fmt.Print(payload.Username)

		greeting := "Hello " + payload.Username

		var output = struct {
			Greeting string `json:"greeting"`
		}{
			Greeting: greeting,
		}

		return json.Marshal(output)
	})

	<-make(chan bool)
}
