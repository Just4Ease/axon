package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/Just4Ease/axon/v2"
	"github.com/Just4Ease/axon/v2/options"
	"github.com/Just4Ease/axon/v2/systems/jetstream"
	"log"
)

func main() {

	name := flag.String("name", "", "help message for flagname")
	flag.Parse()

	ev, err := jetstream.Init(options.Options{
		ServiceName: *name,
		Address:     "localhost:4222",
	})

	if err != nil {
		log.Fatal(err)
	}

	handleSubEv := func() error {
		const topic = "test"
		return ev.Subscribe(topic, func(event axon.Event) {
			defer event.Ack()

			PrettyJson(event.Message())
		})
	}

	ev.Run(context.Background(), handleSubEv)
}

const (
	empty = ""
	tab   = "\t"
)

func PrettyJson(data interface{}) {
	buffer := new(bytes.Buffer)
	encoder := json.NewEncoder(buffer)
	encoder.SetIndent(empty, tab)

	err := encoder.Encode(data)
	if err != nil {
		return
	}
	fmt.Print(buffer.String())
}
