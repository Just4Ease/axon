package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/Just4Ease/axon"
	"github.com/Just4Ease/axon/codec/msgpack"
	"github.com/Just4Ease/axon/options"
	"github.com/Just4Ease/axon/systems/liftbridged"
	"log"
)

func main() {

	name := flag.String("name", "", "help message for flagname")
	flag.Parse()

	ev, err := liftbridged.Init(options.Options{
		//ContentType: "application/json",
		ServiceName: *name,
		Address:     "localhost:4222",
		//Codecs: codec.De
		Marshaler: msgpack.Marshaler{},
	})
	if err != nil {
		log.Fatal(err)
	}

	handleSubEv := func() error {
		const topic = "io.axon.test"
		return ev.Subscribe(topic, func(event axon.Event) {
			//fmt.Print(string(event.Data()), " Event Data")
			defer event.Ack()
			var pl struct {
				FirstName string `json:"first_name"`
			}
			mg, err := event.Parse(&pl)
			if err != nil {
				fmt.Print(err, " Err parsing event into pl.")
				return
			}
			PrettyJson(mg)
		}, options.NewSubscriptionOptions().SetSubscriptionType(options.Shared))
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