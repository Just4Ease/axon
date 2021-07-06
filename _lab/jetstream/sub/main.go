package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/Just4Ease/axon"
	"github.com/Just4Ease/axon/codec/msgpack"
	"github.com/Just4Ease/axon/options"
	"github.com/Just4Ease/axon/systems/jetstream"
	"log"
)

func main() {

	name := flag.String("name", "", "help message for flagname")
	flag.Parse()


	ev, err := jetstream.Init(options.Options{
		//ContentType: "application/json",
		ServiceName: *name,
		Address:     "localhost:4222",
		//Codecs: codec.De
		Marshaler: msgpack.Marshaler{},
	})

	if err != nil {
		log.Fatal(err)
	}

	i := 0

	recvs := make([]int, 0)

	handleSubEv := func() error {
		const topic = "USERS.test"
		return ev.Subscribe(topic, func(event axon.Event) {
			//fmt.Print(string(event.Data()), " Event Data")
			defer event.Ack()
			var pl struct {
				FirstName string `json:"first_name"`
			}
			_, err := event.Parse(&pl)
			if err != nil {
				fmt.Print(err, " Err parsing event into pl.")
				return
			}
			i += 1
			recvs = append(recvs, i)
			fmt.Print(i, "\n")
		}, options.NewSubscriptionOptions().SetSubscriptionType(options.Shared))
	}

	ev.Run(context.Background(), handleSubEv)
}
