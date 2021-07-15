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
	"github.com/Just4Ease/axon/systems/jetstream"
	"log"
	"sync"
	"time"
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

	start := time.Now()
	type Stuff struct {
		mu  *sync.Mutex
		num int
	}

	handleSubEv := func() error {
		const topic = "test"
		stuff := Stuff{
			mu:  &sync.Mutex{},
			num: 0,
		}
		return ev.Subscribe(topic, func(event axon.Event) {
			//fmt.Print(string(event.Data()), " Event Data")
			defer event.Ack()
			//var pl struct {
			//	FirstName string `json:"first_name"`
			//}
			//msg, err := event.Parse(&pl)
			//if err != nil {
			//	fmt.Print(err, " Err parsing event into pl.")
			//	return
			//}

			stuff.mu.Lock()
			stuff.num += 1
			log.Printf("Stuff Count: %d", stuff.num)
			stuff.mu.Unlock()

			//PrettyJson(msg)

		}, options.NewSubscriptionOptions().SetSubscriptionType(options.Shared))
	}
	end := time.Now()

	diff := end.Sub(start)

	fmt.Printf("Start: %s, End: %s, Diff: %s", start, end, diff)

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
