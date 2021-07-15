package main

import (
	"fmt"
	"github.com/Just4Ease/axon/messages"
	"github.com/Just4Ease/axon/options"
	"github.com/Just4Ease/axon/systems/jetstream"
	"github.com/vmihailenco/msgpack/v5"
	"log"
	"sync"
	"time"
)

func main() {
	ev, err := jetstream.Init(options.Options{
		ServiceName: "USERS",
		Address:     "localhost:4222",
	})
	if err != nil {
		log.Fatal(err)
	}

	start := time.Now()
	const topic = "test"

	data, err := msgpack.Marshal(struct {
		FirstName string `json:"first_name"`
	}{
		FirstName: "Justice Nefe",
	})

	if err != nil {
		panic(err)
	}

	msg := messages.NewMessage()
	msg.WithContentType("application/msgpack")
	msg.WithSource("fish-svc")
	msg.WithSpecVersion("v1")
	msg.WithSubject(topic)
	msg.WithBody(data)

	wg := &sync.WaitGroup{}
	for i := 1; i < 20000; i++ {
		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			if err := ev.Publish(msg); err != nil {
				fmt.Print(err, " Error publishing.\n")
			}
		}(wg)
	}
	wg.Wait()
	end := time.Now()

	diff := end.Sub(start)

	fmt.Printf("Start: %s, End: %s, Diff: %s", start, end, diff)
}
