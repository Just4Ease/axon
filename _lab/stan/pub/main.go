package main

import (
	"fmt"
	"github.com/Just4Ease/axon/messages"
	"github.com/Just4Ease/axon/options"
	"github.com/Just4Ease/axon/systems/stand"
	"github.com/vmihailenco/msgpack/v5"
	"sync"
)

func main() {
	ev, _ := stand.Init(options.Options{
		ServiceName: "fish-svc",
		Address:     "localhost:4222",
	}, "sample")

	const topic = "io.axon.test"

	data, err := msgpack.Marshal(struct {
		FirstName string `json:"first_name"`
	}{
		FirstName: "Justice Nefe",
	})

	if err != nil {
		panic(err)
	}

	msg := messages.NewMessage()
	msg.WithContentType(messages.ContentType("application/msgpack"))
	msg.WithSource("fish-svc")
	msg.WithSpecVersion("v1")
	msg.WithSubject(topic)
	msg.WithBody(data)

	wg := &sync.WaitGroup{}
	for i := 1; i < 100000; i++ {
		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			if err := ev.Publish(msg); err != nil {
				fmt.Print(err, " Error publishing.")
			}
		}(wg)
	}
	wg.Wait()
}
