package main

import (
	"context"
	"fmt"
	"github.com/Just4Ease/axon/messages"
	"github.com/Just4Ease/axon/options"
	"github.com/Just4Ease/axon/systems/liftbridged"
	"github.com/vmihailenco/msgpack/v5"
	"log"
	"time"
)

func main() {
	ev, err := liftbridged.Init(options.Options{
		ServiceName: "fish-svc",
		Address:     "localhost:4222",
		Context:     context.Background(),
	})

	if err != nil {
		log.Fatal(err)
	}

	const topic = "io.axon.test"

	data, err := msgpack.Marshal(struct {
		FirstName string `json:"first_name"`
	}{
		FirstName: "Justice Nefe",
	})

	if err != nil {
		panic(err)
	}

	//wg := &sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		//wg.Add(1)
		//go func(wg *sync.WaitGroup) {
		//	defer wg.Done()

		msg := messages.NewMessage()
		msg.WithContentType("application/msgpack")
		msg.WithSource("fish-svc")
		msg.WithSpecVersion("v1")
		msg.WithSubject(topic)
		msg.WithBody(data)


		if err := ev.Publish(msg); err != nil {
			fmt.Print(err, " Error publishing.")
			return
		}
		fmt.Printf("done sending %d \r", i)
		//}(wg)
	}
	//wg.Wait()

	time.Sleep(time.Second * 3)
}
