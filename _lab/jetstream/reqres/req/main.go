package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/Just4Ease/axon/v2/codec/msgpack"
	"github.com/Just4Ease/axon/v2/messages"
	"github.com/Just4Ease/axon/v2/options"
	"github.com/Just4Ease/axon/v2/systems/jetstream"
)

func main() {
	name := flag.String("name", "", "help message for flagname")
	flag.Parse()

	ev, _ := jetstream.Init(options.Options{
		//ContentType: "application/json",
		ServiceName: *name,
		Address:     "localhost:4222",
		//Codecs: codec.De
		Marshaler: msgpack.Marshaler{},
	})

	const endpoint = "callGreetings"

	var in = struct {
		FirstName string `json:"first_name"`
	}{
		FirstName: "Justice Nefe",
	}

	m := msgpack.Marshaler{}

	data, err := m.Marshal(in)

	if err != nil {
		panic(err)
	}

	msg := messages.NewMessage()
	msg.WithContentType("application/msgpack")
	msg.WithSource("fish-svc")
	msg.WithSpecVersion("v2")
	msg.WithSubject(endpoint)
	msg.WithBody(data)

	res, err := ev.Request(msg)
	if err != nil {
		fmt.Print(res)
	}

	PrettyJson(res)
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
