package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/Just4Ease/axon/codec/msgpack"
	"github.com/Just4Ease/axon/messages"
	"github.com/Just4Ease/axon/options"
	"github.com/Just4Ease/axon/systems/stand"
)

func main() {
	name := flag.String("name", "", "help message for flagname")
	flag.Parse()

	ev, _ := stand.Init(options.Options{
		//ContentType: "application/json",
		ServiceName: *name,
		Address:     "localhost:4222",
		//Codecs: codec.De
		Marshaler: msgpack.Marshaler{},
	}, "sample")

	const endpoint = "callGreetings"
	//
	//var in = struct {
	//	FirstName string `json:"first_name"`
	//}{
	//	FirstName: "Justice Nefe",
	//}

	//m := msgpack.Marshaler{}
	//
	//data, err := m.Marshal(in)
	//
	//if err != nil {
	//	panic(err)
	//}

	//msg := messages.NewMessage()
	//msg.WithContentType(messages.ContentType("application/msgpack"))
	//msg.WithSource("fish-svc")
	//msg.WithSpecVersion("v2")
	//msg.WithSubject(endpoint)
	//msg.WithBody(data)

	err := ev.Reply(endpoint, func(mg *messages.Message) (*messages.Message, error) {
		PrettyJson(mg)

		var in struct {
			FirstName string `json:"first_name"`
		}
		msh := msgpack.Marshaler{}

		if err := msh.Unmarshal(mg.Body, &in); err != nil {
			return nil, err
		}


		PrettyJson(in)
		return mg, nil
	})

	if err != nil {
		fmt.Print(err)
	}
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
