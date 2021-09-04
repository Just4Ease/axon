package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/Just4Ease/axon/v2/codec/msgpack"
	"github.com/Just4Ease/axon/v2/options"
	"github.com/Just4Ease/axon/v2/systems/jetstream"
	"log"
)

func main() {

	ev, _ := jetstream.Init(options.Options{
		//ContentType: "application/json",
		ServiceName: "fish-svc",
		Address:     "localhost:4222",
		//Codecs: codec.De
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

	res, err := ev.Request(endpoint, data, options.SetPubContentType("application/msgpack"), options.SetPubMsgVersion("v0.1.1"))
	if err != nil {
		log.Fatal(err)
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
