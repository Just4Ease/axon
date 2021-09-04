package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/Just4Ease/axon/v2/codec/msgpack"
	"github.com/Just4Ease/axon/v2/messages"
	"github.com/Just4Ease/axon/v2/options"
	"github.com/Just4Ease/axon/v2/systems/jetstream"
	"time"
)

func main() {
	//name := flag.String("name", "", "help message for flagname")
	//flag.Parse()

	ev, _ := jetstream.Init(options.Options{
		//ContentType: "application/json",
		ServiceName: "users",
		Address:     "localhost:4222",
		//Codecs: codec.De
	})

	const endpoint = "callGreetings"
	go func() {

		err := ev.Reply(endpoint, func(mg *messages.Message) (*messages.Message, error) {
			PrettyJson(mg)

			in := struct {
				Token    string `json:"token"`
				Password string `json:"password"`
			}{
				Token: "something light",
			}
			//
			//
			//PrettyJson(in)
			msh := msgpack.Marshaler{}

			data, _ := msh.Marshal(in)

			mg.WithBody(data)
			return mg, nil
		}, options.SetSubContentType("application/msgpack"), options.SetExpectedMessageSpecVersion("v0.1.1"))

		if err != nil {
			fmt.Print(err)
		}
		<-make(chan bool)
	}()

	timer := time.AfterFunc(time.Second*10, func() {
		ev.Close()
		fmt.Print("Closed")
	})

	<-timer.C

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
