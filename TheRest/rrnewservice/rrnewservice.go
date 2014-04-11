//
//  Request-reply client.
//  Connects REQ socket to tcp://localhost:5559
//  Sends "Hello" to server, expects "World" back
//

package main

import (
	"encoding/json"
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"log"
	"math/rand"
	"time"
)

type Service struct {
	SID, Name, Address string
}

func main() {

	service := Service{"hello", "Time Service", "tcp://*:5580"}

	requester, _ := zmq.NewSocket(zmq.REQ)
	defer requester.Close()
	requester.Connect("tcp://localhost:5559")

	//send message
	msg := "register:" + string(encodeTOJSON(service))
	fmt.Println("\nSending Message: ", msg, "...")
	requester.Send(msg, 0)

	//receive reply
	reply, _ := requester.Recv(0)
	fmt.Printf("\tReceived reply %d [%s]\n", reply)

	//Sleep for a second
	fmt.Printf("\tTime to rest :-)\n")
	time.Sleep(time.Duration(rand.Intn(1e3)) * time.Millisecond)
}

func encodeTOJSON(service Service) []byte {
	//Encode to JSON
	b, err := json.Marshal(service)
	if err != nil {
		log.Println("Error encoding ", service.SID, " to JSON")
		panic(err)
	}
	return b
}
