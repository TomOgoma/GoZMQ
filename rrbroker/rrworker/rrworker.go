//
//  Hello World worker.
//  Connects REP socket to tcp://*:5560
//  Expects * from client, replies with "World"
//

package main

import (
	zmq "github.com/pebbe/zmq4"

	"encoding/json"
	"fmt"
	"log"
	"math/rand"
)

type Service struct {
	SID, Name, Address string
}

//  Initialize by setting address and registering service with broker
func init() {
	service := Service{"hello", "Hello Service", "tcp://*:5560"}

	fmt.Println("Registering service...")
	fmt.Println("\t", sendRequest("register",
		string(encodeTOJSON(service))))
}

//  Main function is to serve clients
func main() {
	//  Socket to talk to clients
	responder, _ := zmq.NewSocket(zmq.REP)
	defer responder.Close()
	responder.Connect("tcp://localhost:5560")

	fmt.Println("hello worker ready for service...")

	for {
		//  Wait for next request from client
		request, _ := responder.Recv(0)
		fmt.Printf("\nReceived request: [%s]\n", request)

		//  Do some 'work'
		fmt.Println("\tI'm trying to get some work done here...")
		reply := "World"
		//  Occasionally just get the time for no apparent reason
		if rand.Int()%2 == 0 {
			reply += " -->at " + sendRequest("time", "Give me time bro")
		}

		//  Send reply back to client
		fmt.Println("\tSending reply:", reply)
		responder.Send(reply, 0)
		fmt.Println("\tDone! Next Please")
	}
}

//  Send a request to a service through the broker
func sendRequest(SID, message string) (reply string) {
	//Bind to broker
	requester, _ := zmq.NewSocket(zmq.REQ)
	defer requester.Close()
	requester.Connect("tcp://localhost:5559")

	//send message
	msg := SID + ":" + message
	requester.Send(msg, 0)

	//receive reply
	reply, _ = requester.Recv(0)
	return
}

//  Encode service struct to json
func encodeTOJSON(service Service) []byte {
	//Encode to JSON
	b, err := json.Marshal(service)
	if err != nil {
		log.Println("Error encoding ", service.SID, " to JSON")
		panic(err)
	}
	return b
}
