//
//  Time Service worker.
//  Connects REP socket to tcp://*:5580
//  Expects * from client, replies with the current time
//

package main

import (
	zmq "github.com/pebbe/zmq4"

	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"
)

type Service struct {
	SID, Name, Address string
}

//  Initialize by setting address and registering service with broker
func init() {
	address := "tcp://*:5580"
	service := Service{"time", "Time Service", address}

	fmt.Println("Registering service...")
	fmt.Println("\t", sendRequest("register",
		string(encodeTOJSON(service))))
}

//  Main function is to serve clients
func main() {
	//  Socket to talk to clients
	address := "tcp://localhost:5580"
	responder, _ := zmq.NewSocket(zmq.REP)
	defer responder.Close()
	responder.Connect(address)
	fmt.Println("Time Service listening at: ", address)

	for count := 0; ; count++ {

		//  Wait for next request from client
		request, _ := responder.Recv(0)
		fmt.Printf("\nReceived request: [%s]\n", request)

		//Do some work
		time.Sleep(time.Duration(rand.Intn(1e3)) * time.Millisecond)
		msg := time.Now().String()

		//  Send reply back to client
		fmt.Println("\tSending reply ", count, ": ", msg)
		responder.Send(msg, 0)
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
