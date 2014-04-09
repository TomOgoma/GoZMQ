//
//  Hello World worker.
//  Connects REP socket to tcp://*:5560
//  Expects * from client, replies with "World"
//

package main

import (
	zmq "github.com/pebbe/zmq4"

	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"
	//"math/rand"
)

type Service struct {
	SID, Name, Address, Reply string
}

var mydescription Service
var services = make(map[string]Service)
var allowedbinders string

//  Initialize by setting address and registering service with directory service
func init() {
	allowedbinders = "tcp://*:5560"
	mydescription = Service{"hello", "Hello Service", "tcp://localhost:5560", "hello"}
	//  All I need to know are the details of the lookup service
	services["lookup"] = Service{"lookup", "LookUp Service", "tcp://localhost:5569", "lookup"}

	fmt.Println("Registering service...")
	reply, err := sendRequest("lookup", "register", string(encodeTOJSON(mydescription)), 0, nil)
	if err != nil {
		panic(err)
	}
	fmt.Println("\t", reply)
}

//  Main function is to serve clients
func main() {
	//  Socket to talk to clients
	responder, err := zmq.NewSocket(zmq.REP)
	if err != nil {
		panic(err)
	}
	defer responder.Close()
	responder.Bind(allowedbinders)

	for {
		//  Wait for next request from client
		msg, _ := responder.Recv(0)
		fmt.Println("Received ", msg)

		//  Do some 'work'
		time.Sleep(time.Second)

		//  Send reply back to client
		reply := "World"
		sendReply(reply, responder, 0)
	}
}

//  Send an encoded reply to service
func sendReply(message string, responder *zmq.Socket, flags zmq.Flag) {
	reply := mydescription.Reply + ":" + message
	responder.Send(reply, flags)
}

//  Send a request to a service
func sendRequest(SID, service, message string, flags zmq.Flag, requester *zmq.Socket) (reply string, err error) {
	//  Bind to service if not already done
	if requester == nil {
		fmt.Println("Connecting to '", services[SID].Name, "'' at '", services[SID].Address, "'...")
		requester, err = zmq.NewSocket(zmq.REQ)
		if err != nil {
			log.Println(err)
			return
		}
		defer requester.Close()
		requester.Connect(services[SID].Address)
	}

	//  Send message
	msg := fmt.Sprintf("%s:%s", service, message)
	requester.Send(msg, 0)
	if flags != 0 {
		return
	}

	//  Wait to receive reply if there are no more messages to send
	prereply, _ := requester.Recv(0)
	fmt.Println("\tReceived: ", prereply)
	replies := strings.SplitN(prereply, ":", 2)
	if len(replies) != 2 {
		err = errors.New("Unexpected Reply")
		return
	}
	if replies[0] != services[SID].Reply {
		err = errors.New("ServiceListOutdated:Bound to wrong service")
		return
	}
	reply = replies[1]
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
