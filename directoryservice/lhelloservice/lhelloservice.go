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
	msg "llibrary"
	"log"
	"time"
)

//  Mapping of all services offered by broker service to their respective
//  processRequest() functions as defined in the interface
var myservices = make(map[string]msg.ProcessRequest)

//  Mapping of all services to their descriptions (address, SID, reply header...)
var services = make(map[string]msg.Service)
var mydescription msg.Service
var allowedbinders string

//  Initialize by setting address and registering service with directory service
func init() {
	myservices["hello"] = doHelloWorld
	myservices[msg.PPP_HEARTBEAT] = msg.ProcessHeartBeat
	allowedbinders = "tcp://*:5560"
	mydescription = msg.NewService("hello", "Hello Service", "tcp://localhost:5560", "hello", "REP")
	//  All I need to know are the details of the lookup service
	services["lookup"] = msg.NewService("lookup", "LookUp Service", "tcp://localhost:5569", "lookup", "REP")

	fmt.Println("Registering service...")
	reply, err := msg.SendRequest(services["lookup"], "register", string(encodeTOJSON(mydescription)))
	if err != nil {
		log.Println(err)
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
		var service_required msg.ProcessRequest
		var message string
		var reply string
		//  Wait for next request from client
		service_required, message, err = msg.RecieveClientRequest(responder, myservices)
		if err != nil {
			msg.SendToClient(mydescription.Reply, fmt.Sprintf("Error:Receive:%s", err), "Error Receiving message", responder)
			continue
		}
		fmt.Println("Received ", message)

		//  Do some 'work'
		time.Sleep(time.Second)
		reply, err = service_required(message)
		if err != nil {
			msg.SendToClient(mydescription.Reply, fmt.Sprintf("Error:Receive:%s", err), "Error Processing Request", responder)
			continue
		}

		//  Send reply back to client
		msg.SendToClient(mydescription.Reply, "", reply, responder)
	}
}

func doHelloWorld(message string) (reply string, err error) {
	reply = "World"
	return
}

//  Encode service struct to json
func encodeTOJSON(service msg.Service) []byte {
	//Encode to JSON
	b, err := json.Marshal(service)
	if err != nil {
		log.Println("Error encoding ", service.SID, " to JSON")
		panic(err)
	}
	return b
}
