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
)

type Service struct {
	SID, Name, Address, Reply, Socket string
}

var mydescription Service
var services = make(map[string]Service)
var allowedbinders string

//  Initialize by setting address and registering service with directory service
func init() {
	allowedbinders = "tcp://*:5560"
	mydescription = Service{"hello", "Hello Service", "tcp://localhost:5560", "hello", "REP"}
	//  All I need to know are the details of the lookup service
	services["lookup"] = Service{"lookup", "LookUp Service", "tcp://localhost:5569", "lookup", "REP"}

	fmt.Println("Registering service...")
	reply, err := sendRequest("lookup", "register", string(encodeTOJSON(mydescription)), 0, nil)
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
		var header []string
		//  Wait for next request from client
		msg, err := responder.Recv(0)
		if err != nil {
			header = append(header, "Error:Receive")
			sendToClient("Error Receiving Message", mydescription.Reply, header, "Error Receiving message", 0, responder)
			continue
		}
		fmt.Println("Received ", msg)

		//  Do some 'work'
		time.Sleep(time.Second)

		//  Send reply back to client
		reply := "World"
		header = append(header, "")
		sendToClient(reply, mydescription.Reply, header, reply, 0, responder)
	}
}

func sendToClient(message, myreplyheader string,
	header []string, title string, more zmq.Flag, frontend *zmq.Socket) {

	frontend.Send(myreplyheader, zmq.SNDMORE)
	fmt.Println("\t", title)
	for key := range header {
		fmt.Printf("\tSending header to client: %s\n", header[key])
		frontend.Send(header[key], zmq.SNDMORE)
	}
	fmt.Printf("\tSending message to client: %s\n", message)
	frontend.Send(message, more)
}

//  Send a request to a service
func sendRequest(SID, service, message string, flags zmq.Flag, requester *zmq.Socket) (reply []string, err error) {
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
	//  The service required in first packet of envelope
	//  This implies that all odd packets in the message are the service identifiers
	requester.Send(service, zmq.SNDMORE)
	//  The message for given service in second packet of envelope
	requester.Send(message, flags)
	if flags != 0 {
		return
	}

	//  Wait to receive reply if there are no more messages to send
	//  Receive all replies in the envelope before processing
	for count := 0; ; count++ {
		var rep string
		rep, err = requester.Recv(0)
		reply = append(reply, rep)
		if err != nil {
			return
		}
		//  The first message in the envelope is the expected service signature
		if count == 0 && reply[0] != services[SID].Reply {
			err = errors.New("ServiceListOutdated:Bound to wrong service")
			return
		}
		if count == 1 && strings.HasPrefix(reply[1], "Error") {
			err = errors.New(reply[1])
			return
		}
		//  Check if there are more in envelope otherwise break from loop
		var more bool
		more, err = requester.GetRcvmore()
		if err != nil {
			return
		}
		if !more {
			break
		}
	}
	fmt.Println("\tReceived: ", reply)
	if len(reply) < 2 {
		err = errors.New("UnexpectedReply")
		return
	}
	reply = reply[2:]
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
