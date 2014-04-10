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

const (
	REQUEST_TIMEOUT = 2500 * time.Millisecond //  msecs, (> 1000!)
	REQUEST_RETRIES = 3                       //  Before we abandon
)

//  Initialize by setting address and registering service with directory service
func init() {
	allowedbinders = "tcp://*:5560"
	mydescription = Service{"hello", "Hello Service", "tcp://localhost:5560", "hello", "REP"}
	//  All I need to know are the details of the lookup service
	services["lookup"] = Service{"lookup", "LookUp Service", "tcp://localhost:5569", "lookup", "REP"}

	fmt.Println("Registering service...")
	reply, err := sendRequest("lookup", "register", string(encodeTOJSON(mydescription)))
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
func sendRequest(SID, service, message string) (reply []string, err error) {
	//  Bind to service if not already done
	var requester *zmq.Socket
	fmt.Println("Connecting to '", services[SID].Name, "'' at '", services[SID].Address, "'...")
	requester, err = zmq.NewSocket(zmq.REQ)
	if err != nil {
		log.Println(err)
		return
	}
	requester.Connect(services[SID].Address)

	poller := zmq.NewPoller()
	poller.Add(requester, zmq.POLLIN)

	retries_left := REQUEST_RETRIES
	//  Send message
	for retries_left > 0 {
		//  The service required in first packet of envelope
		//  This implies that all odd packets in the message are the service identifiers
		requester.Send(service, zmq.SNDMORE)
		//  The message for given service in second packet of envelope
		requester.Send(message, 0)
		for expect_reply := true; expect_reply; {
			//  Poll socket for a reply, with timeout
			var sockets []zmq.Polled
			sockets, err = poller.Poll(REQUEST_TIMEOUT)
			if err != nil {
				break //  Interrupted
			}

			//  Wait to receive reply if there are no more messages to send
			if len(sockets) > 0 {
				//  Receive all replies in the envelope before processing
				var more bool
				for count := 0; ; count++ {
					var rep string
					rep, err = requester.Recv(0)
					if err != nil {
						retries_left--
						break
					}

					reply = append(reply, rep)
					//  The first message in the envelope is the expected service signature
					if count == 0 && rep != services[SID].Reply {
						err = errors.New("ServiceListOutdated:Bound to wrong service")
						retries_left = 0
						break
					}
					if count == 1 && strings.HasPrefix(rep, "Error") {
						err = errors.New(rep)
						retries_left = 0
						break
					}
					//  Break from loop if there are no more messages or
					//  an error condition occurs
					more, err = requester.GetRcvmore()
					if !more || err != nil {
						retries_left--
						break
					}
				}

				//  If there was an error retrieving the message, then we
				//  should still expect replies
				if err != nil {
					break
				}
				//  All messages retrieved from packet successfully
				//  Leave the retrieve/retry loops
				retries_left = 0
				expect_reply = false
			} else {
				retries_left--
				if retries_left == 0 {
					requester.Close()
					return
				} else {
					err = nil
					reply = nil
					fmt.Println("No response from server, retrying...")
					//  Old socket is confused; close it and open a new one
					requester.Close()
					requester, _ = zmq.NewSocket(zmq.REQ)
					requester.Connect(services[SID].Address)
					// Recreate poller for new client
					poller = zmq.NewPoller()
					poller.Add(requester, zmq.POLLIN)
					//  Send request again, on new socket
					requester.Send(service, zmq.SNDMORE)
					requester.Send(message, 0)
				}
			}
		}
	}
	fmt.Println("\tReceived: ", reply)
	if len(reply) < 2 {
		err = errors.New(fmt.Sprintf("Error:UnexpectedReply:%s", err))
		requester.Close()
		return
	}
	reply = reply[2:]
	requester.Close()
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
