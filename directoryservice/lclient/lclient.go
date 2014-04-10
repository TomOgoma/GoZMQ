//
//  Hello World client.
//  Connects REQ socket to tcp://localhost:5555
//  Sends "Hello" to server, expects "World" back
//

package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"
)

var servicesReq []string

type Service struct {
	SID, Name, Address, Reply, Socket string
}

const (
	REQUEST_TIMEOUT = 2500 * time.Millisecond //  msecs, (> 1000!)
	REQUEST_RETRIES = 3                       //  Before we abandon
)

var services = make(map[string]Service)
var servicesFileName string

//  Init function requests for all services that the client will require
func init() {
	servicesFileName = "dcservicelist.json"
	servicesReq = append(servicesReq, "hello")
	//  All I need to know are the details of the lookup service
	services["lookup"] = Service{"lookup", "LookUp Service", "tcp://localhost:5569", "lookup", "REP"}

	//  Get my service list
	getServiceList()

	//  Get all services I require that are not listed in my service list
	for _, serviceReq := range servicesReq {
		if _, isListed := services[serviceReq]; isListed == false {
			reply, err := sendRequest("lookup", "lookup", serviceReq)
			if err != nil {
				log.Println(err)
				continue
			}
			for i := range reply {
				registerService(reply[i], serviceReq)
			}
		}
	}
}

func main() {
	//  Socket to talk to server
	fmt.Println("Connecting to '", services["hello"].Name, "'' at '", services["hello"].Address, "'...")

	for request_nbr := 0; request_nbr != 10; request_nbr++ {
		msg := fmt.Sprintf("Hello %d", request_nbr)
		reply, err := sendRequest("hello", "", msg)
		if err != nil {
			log.Println("Whoops! ", err)
			continue
		}
		fmt.Println("\tSpliced Received: ", reply)
		fmt.Println("\tOn to the next...")
	}
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

// Populates the services map with mappings for available
// services - services is a global variable hence no need
// for return
func getServiceList() {

	//read service list from file
	filepointer, err := os.Open(servicesFileName)
	if err != nil {
		log.Println(err)
		return
	}

	defer filepointer.Close()
	reader := bufio.NewReader(filepointer)
	line, err := reader.ReadBytes('\n')
	var service Service
	for err == nil {
		///*Decode from JSON
		err = json.Unmarshal(line, &service)
		if err != nil {
			log.Println("Error decoding from json")
			continue
		}
		services[service.SID] = service
		//read next line
		line, err = reader.ReadBytes('\n')
	}
	if err != io.EOF {
		log.Println(err)
	}
}

//  Registers a new service by:
//  1. Decoding the JSON message
//  2. Adding the decoded service and its socket binding to the service list
//  3. Encoding Entire service list to JSON and saving to file
func registerService(jsonMsg, SID string) {
	if jsonMsg == "NotAvailable" {
		err := "Service, " + SID + ", Not available"
		panic(err)
	}
	var newservice Service
	if len(services) == 0 {
		services = make(map[string]Service)
	}
	fmt.Println("\n\tAdding to service list ", jsonMsg, " ...")
	//Decode Message
	fmt.Println("\tDecoding...")
	err := json.Unmarshal([]byte(jsonMsg), &newservice)
	if err != nil {
		panic(err)
	}

	fmt.Println("\tRegistering...")
	//Add to the active service list
	services[newservice.SID] = newservice

	fmt.Println("\tUpdating file...")
	//Encode entire service list to JSON
	var datab, b []byte
	for _, value := range services {
		b, err = json.Marshal(value)
		if err != nil {
			log.Println("Error encoding ", value.SID, " to JSON\n",
				"Service has been registered in service list but will have to ",
				"be reloaded on next run\n", err)
			return
		}
		datab = append(datab, append(b, byte('\n'))...)
	}

	//write to file
	err = ioutil.WriteFile(servicesFileName, datab, os.ModePerm)
	if err != nil {
		log.Println("Error writing service list to file: ", err,
			"\nService has been registered in service list but will have to be",
			" reloaded on next run")
		return
	}
}
