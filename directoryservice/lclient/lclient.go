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
)

var servicesReq []string

type Service struct {
	SID, Name, Address, Reply string
}

var services = make(map[string]Service)
var servicesFileName string

//  Init function requests for all services that the client will require
func init() {
	servicesFileName = "dcservicelist.json"
	servicesReq = append(servicesReq, "hello")
	//  All I need to know are the details of the lookup service
	services["lookup"] = Service{"lookup", "LookUp Service", "tcp://localhost:5569", "lookup"}

	//  Get my service list
	getServiceList()

	//  Get all services I require that are not listed in my service list
	for _, serviceReq := range servicesReq {
		if _, isListed := services[serviceReq]; isListed == false {
			reply, err := sendRequest("lookup", "getservicedesc", serviceReq, 0, nil)
			if err != nil {
				log.Println(err)
				continue
			}
			registerService(reply,
				serviceReq)
		}
	}
}

func main() {
	//  Socket to talk to server
	fmt.Println("Connecting to '", services["hello"].Name, "'' at '", services["hello"].Address, "'...")
	requester, err := zmq.NewSocket(zmq.REQ)
	if err != nil {
		panic(err)
	}
	defer requester.Close()
	requester.Connect(services["hello"].Address)

	for request_nbr := 0; request_nbr != 10; request_nbr++ {
		msg := fmt.Sprintf("Hello %d", request_nbr)
		reply, err := sendRequest("hello", "", msg, 0, requester)
		if err != nil {
			log.Println("Whoops! ", err)
			continue
		}
		fmt.Println("\tSpliced Received: ", reply)
		fmt.Println("\tOn to the next...")
	}
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
	fmt.Println("\tSending: ", msg)
	requester.Send(msg, flags)
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
