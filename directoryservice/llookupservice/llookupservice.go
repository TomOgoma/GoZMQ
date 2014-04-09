package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

type Service struct {
	SID, Name, Address, Reply string
}

var services = make(map[string]Service)
var allowedbinders, servicesFileName string
var mydescription Service

func init() {
	mydescription = Service{"lookup", "LooKUp Service", "tcp://localhost:5569", "lookup"}
	services["lookup"] = mydescription
	allowedbinders = "tcp://*:5569"
	servicesFileName = "dservices.json"

	//  Load Service List
	getServiceList()

	//  List available services
	listServices()
}

func main() {
	//  Socket to talk to clients
	responder, err := zmq.NewSocket(zmq.REP)
	if err != nil {
		panic(err)
	}
	defer responder.Close()
	responder.Bind(allowedbinders)
	fmt.Println("Directory Service at ", services["lookup"].Address, " waiting for connection...")

	for {

		//  Wait for next request from client
		request, _ := responder.Recv(0)
		fmt.Printf("\nReceived request: [%s]\n", request)

		//  Work on request

		message := strings.SplitN(request, ":", 2)
		//  Invalid request
		if len(message) != 2 {
			sendToClient("InvalidRequest", nil, "Invalid Service Request", 0, responder)
		} else if message[0] == "register" {
			//  Valid (Register Service) request
			registerService(message[1], responder)
		} else if message[0] == "getservicedesc" {
			//  Valid (Get Service Description) request
			service, isPresent := services[message[1]]
			//  Service not available
			if !isPresent {
				sendToClient("NotAvailable", nil, "Service not found", 0, responder)
				continue
			}
			//  Service located
			sendToClient(string(encodeTOJSON(service)), nil,
				"Sending service description", 0, responder)
		}
	}
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

// Lists all available services in the service list including the
// "register" service that is not in the service list
func listServices() {
	fmt.Println("\n\n=====================\nAvailable services:\nSID\t\tName\t\t\tAddress")
	for _, service := range services {
		fmt.Println(service.SID, "\t\t", service.Name, "\t", service.Address)
	}
	fmt.Println("=====================\n")
}

//  Registers a new service by:
//  1. Decoding the JSON "register" message
//  2. Adding the decoded service to the service list
//  3. Encoding Entire service list to JSON and saving to file
func registerService(message string, frontend *zmq.Socket) {
	var newservice Service
	var header []string
	if len(services) == 0 {
		services = make(map[string]Service)
	}
	fmt.Println("\n\tProcessing Service Registration Request for ", message, " ...")
	//Decode Message
	fmt.Println("\tDecoding...")
	err := json.Unmarshal([]byte(message), &newservice)
	if err != nil {
		log.Println("Error decoding from JSON ", err)
		sendToClient("Failed:MessageDecode", header, "Failed Registration at Message Decode", 0, frontend)
		return
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
			log.Println("Error encoding ", value.SID, " to JSON")
			sendToClient("Registered:FileWriteFail:EncodeError", header, "", 0, frontend)
			return
		}
		datab = append(datab, append(b, byte('\n'))...)
	}

	//write to file
	err = ioutil.WriteFile(servicesFileName, datab, os.ModePerm)
	if err != nil {
		log.Println("Error writing service list to file: ", err)
		sendToClient("Registered:FileWriteFail", header, "", 0, frontend)
		return
	}

	//  Inform service
	sendToClient("Registered", header, "Service registered Successfully", 0, frontend)

	//  Updated service list
	listServices()
}

func sendToClient(message string, header []string, title string, more zmq.Flag, frontend *zmq.Socket) {

	fmt.Println("\t", title)
	for key := range header {
		fmt.Printf("\tSending to client: %s\n", header[key])
		frontend.Send(header[key], zmq.SNDMORE)
	}
	message = mydescription.Reply + ":" + message
	fmt.Printf("\tSending to client: %s\n", message)
	frontend.Send(message, more)
	fmt.Println("\tDone")
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
