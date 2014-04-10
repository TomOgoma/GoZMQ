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
)

type Service struct {
	SID, Name, Address, Reply, Socket string
}

//  Interface through which all client requests are to be processed
type processRequest func(string) (string, error)

//  Mapping of all services offered by broker service to their respective
//  processRequest() functions as defined in the interface
var myservices = make(map[string]processRequest)

//  Mapping of all services to their descriptions (address, SID, reply header...)
var services = make(map[string]Service)

const allowedbinders = "tcp://*:5569"
const servicesFileName = "dservices.json"

func init() {
	myservices["lookup"] = getServiceDesc
	myservices["register"] = registerService
	services["lookup"] = Service{"lookup", "LooKUp Service", "tcp://localhost:5569", "lookup", "REP"}
	services["register"] = Service{"register", "Registration Service", "tcp://localhost:5569", "lookup", "REP"}

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

	//  Wait for next request from client
	for {
		//  Loop within a message envelope from a single client
		fmt.Println("\nWaiting for next client...\n")
		var serviceRequired processRequest
		for count := 0; ; count++ {
			var request string
			var header []string
			request, err := responder.Recv(0)
			if err != nil {
				log.Println(err)
				header = append(header, "Error:Receive")
				sendToClient("Error Receiving Message", services["lookup"].Reply, header, "Error Receiving message", 0, responder)
				break
			}
			fmt.Println("\tCurrent: ", request)
			//  All messages where count is even in the envelope are
			//  the expected service signature hence if the service is not
			//  Present return an error
			if count%2 == 0 {
				var isPresent bool
				serviceRequired, isPresent = myservices[request]
				if !isPresent {
					header = append(header, "Error:InvalidService")
					sendToClient("Invalid Service Request", services["lookup"].Reply, header, "Invalid Service Request", 0, responder)
					break
				}
				continue
			}
			//  Check if there are more in envelope and deal with any errors
			more, err := responder.GetRcvmore()
			if err != nil {
				log.Println(err)
				header = append(header, "Error:Receive")
				sendToClient("Error Receiving Message", services["lookup"].Reply, header, "Error Receiving message", 0, responder)
				break
			}
			//  Process the current message retrieved from the envelope
			var reply string
			errorstate := ""
			reply, err = serviceRequired(request)
			if err != nil {
				log.Println(err)
				errorstate = fmt.Sprintf("Error:%s", err)
			}
			//  If no more messages in envelope, then send all processed results
			//  to the client otherwise keep processed message in header
			if more {
				header = append(header, errorstate, reply)
			} else {
				header = append(header, errorstate)
				sendToClient(reply, services["lookup"].Reply, header, errorstate, 0, responder)
				fmt.Println("\tDone")
				break
			}
		}
	}
}

func getServiceDesc(message string) (reply string, err error) {
	//  Valid (Get Service Description) request
	service, isPresent := services[message]
	//  Service not available
	if !isPresent {
		err = errors.New("NotAvailable")
		reply = "NotAvailable"
		return
	}
	//  Service located
	reply = string(encodeTOJSON(service))
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
		if _, isPresent := services[service.SID]; !isPresent {
			services[service.SID] = service
		}
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
	fmt.Println("\n\n=====================\nAvailable services:\n{SID Name Address Reply Socket}")
	for _, service := range services {
		fmt.Println(service)
	}
	fmt.Println("=====================\n")
}

//  Registers a new service by:
//  1. Decoding the JSON "register" message
//  2. Adding the decoded service to the service list
//  3. Encoding Entire service list to JSON and saving to file
func registerService(message string) (reply string, err error) {
	var newservice Service
	if len(services) == 0 {
		services = make(map[string]Service)
	}
	fmt.Println("\n\tProcessing Service Registration Request for ", message, " ...")
	//Decode Message
	fmt.Println("\tDecoding...")
	err = json.Unmarshal([]byte(message), &newservice)
	if err != nil {
		log.Println("Error decoding from JSON ", err)
		err = errors.New(fmt.Sprintf("DecodeFail:%s", err))
		reply = fmt.Sprintf("%s", err)
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
			err = errors.New(fmt.Sprintf("WriteFileFail:EncodeFail:%s", err))
			reply = fmt.Sprintf("%s", err)
			return
		}
		datab = append(datab, append(b, byte('\n'))...)
	}

	//write to file
	err = ioutil.WriteFile(servicesFileName, datab, os.ModePerm)
	if err != nil {
		log.Println("Error writing service list to file: ", err)
		err = errors.New(fmt.Sprintf("WriteFileFail:%s", err))
		reply = fmt.Sprintf("%s", err)
		return
	}

	//  Updated service list
	listServices()

	reply = "RegistrationSuccess"
	return
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
