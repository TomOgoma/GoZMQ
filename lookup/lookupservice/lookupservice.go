package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"io"
	"io/ioutil"
	msg "llibrary"
	"log"
	"os"
	"time"
)

//  Mapping of all services to their descriptions (address, SID, reply header...)
var services = make(map[string]msg.Service)

//  Mapping of all services offered by broker service to their respective
//  processRequest() functions as defined in the interface
var myservices = make(map[string]msg.ProcessRequest)

const (
	ALLOWED_BINDERS   = "tcp://*:5569"
	SERVICES_FILENAME = "dservices.json"
)

func init() {
	myservices["lookup"] = getServiceDesc
	myservices["register"] = registerService
	myservices[msg.PPP_HEARTBEAT] = msg.ProcessHeartBeat
	services["lookup"] = msg.NewService("lookup", "LooKUp Service", "tcp://localhost:5569", "lookup", "REP")
	services["register"] = msg.NewService("register", "Registration Service", "tcp://localhost:5569", "lookup", "REP")

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
	responder.Bind(ALLOWED_BINDERS)
	fmt.Println("Directory Service at ", services["lookup"].Address, " waiting for connection...")

	//  Wait for next request from client
	for {
		var service_required msg.ProcessRequest
		var message string
		service_required, message, err = msg.RecieveClientRequest(responder, myservices)
		var reply string
		if err != nil {
			msg.SendToClient(services["lookup"].Reply, fmt.Sprintf("%s", err), "Error Receiving Message", responder)
			continue
		}
		reply, err := service_required(message)
		if err != nil {
			msg.SendToClient(services["lookup"].Reply, fmt.Sprintf("%s", err), "Error Processing Request", responder)
			continue
		}
		msg.SendToClient(services["lookup"].Reply, "", reply, responder)
	}
}

func getServiceDesc(SID string) (reply string, err error) {
	//  Get description of requested service
	service, isPresent := services[SID]
	//  Service not available
	if !isPresent {
		err = errors.New("NotAvailable")
		reply = "NotAvailable"
		return
	}

	//  Get heartbeat of requested service
	var heartbeat []string
	heartbeat, err = msg.SendRequest(service, msg.PPP_HEARTBEAT, "")
	heartbeat_state := time.Now().String()
	if err != nil {
		heartbeat_state = fmt.Sprintf("%s", err)
	}
	if len(heartbeat) < 1 || heartbeat[0] != msg.PPP_READY {
		err = errors.New("Error:ServiceNotReady")
		heartbeat_state = fmt.Sprintf("%s", err)
	}
	service.Heartbeat_state = heartbeat_state
	services[SID] = service

	//  Service located
	reply = string(encodeTOJSON(service))
	return
}

// Populates the services map with mappings for available
// services - services is a global variable hence no need
// for return
func getServiceList() {

	//read service list from file
	filepointer, err := os.Open(SERVICES_FILENAME)
	if err != nil {
		log.Println(err)
		return
	}

	defer filepointer.Close()
	reader := bufio.NewReader(filepointer)
	line, err := reader.ReadBytes('\n')
	service := msg.NewService("", "", "", "", "")
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
	newservice := msg.NewService("", "", "", "", "")
	if len(services) == 0 {
		services = make(map[string]msg.Service)
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

	//  Append latest heartbeat to the service description
	newservice.Heartbeat_state = time.Now().String()

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
	err = ioutil.WriteFile(SERVICES_FILENAME, datab, os.ModePerm)
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
