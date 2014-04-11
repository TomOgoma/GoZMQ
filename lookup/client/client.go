//
//  Hello World client.
//  Connects REQ socket to tcp://localhost:5555
//  Sends "Hello" to server, expects "World" back
//

package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	msg "llibrary"
	"log"
	"os"
)

var servicesReq []string

var services = make(map[string]msg.Service)
var servicesFileName string

//  Init function requests for all services that the client will require
func init() {
	servicesFileName = "dcservicelist.json"
	servicesReq = append(servicesReq, "hello")
	//  All I need to know are the details of the lookup service
	services["lookup"] = msg.NewService("lookup", "LookUp Service", "tcp://localhost:5569", "lookup", "REP")

	//  Get my service list
	getServiceList()

	//  Get all services I require that are not listed in my service list
	for _, serviceReq := range servicesReq {
		if _, isListed := services[serviceReq]; isListed == false {
			reply, err := msg.SendRequest(services["lookup"], "lookup", serviceReq)
			if err != nil {
				panic(err)
			}
			for i := range reply {
				registerService(reply[i], serviceReq)
			}
		}
	}
}

func main() {
	for request_nbr := 0; request_nbr != 10; request_nbr++ {
		message := fmt.Sprintf("Hello %d", request_nbr)
		reply, err := msg.SendRequest(services["hello"], "hello", message)
		if err != nil {
			log.Println("Whoops! ", err)
			reply, err = msg.SendRequest(services["lookup"], "lookup", "hello")
			if err != nil {
				log.Println(err)
				continue
			}
			for i := range reply {
				registerService(reply[i], "hello")
			}
			continue
		}
		fmt.Println("\tSpliced Received: ", reply)
		fmt.Println("\tOn to the next...")
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
	service := msg.NewService("", "", "", "", "")
	for err == nil {
		//  Decode from JSON
		err = json.Unmarshal(line, &service)
		if err != nil {
			log.Println("Error decoding from json")
			continue
		}
		fmt.Println(service.SID)
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
	newservice := msg.NewService("", "", "", "", "")
	if len(services) == 0 {
		services = make(map[string]msg.Service)
	}
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
