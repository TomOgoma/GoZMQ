//
//  Simple request-reply broker.
//

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
	SID, Name, Address string
	Backend            *zmq.Socket
}

var services = make(map[string]Service)
var poller *zmq.Poller
var address string

func main() {
	//  Initialize polling
	poller = zmq.NewPoller()

	//  Prepare our frontend sockets
	frontend, _ := zmq.NewSocket(zmq.ROUTER)
	defer frontend.Close()
	address = "tcp://*:5559"
	frontend.Bind(address)
	//  Initialize frontend poll set
	poller.Add(frontend, zmq.POLLIN)

	//  Load Service List
	getServiceList()
	//Make sure all services in list are closed at end of execution
	for _, service := range services {
		defer service.Backend.Close()
	}
	//  Initialize polling and binding to addresses
	initializeServices()

	fmt.Println("Broker at ", address, " waiting for connection...")
	//  List available services
	listServices()

	//  Switch messages between sockets
	for {
		sockets, _ := poller.Poll(-1)
		for _, socket := range sockets {
			switch s := socket.Socket; s {
			case frontend:
				backend := serveFrontend(s)
				//  (For newly registered services)
				//  Make sure socket is closed at end of execution
				if backend != nil {
					defer backend.Close()
					backend = nil
				}

			//  All services fall under default
			default:
				//fmt.Println("Receiving message from service...")
				for {
					msg, _ := s.Recv(0)
					//fmt.Printf("\tIn-->%s\n", msg)
					if more, _ := s.GetRcvmore(); more {
						//fmt.Printf("\tForwarding to client: %s\n", msg)
						frontend.Send(msg, zmq.SNDMORE)
					} else {
						//fmt.Printf("\tForwarding to client: %s\n", msg)
						frontend.Send(msg, 0)
						//fmt.Println("\tDone")
						break
					}
				} //end for (receive message from service)
			} //end switch (sockets polled)
		} //end for(sockets polled)
	} //end for(forever)
}

func getSocket() *zmq.Socket {
	socket, err := zmq.NewSocket(zmq.DEALER)
	if err != nil {
		log.Println(err)
		return nil
	}
	return socket
}

// Initializes the list of all services subscribing them to
// the poller
// If a service is not mapped to a socket, it is removed from
// the service list
func initializeServices() {
	for _, service := range services {
		//  Initialize backend poll set
		//  Delete mappings that didn't successfully bind to a socket
		if service.Backend == nil {
			fmt.Printf("Whoops, problem creating binding for %s\n", service.Name)
			delete(services, service.SID)
			continue
		}
		service.Backend.Bind(service.Address)
		poller.Add(service.Backend, zmq.POLLIN)
	}
}

// Populates the services map with mappings for available
// services - services is a global variable hence no need
// for return
func getServiceList() {

	//read service list from file
	filepointer, err := os.Open("services.json")
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
		services[service.SID] = Service{
			service.SID,
			service.Name,
			service.Address,
			getSocket()}
		//read next line
		line, err = reader.ReadBytes('\n')
	}
	if err != io.EOF {
		log.Println(err)
	}
}

//  Registers a new service by:
//  1. Decoding the JSON "register" message
//  2. Adding the decoded service and its socket binding to the service list
//  3. Initializing the service and adding it to the poller
//  4. Encoding Entire service list to JSON and saving to file
func registerService(message string, header []string, frontend *zmq.Socket) *zmq.Socket {
	if len(services) == 0 {
		services = make(map[string]Service)
	}
	var newservice Service
	clientmessage := ""
	fmt.Println("\nProcessing Service Registration Request for ", message, " ...")
	//Decode Message
	fmt.Println("\tDecoding...")
	err := json.Unmarshal([]byte(message), &newservice)
	if err != nil {
		log.Println("Error decoding from JSON ", err)
		sendToClient("Failed:Decoding Message", header, "Failed Registration at Message Decode", 0, frontend)
		return nil
	}

	fmt.Println("\tRegistering...")
	//Add to the active service list
	services[newservice.SID] = Service{newservice.SID, newservice.Name, newservice.Address, getSocket()}

	//  Initialize backend poll set
	//  Delete mapping if service didn't successfully bind to a socket
	if services[newservice.SID].Backend == nil {
		log.Println(services[newservice.SID])
		log.Printf("Whoops, problem creating binding for %s\n", newservice.Name)
		delete(services, newservice.SID)
		sendToClient("Failed:Socket Creation", header, "Failed Registration at socket creation", 0, frontend)
		return nil
	}
	services[newservice.SID].Backend.Bind(services[newservice.SID].Address)
	poller.Add(services[newservice.SID].Backend, zmq.POLLIN)

	fmt.Println("\tUpdating file...")
	//Encode entire service list to JSON
	var datab, b []byte
	for _, value := range services {
		b, err = json.Marshal(value)
		if err != nil {
			log.Println("Error encoding ", value.SID, " to JSON")
			clientmessage = ":FileWriteFail:EncodeError"
		}
		datab = append(datab, append(b, byte('\n'))...)
	}

	//write to file
	err = ioutil.WriteFile("services.json", datab, os.ModePerm)
	if err != nil {
		log.Println("Error writing service list to file: ", err)
		clientmessage = ":FileWriteFail"
	}

	//  Inform service
	fmt.Println("\tSuccess! Informing service...")
	sendToClient("Registered"+clientmessage, header, "Service registered Successfully", 0, frontend)

	//  Updated service list
	listServices()
	return services[newservice.SID].Backend
}

// Lists all available services in the service list including the
// "register" service that is not in the service list
func listServices() {
	fmt.Println("\n\n=====================\nAvailable services:\nSID\t\tName\t\t\tAddress")
	for _, service := range services {
		fmt.Println(service.SID, "\t\t", service.Name, "\t\t\t", service.Address)
	}
	fmt.Println("register\tService Registration\t", address)
	fmt.Println("=====================\n")
}

func serveFrontend(frontend *zmq.Socket) (back *zmq.Socket) {
	var header []string
	var service Service
	isPresent := false
	//fmt.Println("\nReceiving message from client...")
	for {
		msg, _ := frontend.Recv(0)
		//fmt.Printf("\tIn-->%s\n", msg)
		message := strings.SplitN(msg, ":", 2)
		if more, _ := frontend.GetRcvmore(); more {
			//  If this part of the message contains no service description
			//  treat is as a header
			if len(message) != 2 {
				header = append(header, msg)
				continue
			}

			//  If the service request is to register a service, register it
			if message[0] == "register" {
				back = registerService(message[1], header, frontend)
				return
			}

			//  If the service requested is not in the service list then assume
			//  it will be at end of message and treat message as part of header
			service, isPresent = services[message[0]]
			if !isPresent {
				header = append(header, msg)
				continue
			}

			//  If the service requested is available send the early parts of
			//  message at this point
			for key := range header {
				//fmt.Printf("\tForwarding to %s at %s: %s\n", service.Name, service.Address, header[key])
				service.Backend.Send(header[key], zmq.SNDMORE)
			}
			header = header[:0] // Empty header to avoid accidental resend below
			service.Backend.Send(message[1], zmq.SNDMORE)
		} else {
			// If the service request is to register a service, register it
			if message[0] == "register" {
				back = registerService(message[1], header, frontend)
				return
			}

			// Has the service already been discovered? -If not, discover it
			if !isPresent {
				service, isPresent = services[message[0]]
			}

			//  If the service is still not discovered at this point, then we
			//   have a problem. Report back to client
			if !isPresent {
				sendToClient("InvalidService", header, "Invalid Service SID", 0, frontend)
				break
			}

			//  Otherwise everything is good fetch the service and send request
			//  Send the assumed header of the message
			for key := range header {
				//fmt.Printf("\tForwarding to %s at %s: %s\n", service.Name, service.Address, header[key])
				service.Backend.Send(header[key], zmq.SNDMORE)
			}
			//  Send the rest of the message
			//fmt.Printf("\tForwarding to %s at %s: %s\n", service.Name, service.Address, message[1])
			service.Backend.Send(message[1], 0)
			//fmt.Println("\tDone")
			break
		}
	}
	return
}

func sendToClient(message string, header []string, title string, more zmq.Flag, frontend *zmq.Socket) {

	//fmt.Println("\t", title)
	for key := range header {
		//fmt.Printf("\tSending to client: %s\n", header[key])
		frontend.Send(header[key], zmq.SNDMORE)
	}
	//fmt.Printf("\tSending to client: %s\n", message)
	frontend.Send(message, more)
	//fmt.Println("\tDone")
}
