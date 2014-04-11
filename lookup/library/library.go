package msg

import (
	"errors"
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"log"
	"time"
)

const (
	//  Client constants for sending requests for service
	REQUEST_TIMEOUT = 2500 * time.Millisecond //  msecs, (> 1000!)
	REQUEST_RETRIES = 3                       //  Before we abandon

	//  Paranoid Pirate Protocol constants
	PPP_READY     = "\001" //  Signals worker is ready
	PPP_HEARTBEAT = "\002" //  Signals worker heartbeat
)

type Service struct {
	SID, Name, Address, Reply, Socket_desc, Heartbeat_state string
}

//  Interface through which all client requests are to be processed
type ProcessRequest func(string) (string, error)

func NewService(sid, name, address, reply, socket_desc string) Service {
	return Service{
		sid,
		name,
		address,
		reply,
		socket_desc,
		time.Now().String(),
	}
}

func ProcessHeartBeat(message string) (reply string, err error) {
	reply = PPP_READY
	return
}

func RecieveClientRequest(receiver *zmq.Socket, myservices map[string]ProcessRequest) (service_required ProcessRequest, message string, err error) {
	for count := 0; ; count++ {
		var request string
		request, err = receiver.Recv(0)
		if err != nil {
			err = errors.New(fmt.Sprintf("Error:Receive:%s", err))
			return
		}
		fmt.Println("\tCurrent: ", request)
		//  Retrieve message parts from the envelope
		//  The first part is either:
		//  a. A heartbeat request
		//  b. The service's SID
		//  The second part: the message
		if count == 0 {
			var isPresent bool
			service_required, isPresent = myservices[request]
			fmt.Printf("%s is present? %s", request, isPresent)
			if !isPresent {
				err = errors.New("Error:InvalidService")
				return
			}
		}
		if count == 1 {
			message = request
		}
		//  Check if there are more in envelope and deal with any errors
		var more bool
		more, err = receiver.GetRcvmore()
		if err != nil {
			err = errors.New(fmt.Sprintf("Error:Receive:%s", err))
			return
		}
		if !more {
			return
		}
	}
}

func SendToClient(signature, error_status, message string, frontend *zmq.Socket) {
	fmt.Printf("\tSending signature to client: %s\n", signature)
	frontend.Send(signature, zmq.SNDMORE)
	fmt.Printf("\tSending errorstatus to client: %s\n", error_status)
	frontend.Send(error_status, zmq.SNDMORE)
	fmt.Printf("\tSending message to client: %s\n", message)
	frontend.Send(message, 0)
}

//  Send a request to a service
func SendRequest(service Service, request, message string) (reply []string, err error) {
	//  Bind to service if not already done
	var requester *zmq.Socket
	fmt.Println("Connecting to '", service.Name, "'' at '", service.Address, "'...")
	requester, err = zmq.NewSocket(zmq.REQ)
	if err != nil {
		log.Println(err)
		return
	}
	requester.Connect(service.Address)

	poller := zmq.NewPoller()
	poller.Add(requester, zmq.POLLIN)

	retries_left := REQUEST_RETRIES
	request_timeout := REQUEST_TIMEOUT
	if request == PPP_HEARTBEAT {
		retries_left = 1
		request_timeout = 1500 * time.Millisecond
	}
	for retries_left > 0 {
		//  Send message
		//  The service required in first packet of envelope
		//  The message for given service in second packet of envelope
		//  Heartbeat messages only contain one message in envelope
		if request == PPP_HEARTBEAT {
			_, err = requester.Send(PPP_HEARTBEAT, 0)
		} else {
			_, err = requester.Send(request, zmq.SNDMORE)
			_, err = requester.Send(message, 0)
		}
		for expect_reply := true; expect_reply; {
			//  Poll socket for a reply, with timeout
			var sockets []zmq.Polled
			sockets, err = poller.Poll(request_timeout)
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
					//  Unpacking three parts from the message envelope
					//  The first part: the expected service reply signature
					//  The second part is either:
					//  a. error state (empty if no error) OR
					//  b. the heartbeat reply of the server
					//  The third part: the actual message
					if count == 0 && rep != service.Reply {
						err = errors.New("ServiceListOutdated:Bound to wrong service")
						retries_left = 0
						break
					}
					if count == 1 && rep != "" && rep != PPP_READY {
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
					if err == nil {
						err = errors.New("Error:TimeOut")
					}
					requester.Close()
					return
				} else {
					err = nil
					reply = nil
					fmt.Println("No response from server, retrying...")
					//  Old socket is confused; close it and open a new one
					requester.Close()
					requester, _ = zmq.NewSocket(zmq.REQ)
					requester.Connect(service.Address)
					// Recreate poller for new client
					poller = zmq.NewPoller()
					poller.Add(requester, zmq.POLLIN)
					//  Send request again, on the new socket
					if request == PPP_HEARTBEAT {
						_, err = requester.Send(PPP_HEARTBEAT, 0)
					} else {
						_, err = requester.Send(request, zmq.SNDMORE)
						_, err = requester.Send(message, 0)
					}
				}
			}
		}
	}
	fmt.Println("\tReceived: ", reply)
	//  Deall with invalid/distorted replies first
	//  Followed by heartbeat replies
	//  Then package reply for return to the function caller
	if len(reply) < 2 {
		err = errors.New(fmt.Sprintf("Error:UnexpectedReply:%s", err))
		requester.Close()
		return
	}
	if reply[1] == PPP_READY {
		reply = append(reply, reply[1])
	}
	reply = reply[2:]
	requester.Close()
	return
}
