//
//  Request-reply client.
//  Connects REQ socket to tcp://localhost:5559
//  Sends "Hello" to server, expects "World" back
//

package main

import (
	zmq "github.com/pebbe/zmq4"

	"fmt"
	"math/rand"
	"time"
)

func main() {
	requester, _ := zmq.NewSocket(zmq.REQ)
	defer requester.Close()
	requester.Connect("tcp://localhost:5559")

	for request := 0; request < 100; request++ {

		//send message
		msg := "time:time"
		fmt.Println("\nSending Message ", request, ": ", msg, "...")
		requester.Send(msg, 0)

		//receive reply
		reply, _ := requester.Recv(0)
		fmt.Printf("\tReceived reply %d [%s]\n", request, reply)

		//Sleep for a second
		fmt.Printf("\tTime to rest :-)\n")
		time.Sleep(time.Duration(rand.Intn(1e3)) * time.Millisecond)
	}
}
