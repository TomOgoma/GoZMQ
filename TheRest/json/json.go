package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
)

type Message struct {
	Name string
	Body string
	Time int64
}

var messages = make(map[string]Message)
var messagesd = make(map[string]Message)

func main() {
	// Encoding
	messages["Alice"] = Message{"Alice", "Hello", 1294706395881547000}
	messages["Bob"] = Message{"Bob", "World", 1294706395881547000}

	var datab, b []byte
	var err error
	for key := range messages {
		b, err = json.Marshal(messages[key])
		if err != nil {
			log.Println("Error encoding ", key, " to JSON")
			panic(err)
		}
		datab = append(datab, append(b, byte('\n'))...)
	}
	//write to file
	ioutil.WriteFile("services.json", datab, os.ModeSetgid)

	var m Message
	filepointer, err := os.Open("services.json")
	if err != nil {
		log.Println(err)
		return
	}
	defer filepointer.Close()
	reader := bufio.NewReader(filepointer)
	line, err := reader.ReadBytes('\n')
	for err == nil {
		///*Decoding
		err = json.Unmarshal(line, &m)
		if err != nil {
			log.Println("Error decoding from json")
			//return
		}
		messagesd[m.Name] = m
		line, err = reader.ReadBytes('\n')
	}
	if err != nil {
		log.Println(err)
		//return
	}
	for key := range messagesd {
		fmt.Println(messagesd[key])
	}
}
