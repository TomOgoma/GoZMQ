



package main

import (
	
		generate "github.com/jacobsa/oglemock/generate"
	
		io "io"
	
		log "log"
	
		os "os"
	
		reflect "reflect"
	
)

func getTypeForPtr(ptr interface{}) reflect.Type {
	return reflect.TypeOf(ptr).Elem()
}

func main() {
	// Reduce noise in logging output.
	log.SetFlags(0)

	interfaces := []reflect.Type{
		
			getTypeForPtr((*io.Reader)(nil)),
		
			getTypeForPtr((*io.Writer)(nil)),
		
	}

	err := generate.GenerateMockSource(os.Stdout, "mock_io", interfaces)
	if err != nil {
		log.Fatalf("Error generating mock source: %v", err)
	}
}
