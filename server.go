package main

import (
	"log"
	"net"
	"net/rpc"
)

type Args struct {
	A int
	B int
}

type Calculator struct{}

// RPC-compatible method requirements:
// - Exported method
// - Exported argument types
// - Exactly two arguments after receiver
// - Second argument must be a pointer
// - Returns error
func (c *Calculator) Add(args Args, reply *int) error {
	*reply = args.A + args.B
	return nil
}

func main() {
	calculator := new(Calculator)

	if err := rpc.Register(calculator); err != nil {
		log.Fatal("failed to register RPC service:", err)
	}

	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatal("failed to listen:", err)
	}

	log.Println("RPC server listening on :8080")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("failed to accept connection:", err)
			continue
		}

		go rpc.ServeConn(conn)
	}
}