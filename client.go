package main

import (
	"fmt"
	"log"
	"net/rpc"
)

type Args struct {
	A int
	B int
}

func main() {
	client, err := rpc.Dial("tcp", "localhost:8080")
	if err != nil {
		log.Fatal("failed to connect:", err)
	}
	defer client.Close()

	args := Args{
		A: 10,
		B: 20,
	}

	var reply int

	err = client.Call("Calculator.Add", args, &reply)
	if err != nil {
		log.Fatal("RPC call failed:", err)
	}

	fmt.Println("Result:", reply)
}