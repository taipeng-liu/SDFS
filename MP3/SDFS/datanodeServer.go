package sdfs

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"net/http"
)

func RunDatanodeServer (Port string) {
	var datanode = new(Datanode)

	err := rpc.Register(datanode)
	if err != nil {
		log.Fatal("Register(datanode) error:", err)
	}

	rpc.HandleHTTP()

	listener, err := net.Listen("tcp", ":" + Port)
	if err != nil {
		log.Fatal("Listen error", err)
	}
	
	fmt.Printf("===RunDatanodeServer: Listen on port %s\n", Port)
	err = http.Serve(listener, nil)
	if err != nil {
		log.Fatal("Serve(listener, nil) error: ", err)
	}
}
