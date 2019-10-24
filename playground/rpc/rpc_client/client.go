package main

import (
	"log"
	"net/rpc"
	"fmt"
)


//In practice, share the type between server and client
type Item struct {
	Title string
	Body string
}


func main() {
	var reply Item
	var db []Item

	client, err := rpc.DialHTTP("tcp", "localhost:4040")

	if err != nil {
		log.Fatal("Connection error: ", err)
	}
	
	a := Item{"first", "a test item"}
	b := Item{"second", "a second item"}
	c := Item{"third", "a third item"}

	client.Call("API.AddItem", a, &reply)
	client.Call("API.AddItem", b, &reply)
	client.Call("API.AddItem", c, &reply)
	client.Call("API.GetDB", "", &db)

	fmt.Println("database: ", db)
}
