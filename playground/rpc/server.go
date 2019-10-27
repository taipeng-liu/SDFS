package main

import (
	//"fmt"
	"log"
	"net"
	"net/rpc"
	"net/http"
	"time"
)

type Item struct {
	Title string
	Body string
}

type API int

var database []Item

func (a *API) GetDB(title string, reply *[]Item) error{
	*reply = database
	return nil
}


func (a *API) GetByName(title string, reply *Item) error{
	var getItem Item

	for _, val := range database{
		if val.Title == title {
			getItem = val
		}
	}

	*reply = getItem

	return nil
}


func (a *API) AddItem(item Item, reply *Item) error {
	database = append(database, item)
	*reply = item
	return nil
}

func (a *API) EditItem(edit Item, reply *Item) error {
	var changed Item

	for idx, val := range database {
		if val.Title == edit.Title {
			database[idx] = Item{edit.Title, edit.Body}
			changed = database[idx]
		}
	}

	*reply = changed

	return nil
}

func (a *API) DeleteItem(item Item, reply *Item) error {
	var del Item

	for idx, val := range database {
		if val.Title == item.Title && val.Body == item.Body {
			database = append(database[:idx], database[idx+1:]...)
			del = item
			break
		}
	}
	
	*reply = del

	return nil
}

func RunServer(port string) {
	var api = new(API)

	server := rpc.NewServer()

	err := server.Register(api)
	if err != nil {
		log.Fatal("error registering API", err)
	}

	oldMux := http.DefaultServeMux
	mux := http.NewServeMux()
	http.DefaultServeMux = mux



	server.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)

	http.DefaultServeMux = oldMux

	listener, err := net.Listen("tcp", ":" + port)
	if err != nil {
		panic(err)
	}

	log.Printf("serving rpc on port %d", 4041)
	http.Serve(listener, mux)
}

func main() {
	go RunServer("4040")
	go RunServer("4041")
	time.Sleep(10*time.Second)
}
