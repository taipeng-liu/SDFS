package sdfs

import(
	"fmt"
	"log"
	"net"
	"net/rpc"
	"net/http"
)

type Datanode struct{
	NamenodeAddr string
}

//////////////////////////////////////Methods///////////////////////////////////

func (d *Datanode) Put (req PutRequest, resp *PutResponse) error{
	//TODO
	return nil
}

func (d *Datanode) Get (req GetRequest, resp *GetResponse) error{
	//TODO
	return nil
}

func (d *Datanode) Delete (req DeleteRequest, resp *DeleteResponse) error{
	//TODO
	return nil
}

/////////////////////////////////////////Functions///////////////////////////////

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

