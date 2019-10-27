package sdfs

import(
	"fmt"
	"log"
	"net"
	"net/rpc"
	"net/http"

	Config "../Config"
	//Mem "../Membership"
)

type Namenode struct{
	Filemap map[string][]string 
}

//////////////////////////////////////////Functions////////////////////////////////////////////

func RunNamenodeServer() {
	var namenode = new(Namenode)
	namenodeServer := rpc.NewServer()

	err := namenodeServer.Register(namenode)
	if err != nil {
		log.Fatal("Register(namenode) error:", err)
	}

	//======For multiple servers=====
	oldMux := http.DefaultServeMux
	mux := http.NewServeMux()
	http.DefaultServeMux = mux
	//===============================

	namenodeServer.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)
	
	//=======For multiple servers=====
	http.DefaultServeMux = oldMux
	//================================

	listener, err := net.Listen("tcp", ":" + Config.NamenodePort)
	if err != nil {
		log.Fatal("Listen error", err)
	}
	
	fmt.Printf("===RunNamenodeServer: Listen on port %s\n", Config.NamenodePort)
	err = http.Serve(listener, mux)
	if err != nil {
		log.Fatal("Serve(listener, nil) error: ", err)
	}
}

///////////////////////////////////RPC Methods////////////////////////////
/*
	Given a request, return response containing a list of all Datanodes who has the file
*/

func (n *Namenode) GetDatanodeList(req FindRequest, resp *FindResponse) error {
	resp.DatanodeList = []string{"fa19-cs425-g73-01.cs.illinois.edu"}
	return nil
}
func (n *Namenode) InsertFile(req InsertRequest, resp *InsertResponse) error {
	resp.DatanodeList = []string{"fa19-cs425-g73-01.cs.illinois.edu"}
	return nil
}

/*
func (n *Namenode) GetDatanodeList(req *FindRequest, resp *FindResponse) error {
	if val, ok := n.Filemap[FindRequest.Filename]; ok {
		return n.Filemap[FindRequest.Filename]
	} 
	return nil
}
*/



/*
	Figure out the value of Filamap[sdfsfilename] (use Mmonitoring List AKA MemHBList)
	Insert pair (sdfsfilename, datanodeList) into Filemap
	Send datanodeList back to InsertResponse
*/

/*
func (n *Namenode) InsertFile(req *InsertRequest, resp *InsertResponse) error {
	
	datanodeList := Mem.GetListByRelateIndex([]int{-2,-1,1}, InsertRequest.LocalID)

	for i, datanodeID := range datanodeList {
		n.Filemap[InsertRequest.Filename] = append(n.Filemap[InsertRequest.Filename], datanodeID) 
	}
	// n.Filemap[InsertRequest.Filename] = datanodeList

	return datanodeList
}
*/

///////////////////////////////////Member Function////////////////////////////

//***Function: Simply add a new entry into Filemap, return added key and value

/*
func (n *Namenode) Add(nodeID string, sdfsfilename string) {
	return
}

func (n *Namenode) Delete() {
	//TODO
	//delete an item from filemap by key
	//return deleted key and value
	return
}

func (n *Namenode) Find() {
	//TODO
	//find value by key
	//return value if found or nil
	return
}

func (n *Namenode) Update() {
	//TODO
	//modify value by key
	//return modified key and value
	return
}


*/
