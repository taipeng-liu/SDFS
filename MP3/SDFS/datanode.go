package sdfs

import(
	"fmt"
	"log"
	"net"
	"net/rpc"
	"net/http"
	"os"
	"io"

	Config "../Config"
)

type Datanode struct{
	NamenodeAddr string
}



/////////////////////////////////////////Functions////////////////////////////////

func RunDatanodeServer () {
	var datanode = new(Datanode)
	datanodeServer := rpc.NewServer()

	err := datanodeServer.Register(datanode)
	if err != nil {
		log.Fatal("Register(datanode) error:", err)
	}

	//======For multiple servers=====
	oldMux := http.DefaultServeMux
	mux := http.NewServeMux()
	http.DefaultServeMux = mux
	//===============================

	datanodeServer.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)

	//=======For multiple servers=====
	http.DefaultServeMux = oldMux
	//================================

	listener, err := net.Listen("tcp", ":" + Config.DatanodePort)
	if err != nil {
		log.Fatal("Listen error", err)
	}
	
	fmt.Printf("===RunDatanodeServer: Listen on port %s\n", Config.DatanodePort)
	err = http.Serve(listener, mux)
	if err != nil {
		log.Fatal("Serve(listener, nil) error: ", err)
	}
}

//////////////////////////////////////Methods///////////////////////////////////

func (d *Datanode) GetNamenodeAddr(req string, resp *string) error{
	//No namenode right now, start a selection process
	if d.NamenodeAddr == "" {
		d.NamenodeAddr = StartElection()
	}
	
	*resp = d.NamenodeAddr
	return nil
}

func (d *Datanode) Put(req PutRequest, resp *PutResponse) error{
	sdfsfilepath := Config.SdfsfileDir + "/" + req.Filename

	sdfsfile, err := os.OpenFile(sdfsfilepath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0755)
	if err != nil {
		log.Println("os.OpenFile() error")
		resp.Err = err
		return err
	}

	if _, err = sdfsfile.WriteAt(req.Block.Content, int64(req.Block.Idx) * req.Block.Size); err != nil {
		log.Println("sdfsfile.WriteAt() error")
		resp.Err = err
		return err
	}
	return nil
}

func (d *Datanode) Get(req GetRequest, resp *GetResponse) error{
	sdfsfilepath := Config.SdfsfileDir + "/" + req.Filename

	//Open file
	sdfsfile, err := os.Open(sdfsfilepath)
	if err != nil {
		log.Printf("os.Open() can't open file %s\n", sdfsfilepath)
		return err
	}
	defer sdfsfile.Close()

	//Read file into resp
	buf := make([]byte, req.Size)

	n, err := sdfsfile.ReadAt(buf, req.Offset)
	if err != nil {
		if err != io.EOF{
			return err
		} else {
			resp.Eof = true
		}
	}
	
	resp.Content = buf[:n] //TODO: test n or n + 1

	return nil
}

func (d *Datanode) Delete(req DeleteRequest, resp *DeleteResponse) error{
	sdfsfilepath := Config.SdfsfileDir + "/" + req.Filename

	if err := os.Remove(sdfsfilepath); err != nil{
		return err
	}
	return nil
}

func StartElection() string{
	//TODO modify it
	return "fa19-cs425-g73-01.cs.illinois.edu"
}

