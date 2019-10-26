package sdfs

import(
	"fmt"
	"log"
	"net"
	"net/rpc"
	"net/http"
	"os"

	Config "../Config"
)

type Datanode struct{
	NamenodeAddr string
	SdfsfileList []string
}

//////////////////////////////////////Methods///////////////////////////////////

func (d *Datanode) GetNamenodeAddr(req string, resp *string) error{
	*resp = d.NamenodeAddr
	return nil
}

func (d *Datanode) GetSdfsfileList(req string, resp *[]string) error{
	*resp = d.SdfsfileList
	return nil
}

func (d *Datanode) Put(req PutRequest, resp *PutResponse) error{
	sdfsfilepath := Config.GetSdfsfilePath(req.FileInfo.Filename)

	sdfsfile, err := os.OpenFile(sdfsfilepath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0755)
	if err != nil {
		log.Println("os.OpenFile() error")
		resp.Err = err
		return err
	}

	if _, err := sdfsfile.WriteAt(req.Block.Content, int64(req.Block.Idx) * req.Block.Size); err != nil {
		log.Println("sdfsfile.WriteAt() error")
		resp.Err = err
		return err
	}
	return nil
}

func (d *Datanode) Get(req GetRequest, resp *GetResponse) error{
	//TODO
	return nil
}

func (d *Datanode) Delete(req DeleteRequest, resp *DeleteResponse) error{
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

