package sdfs

import(
)

type Namenode struct{
	Filemap map[string][]string
}

////////////////////////////////////////Methods////////////////////////////

func (n *Namenode) GetDatanodeList (req *FindRequest, resp *FindResponse) error {
	//TODO
	//Given a request, return response containing a list of all
	//Datanodes who has the file
	return nil
}

func (n *Namenode) InsertFile (req *InsertRequest, resp *InsertResponse) error {
	//TODO
	//Figure out the value of Filamap[sdfsfilename]
	//i.e. find datanodes who should save the file
	//Insert pair (sdfsfilename, datanodeList) into Filemap
	//Send datanodeList back to InsertResponse
	return nil
}


func (n *Namenode) Add() {
	//TODO
	//add a new item into filemap
	//return added key and value
}

func (n *Namenode) Delete() {
	//TODO
	//delete a item from filemap by key
	//return deleted key and value
}

func (n *Namenode) Find() {
	//TODO
	//find value by key
	//return value if found or nil
}

func (n *Namenode) Edit() {
	//TODO
	//modify value by key
	//return modified key and value
}


//////////////////////////////////////////Functions////////////////////////////////////////////

func RunNamenodeServer(Port string) {
	//TODO
}
