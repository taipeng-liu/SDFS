package sdfs

import(
)

type Namenode int

var filemap map[Filename]NodeIDList

//Not comprehensively implemented!!


///////////////RPC functions////////////////////

func (n *Namenode) SearchDatanode (request *Request, response *Response) error {
	//TODO
	//Given a request, return response containing a list of all
	//Datanodes who has the file
	return nil
}


//////////////Map Operations////////////////////
func Add() {
	//add a new item into filemap
	//return added key and value
}

func Delete() {
	//delete a item from filemap by key
	//return deleted key and value
}

func Find() {
	//find value by key
	//return value if found or nil
}

func Edit() {
	//modify value by key
	//return modified key and value
}
