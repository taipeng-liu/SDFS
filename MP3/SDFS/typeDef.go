package sdfs

type FindRequest struct {
	Filename string
}

type FindResponse struct {
	DatanodeList []string
}

type InsertRequest struct {
	Filename string
	NodeID   string
}

type InsertResponse struct {
	DatanodeList []string
}

type PermissionRequest struct {
	Filename  string
	MustWrite bool
}

type ReReplicaRequest struct {
	Filename     string
	DatanodeList []string
}

type PutRequest struct {
	Filename string
	Eof      bool
	Offset   int64
	Content  []byte
	Hostname string
}

type PutResponse struct {
	Response string
}

type GetRequest struct {
	Filename string
	Offset   int64
	Size     int
}

type GetResponse struct {
	Eof     bool
	Content []byte
}

type DeleteRequest struct {
	Filename string
}

type DeleteResponse struct {
	Err       error
	Statement string
}
