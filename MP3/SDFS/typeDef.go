package sdfs

type Block struct {
	Idx      int
	Size     int64
	Content  []byte
}

type FileInfo struct {
	Filename    string
	Filesize    int
	Totalblock  int
}

type FindRequest struct {
	Filename string
}

type FindResponse struct {
	DatanodeList []string
}

type InsertRequest struct {
	Filename string
	LocalID	string
}

type InsertResponse struct {
	DatanodeList []string
}

type PutRequest struct {
	Fileinfo  FileInfo
	Block     Block
}

type PutResponse struct {
	Err       error
	Statement string
}

type GetRequest struct {
	Filename  string
	Offset    int64
	Size	  int
}

type GetResponse struct {
	Eof     bool
	Content []byte
}

type DeleteRequest struct {
	Filename  string
}

type DeleteResponse struct {
	Err       error
	Statement string
}
