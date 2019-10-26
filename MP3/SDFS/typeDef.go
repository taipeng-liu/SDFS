package sdfs

type Block struct {
	Idx      int
	Size     int
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
}

type GetResponse struct {
	Fileinfo  FileInfo
	Block     Block
}

type DeleteRequest struct {
	Filename  string
}

type DeleteResponse struct {
	Err       error
	Statement string
}
