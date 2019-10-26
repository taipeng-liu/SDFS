package main

import(
	"fmt"
	"os"
	"io"
)

var BUFFER_SIZE int64

const (
	s = "vm1.log"
	d = "test.log"
)

func main() {
	BUFFER_SIZE = 512

	sourceFileStat, err := os.Stat(s)
	if err != nil {
		fmt.Println("Stat(s) error")
		return
	}

	if !sourceFileStat.Mode().IsRegular() {
		fmt.Println("is not regular")
	}

	source, err := os.Open(s)
	//check err
	defer source.Close()

	_, err = os.Stat(d)
	//check err

	destination, err := os.OpenFile(d, os.O_RDWR|os.O_CREATE, 0755)

	defer destination.Close()

	buf := make([]byte, BUFFER_SIZE)
	for {
		n, err := source.Read(buf)
		if err != nil && err != io.EOF{
			return
		}
		if n == 0 {
			return
		}

		if _, err := destination.Write(buf[:n]); err != nil {
			return
		}
	}
	return 
}
