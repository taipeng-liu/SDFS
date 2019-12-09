package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

var valueList []string 

//filename_prefix_K
func gatherLink(content string) {

	var val = ""
	var startIdx = 0
	//Find value (Todo: make sure [ will not appear at end of line)
	for idx, c := range content {
		if c == '[' {
			startIdx = idx + 1
		} else if c == ']' {
			val = content[startIdx, idx]
			valueList = append(valueList, val)
		}
	}
}

func postProcess(value string) {


}

func main {

	fileDir := os.Args[1]

	//How do we know the key?

	data, fileErr := os.Open(fileDir)
	if fileErr != nil {
		fmt.Printf("os.Open() error: Can't open file %s\n", fileDir)
		return
	}
	defer data.Close()

	var scanner = bufio.NewScanner(data)

	for scanner.Scan() {
		//fmt.Println(scanner.Text())
		gatherLink(scanner.Text())
	}

	// res := PostProcess(MapperResult)

	// fmt.Fprint(os.Stdout, res)

}