package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	// "strings"
)

//General format as: key : [val]\n

func wordCount(content string) int {
	var cnt = 0
	var startIdx = 0
	//Find value (Todo: make sure [ will not appear at end of line)
	for idx, c := range content {
		if c == '[' {
			startIdx = idx + 1
		} else if c == ']' {
			val, _ := strconv.Atoi(content[startIdx:idx])
			cnt += val
		}
	}
	return cnt
}

// func postProcess() string {
// 	res := ""

// 	for idx, val := range valueList {
// 		if idx != len(valueList) - 1 {
// 			res += val + ","
// 		} else {
// 			res += val + "\n"
// 		}
// 	}

// 	fmt.Println(res)
// 	return res
// }

func main() {
	var totalCnt = 0

	//Read from argument
	filepath := os.Args[1]

	//Open file
	file, err := os.Open(filepath)
	if err != nil {
		fmt.Printf("os.Open() can't open file %s\n", filepath)
		return
	}
	defer file.Close()

	//Read file line by line
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		//Parse each line
		totalCnt += wordCount(scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		fmt.Println(err)
	}

	// res := PostProcess(output)

	fmt.Fprintf(os.Stdout, strconv.Itoa(totalCnt))
}
