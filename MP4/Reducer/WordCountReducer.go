package main

import (
	"fmt"
	"os"
	"bufio"
	"strconv"

	Config "../Config"
)

//General format as: key : [val]\n
func PostProcess(wordMap map[string]int) string {
	res := ""

	for key, val := range wordMap {
		res += key + ": " + "[" + strconv.Itoa(val) + "]" + "\n"
	}

	return res
}


func main() {
	var output map[string]int
	output = make(map[string]int)

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
		parsedLine := Config.ParseString(scanner.Text())
		key := parsedLine[0]
		value, _ := strconv.Atoi(parsedLine[1])

		oldvalue, ok := output[key]
		if !ok {
			output[key] = value
		} else {
			output[key] = oldvalue + value
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Println(err)
	}

	res := PostProcess(output)

	fmt.Fprintf(os.Stdout, res)
}
