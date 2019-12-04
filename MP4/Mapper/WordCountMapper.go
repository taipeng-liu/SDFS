package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	//"io/ioutil"
)

//TODO Exclude ","
func Parse(cmd string) []string {
	cmd = strings.Join(strings.Fields(cmd), " ")
	return strings.Split(cmd, " ")
}

//Count each word in a word list
func countFromWordList(wordList []string, wordMap map[string]int) {
	//Iterate word list
	for _, word := range wordList {
		if _, ok := wordMap[word]; ok {
			//If the word exists in word map
			wordMap[word]++
		} else {
			//Not exists in word map
			wordMap[word] = 1
		}
	}
}

//General format as: key : [val]\n
func PostProcess(wordMap map[string]int) string {
	res := ""

	for key, val := range wordMap {
		res += key + ": " + "[" + strconv.Itoa(val) + "]" + "\n"
	}

	return res
}

func main() {
	var wordMap map[string]int
	wordMap = make(map[string]int)

	//Read from arguments
	filepath := os.Args[1]
	//prefix := os.Args[2]

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
		wordList := Parse(scanner.Text())

		//Count each word in the line
		countFromWordList(wordList, wordMap)
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error")
	}

	res := PostProcess(wordMap)

	// b, err := json.Marshal(wordMap)
	// if err != nil {
	// 	fmt.Println(err)
	// }

	// s := string(b)

	fmt.Fprintf(os.Stdout, res)
	//helper.WriteWordMapToJsonFile(wordMap, prefix)
}
