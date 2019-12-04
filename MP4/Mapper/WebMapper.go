package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

var MapperResult map[string][]string = make(map[string][]string)

const JsonFileName = "webMap.json"

func parsePair(pair string) {
	res := strings.Split(pair, " ")
	if len(res) > 2 {
		fmt.Println("Data Error!")
	}
	src := res[0]
	tgt := res[1]

	MapperResult[tgt] = append(MapperResult[tgt], src)
}

//General format as: key : [val]\n
func PostProcess(wordMap map[string][]string) string {
	res := ""

	for key, list := range wordMap {
		res += key + ": " + "["
		for idx, val := range list {
			res += val
			if idx != len(list)-1 {
				res += "," + " "
			}
		}
		res += "]" + "\n"
	}

	return res
}

func main() {

	fileDir := os.Args[1]
	// prefix := os.Args[2]

	data, fileErr := os.Open(fileDir)
	if fileErr != nil {
		fmt.Println(fileErr)
		panic(fileErr)
	}
	defer data.Close()

	var scanner = bufio.NewScanner(data)

	for scanner.Scan() {
		//fmt.Println(scanner.Text())
		parsePair(scanner.Text())
	}

	res := PostProcess(MapperResult)

	// b, err := json.Marshal(MapperResult)
	// if err != nil {
	// 	fmt.Println(err)
	// }

	// s := string(b)

	fmt.Fprint(os.Stdout, res)
	// helper.WriteStringSliceMapToJson(MapperResult, prefix)
	// ioutil.WriteFile(JsonFileName, b, 0644)

}
