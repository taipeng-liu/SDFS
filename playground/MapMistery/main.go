package main

import (
	"fmt"
)


func main() {
	var mymap map[string][]string = make(map[string][]string)

	mymap["a"] = []string{"hello"}

	fmt.Println(mymap)

	if _, ok := mymap["a"]; ok {
		mymap["a"] = append(mymap["a"], "world")
	}
	fmt.Println(mymap)
}
