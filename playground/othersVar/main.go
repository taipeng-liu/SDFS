package main

import (
	"fmt"
	others "./othersDir"
)


func main() {
	others.SetOthersVar(6)

	fmt.Println(others.OthersVar)
}
