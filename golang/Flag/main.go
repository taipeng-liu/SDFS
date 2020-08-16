package main

import (
	"fmt"
	"flag"
)

func main() {
	var firstFlagVal = flag.Int("first", 0, "First flag")
	var secondFlagVal = flag.String("second", "Default", "Second flag")
	var thirdFlagVal = flag.Bool("third", false, "Third flag")

	flag.Parse()

	fmt.Println(*firstFlagVal)
	fmt.Println(*secondFlagVal)
	fmt.Println(*thirdFlagVal)
}
