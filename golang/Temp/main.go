package main

import (
	"fmt"
)

func reverseList(myList []int) []int {
	for i, j := 0, len(myList)-1; i < j; i, j = i+1, j-1 {
		myList[i], myList[j] = myList[j], myList[i]
	}

	return myList
}

func foo1(s map[string]string, mval string) string{
	for key, val := range s {
		if val == mval {
			delete(s, key)
			return val
		}
	}
	return ""
}

func foo2() string{
	return "hi from foo2"
}

func modifySlice(l []int) {
	if len(l) > 0 {
		a:=append(l[:1], l[2:]...)
		fmt.Println(a)
	}
}

func main() {
	myL := []int{1,2,3,4}

	modifySlice(myL)
	//myL = append(myL[:1], myL[2:]...)
	fmt.Println(myL)
}
