package main

import (
	"fmt"
	"time"
)

var testChan chan int = make(chan int)
var variableChan chan bool = make(chan bool)
var fromTest2 chan bool = make(chan bool)
var fromTest3 chan bool = make(chan bool)

func main() {
	go test3()
	go test2()
	go test()

	myslice := []int{1,2,3,4,5,6}

	for _, n := range(myslice) {
		fmt.Println("before put")
		testChan <- n
		fmt.Println("after put")
	}

	testChan <- 0
	testChan <- 0
	testChan <- 0
	go test4(<-variableChan, <-fromTest2, <-fromTest3)

	time.Sleep(20*time.Second)
}

func test4(b bool, c bool, d bool){
	fmt.Println("test4:", b)
}

func test() {
	for{
		n := <-testChan
		if n != 0{
			fmt.Println("test:",n)
			time.Sleep(2*time.Second)
		}else {
			variableChan<-true
			return
		}
	}
}

func test2() {
	for{
		n := <-testChan
		if n != 0{
			fmt.Println("test2:",n)
			time.Sleep(2*time.Second)
		}else {
			fromTest2 <-true
			return
		}
	}

}
func test3() {
	for{
		n := <-testChan
		if n != 0{
			fmt.Println("test3:",n)
			time.Sleep(2*time.Second)
		}else {
			fromTest3<-true
			return
		}
	}

}

