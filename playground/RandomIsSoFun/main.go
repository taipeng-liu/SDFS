package a

import (
	"fmt"
	"math/rand"
	"time"
)

func HaveSomeLucky(){
	var luck int = rand.Intn(100)
	if luck > 98 {
		fmt.Println("WOW!!!You're super lucky!")
	}else if luck > 89 {
		fmt.Println("Lukcy day!")
	}else if luck > 79 {
		fmt.Println("Have a good day!")
	}else if luck > 59 {
		fmt.Println("Well, normal day")
	}else if luck > 30 {
		fmt.Println("Anyway, it's not so bad")
	}else if luck > 2 {
		fmt.Println("Emmmm...Be careful my boy...")
	}else {
		fmt.Println("Don't go outside!!!")
	}

}

func main() {

	var count [10]int
	for i:=0; i < 100000; i++{
		rand.Seed(time.Now().UnixNano())
		count[rand.Intn(10)]++
	}
	fmt.Println(count)
}
