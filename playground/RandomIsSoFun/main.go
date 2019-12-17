package main

import (
	"fmt"
	"math/rand"
	"time"
)

func HaveSomeLucky(){
	var luck int = rand.Intn(100)
	if luck > 98 {
		fmt.Println("SSSSSSSSSSSSS")
	}else if luck > 89 {
		fmt.Println("SSS")
	}else if luck > 79 {
		fmt.Println("SS")
	}else if luck > 59 {
		fmt.Println("S")
	}else if luck > 30 {
		fmt.Println("R")
	}else if luck > 2 {
		fmt.Println("N")
	}else {
		fmt.Println("Nothing")
	}

}
//EPmap = event-posibility map
func Roll(EPmap map[string]float64, t int) []string{

	//function defined type
	type event struct {
		Name string
		Range float64
	}

	//event accumulated posibility map
	var eventList []*event
	var total_posibility float64

	for name, posibility := range EPmap {
		if posibility < 0 {
			fmt.Println("Please give me positive number!")
			return []string{}
		}
		total_posibility += posibility
		e := event{name, total_posibility}
		eventList = append(eventList, &e)
	}

	//Roll t times
	results := make([]string, t)

	for i := 0; i < t; i++ {
		//Get a random number from 1 to 10,000
		rand.Seed(time.Now().UnixNano())
		var res float64 = total_posibility * float64(rand.Intn(10000))/10000

		//Search event
		for _, event := range eventList {
			if res < event.Range {
				results[i] = event.Name
				break
			}
		}
	}

	return results
}

func ReduceList(l []string) map[string]int {
	res := make(map[string]int)

	for _, item := range l {
		if _, ok := res[item]; ok{
			res[item]++
		}else {
			res[item] = 1
		}
	}

	return res
}

func main() {
	mymap := map[string]float64{"SSR" : 2.5, "SR" : 25, "R" : 62.8, "SSR-Card": 0.5, "SR-Card" : 3, "R-Card" : 6.2}

	res := Roll(mymap, 100)
	//fmt.Println(res)

	resmap := ReduceList(res)
	fmt.Println(resmap)
}
