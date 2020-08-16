package main

import "fmt"

type MapleJuice interface{
	Maple
	Juice
}

type Maple interface{
	Maple() string
}

type Juice interface{
	Juice() string
}

type WordCount struct{
	maple_word string
	juice_word string
}

func (wc WordCount) Maple() string{
	return wc.maple_word
}
func (wc WordCount) Juice() string{
	return wc.juice_word
}

type UrlConvert struct{
	input_url string
	output_url string
}

func (uc UrlConvert) Maple() string{
	return uc.input_url
}

func (uc UrlConvert) Juice() string{
	return uc.output_url
}

func main() {
	var m Maple
	//var j Juice
	var mj MapleJuice

	m = WordCount{"Input word", "Output word"}
	mj = UrlConvert{"www.input.com", "www.output.com"}

	if val, ok := m.(WordCount); ok{
		fmt.Println(val.Maple())
		fmt.Println(val.Juice())
	}

	if val, ok := mj.(UrlConvert); ok{
		fmt.Println(val.Maple())
		fmt.Println(val.Juice())
	}
}
