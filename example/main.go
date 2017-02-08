package main

import (
	"fmt"
	"math/rand"
	//sync"
	//	"sync/atomic"
	"time"
	//	"unsafe"

	"dana-tech.com/go/multireadlist"
)

func cleanvalue(value interface{}) {
	fmt.Println("clean", value)
}

var index int = 1

var list *multireadlist.MultiReadList = multireadlist.NewMultiReadList()

func write() {
	for {

		ele := multireadlist.NewElement(index, cleanvalue)

		list.PutHeader(ele)
		time.Sleep(time.Millisecond * 10)
		index++
	}
}

func read() {
	fmt.Println("new reader")
	var randomeid int64 = time.Now().UnixNano()
	var reader multireadlist.MultiListReader
	reader.NewMultiListReader(list)
	defer reader.Close()
	fmt.Println(reader)
	var index int

	var last int
	var ele *multireadlist.Element
	for {
		ele = reader.Read()
		if ele == nil {
			time.Sleep(time.Millisecond * 50)

			continue
		} else {
			if last == 0 {
				last = ele.GetValue().(int) - 1
			}

			fmt.Println(randomeid, ele.GetValue().(int))
			if ele.GetValue().(int) != last+1 {
				panic("seq not correct")
			} else {
				last = ele.GetValue().(int)
			}
			if index > 1000 {
				fmt.Println("exit")
				break
			}
			index++
		}
	}

}

func randomnewreader() {
	var all int = 20
	for {
		time.Sleep(time.Second * time.Duration(rand.Int()%20))
		go read()
		if all > 20 {
			break
		}
		all++
	}
}

func main() {

	go write()
	go read()
	randomnewreader()
	select {}

}
