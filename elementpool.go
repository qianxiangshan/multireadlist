package multireadlist

import (
	"sync"
)

var pool sync.Pool

func init() {
	pool.New = func() interface{} {
		return new(Element)
	}
}

func getElement() *Element {
	return pool.Get().(*Element)
}

func putElement(ele *Element) {
	pool.Put(ele)
}
