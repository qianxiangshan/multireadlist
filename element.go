package multireadlist

import (
	"sync/atomic"
)

type ValueClean func(interface{}) int

type Element struct {
	Seq         int
	ref         int32
	pre         *Element
	next        *Element
	value       interface{}
	valuehandle ValueClean
}

func NewElement(data interface{}, valueclen ValueClean) *Element {
	ele := getElement()
	ele.valuehandle = valueclen
	ele.value = data
	return ele
}

func (e *Element) Clean() int {
	tmp := e.valuehandle(e.value)
	e.ref = 0
	e.Seq = 0
	e.pre = nil
	e.next = nil
	e.value = nil
	return tmp
}

func (e *Element) GetValue() interface{} {
	return e.value
}

func (e *Element) PutValue(data interface{}) {
	e.value = data
}

func (e *Element) addRef() {
	atomic.AddInt32(&e.ref, 1)
}

func (e *Element) releaseRef() {
	atomic.AddInt32(&e.ref, -1)
}
