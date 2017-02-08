package multireadlist

import (
	"fmt"
	"runtime"
	"sync/atomic"
	"time"
	"unsafe"

	"dana-tech.com/wbw/logs"
)

//一写多重复读队列
type MultiReadList struct {
	//前置头,并不存储数据,只有next是指向header的数据.
	preheader Element
	last      *Element
	length    int32
	destroyed bool
}

func NewMultiReadList() *MultiReadList {

	mlist := new(MultiReadList)

	go mlist.clean()

	return mlist
}

func (ml *MultiReadList) clean() {
	defer func() {
		pani := recover()
		if pani != nil {
			var stack string
			for i := 1; ; i++ {
				_, file, line, ok := runtime.Caller(i)
				if !ok {
					break
				}
				stack = stack + fmt.Sprintln(file, line)
			}
			logs.Logger.Errorf(" panic %v error %s", pani, stack)
		}
		if !ml.destroyed {
			go ml.clean()
		}
	}()

	for !ml.destroyed || ml.length > 0 {
		//每秒检测一次
		time.Sleep(time.Second)
		if ml.length <= 20 && !ml.destroyed {
			//长度小于20 则不进行垃圾处理
			continue
		} else {
			//开始回收
			for {
				if ml.last.ref == 0 {
					last := ml.last
					//检出前一个节点的后一个节点的指针
					last.pre.next = nil
					//更新最后一个记录指针
					ml.last = last.pre
					eleseq := last.Seq

					valueseq := last.Clean()
					logs.Logger.Infof("clean ele seq %d ,length %d,value %d", eleseq, ml.length-1, valueseq)
					putElement(last)

					atomic.AddInt32(&ml.length, -1)

				} else {
					logs.Logger.Debugf("clean last ref", ml.last.ref)
					break
				}

				if ml.length < 20 {
					break
				}
			}
		}
	}
}

func (ml *MultiReadList) Destory() {
	ml.destroyed = true

}

func (ml *MultiReadList) PutHeader(ele *Element) {

	//向队列头插入数据,
	//判断队列否为空,只有初始化时才是空,之后都不可能是空
	//写入操作是序列化的所以不用使用同步
	if ml.preheader.next == nil {
		//为空
		ele.pre = nil
		ele.next = nil
		//添加头
		ml.preheader.next = ele
		ml.last = ele
	} else {
		//不为空,则向前添加一个
		ele.next = ml.preheader.next
		ele.pre = nil
		atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&ml.preheader.next.pre)), unsafe.Pointer(ele))
		//ml.preheader.next.pre = ele
		//挂在头上
		atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&ml.preheader.next)), unsafe.Pointer(ele))
		//		ml.preheader.next = ele
		//fmt.Println("not empty list put header")
	}

	atomic.AddInt32(&ml.length, 1)
	logs.Logger.Debugf("list len ", ml.length)
	return
}
func (ml *MultiReadList) getHeader() *Element {

	var ele *Element

	elepointer := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&ml.preheader.next)))
	ele = (*Element)(elepointer)
	//判断第一个元素是否为空,是空则表示当前没有数据
	if ele != nil {
		ele.addRef()
	}

	return ele
}

type MultiListReader struct {
	//当前持有的数据,每次只能持有一个数据
	element *Element
	ml      *MultiReadList
}

//创建一个reader
func (mr *MultiListReader) NewMultiListReader(ml *MultiReadList) {
	mr.ml = ml
}
func (mr *MultiListReader) Close() {
	if mr.element != nil {
		mr.element.releaseRef()
		mr.element = nil
	}

}

func (mr *MultiListReader) HasMoreData() bool {
	elepointer := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&mr.element.pre)))
	ele := (*Element)(elepointer)
	if ele == nil {
		return false
	} else {
		return true
	}

}
func (mr *MultiListReader) Read() *Element {

	if mr.element == nil {
		logs.Logger.Infof("read header")
		mr.element = mr.ml.getHeader()
		return mr.element
	}
	ele := mr.element
	//增持下一个
	elepointer := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&mr.element.pre)))
	next := (*Element)(elepointer)
	if next != nil {
		next.addRef()
		mr.element = next
		//减持当前的,如果没有最新的那么就不减持保持一个数据,要不然处理很麻烦,要重新getheader才行
		//没有最新的,目前采用定时检测吧,后续更改为信号量处理好了
		ele.releaseRef()

	}
	return next

}
