package multireadlist

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"dana-tech.com/wbw/logs"
)

const (
	ReadFromNew = iota
	ReadFromOld
)

func clean(data interface{}) int {
	logs.Logger.Warnf("one of reader read too slow")
	return 1
}

var senty Element

func init() {
	senty.valuehandle = clean
}

var (
	NoMoreElement = &senty
)

//一写多重复读队列
type MultiReadList struct {
	//前置头,并不存储数据,只有next是指向header的数据.
	preheader Element
	last      *Element
	mutex     sync.Mutex
	length    int32
	//强制回收的长度,设为1000吧
	maxele    int32
	minele    int32
	destroyed bool
}

func NewMultiReadList(buffersizemin int32, buffersizemax int32) *MultiReadList {

	mlist := new(MultiReadList)
	mlist.minele = buffersizemin
	mlist.maxele = buffersizemax
	go mlist.clean()

	return mlist
}
func (ml *MultiReadList) Refesh() {
	ml.mutex.Lock()
	defer ml.mutex.Unlock()
	if ml.destroyed {
		go ml.clean()
		ml.destroyed = false
	}
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
		} else {
			logs.Logger.Debugf("buffer clean over %d", ml.length)
		}

	}()

	for !ml.destroyed || ml.length > 0 {
		//每秒检测一次
		time.Sleep(time.Second)
		if ml.length <= ml.minele && !ml.destroyed {
			//长度小于20 则不进行垃圾处理
			continue
		} else {
			//开始回收
			for {
				// last handle with mutex for safe
				ml.mutex.Lock()

				if ml.last == nil {
					logs.Logger.Warnf("last nil length %d", ml.length)
					//清理完了,退出
					break
				}

				if ml.last.ref == 0 {
					last := ml.last
					//检出前一个节点的后一个节点的指针
					if last.pre != nil {
						last.pre.next = nil
					}
					//更新最后一个记录指针
					ml.last = last.pre

					last.Clean()
					logs.Logger.Debugf("clean length %d", ml.length-1)
					putElement(last)
					atomic.AddInt32(&ml.length, -1)
					ml.mutex.Unlock()
				} else {
					logs.Logger.Debugf("clean last ref %d list len %d", ml.last.ref, ml.length)

					if ml.length > ml.maxele {
						logs.Logger.Debugf("list len bigger than maxlen")
						//长度超出最大长度强制回收.
						//next置为nomoreelement.
						last := ml.last
						//检出前一个节点的后一个节点的指针
						if last.pre != nil {
							last.pre.next = nil
							logs.Logger.Debugf("list too long ,last.pre not nil")
						}

						//更新最后一个记录指针
						ml.last = last.pre

						//将last的pre置为nomoreele
						atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&last.pre)), unsafe.Pointer(NoMoreElement))

						atomic.AddInt32(&ml.length, -1)
						ml.mutex.Unlock()
					} else {
						ml.mutex.Unlock()
						break
					}

				}

				if ml.length < ml.minele && !ml.destroyed {
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

func (ml *MultiReadList) getTail() *Element {

	var ele *Element
	ml.mutex.Lock()
	defer ml.mutex.Unlock()
	elepointer := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&ml.last)))
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
	readpos int
	//close usage
	mutex sync.Mutex
}

//创建一个reader
func (mr *MultiListReader) InitMultiListReader(ml *MultiReadList) {
	mr.ml = ml
}
func (mr *MultiListReader) Close() {

	if mr.element != nil {
		mr.element.releaseRef()
		mr.element = nil
	}

}

func (mr *MultiListReader) SetReadAttribute(attr int) {
	mr.readpos = attr
}

//设置从最老的数据还是读取.
func (mr *MultiListReader) SetReadFromOld() {
	mr.element = mr.ml.getTail()
}

func (mr *MultiListReader) HasMoreData() bool {
	if mr.element == nil {
		return false
	}
	elepointer := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&mr.element.pre)))
	ele := (*Element)(elepointer)
	if ele == nil {
		return false
	} else {
		return true
	}

}

//默认从最新的数据读取
func (mr *MultiListReader) Read() *Element {

	if mr.element == nil {
		if mr.readpos == ReadFromOld {
			logs.Logger.Debugf("read tail %d", mr.ml.length)
			mr.element = mr.ml.getTail()
		} else {
			logs.Logger.Debugf("read header")
			mr.element = mr.ml.getHeader()
		}
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
