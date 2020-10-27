package pool

import (
	"errors"
	"reflect"
	"sync"
	"time"
	"unsafe"

	"github.com/wuyongjia/hashmap"
)

const (
	DEFAULT_LENGTH             = 20480 // default max size of the pool
	DEFAULT_TIMEOUT            = 1800  // default every 1800 seconds to recycle the status
	DEFAULT_EXPIRATION_COUNTER = 10    // it depends on the counter, expired data will be deleted
	CHAN_GET                   = 'g'
	CHAN_LOOP_CANCLE           = 'c'
	CHAN_RECYCLE_CANCLE        = 'c'
)

type Mode int

const (
	MODE_NONE  Mode = 1
	MODE_QUEUE Mode = 2
)

var (
	ErrorPoolFull             = errors.New("the pool is full")
	ErrorNewItemMustPointer   = errors.New("the new item must be a pointer")
	ErrorMustPointer          = errors.New("the item must be a pointer")
	ErrorLastBoundary         = errors.New("the cursor is now at the last boundary")
	ErrorItemNotExist         = errors.New("the item does not exist in the pool")
	ErrorItemHaveBeenRecycled = errors.New("the item may have been recycled")
)

type AllocFunc func() interface{}
type AllocWithIdFunc func(id uint64) interface{}

type UpdateFunc func(ptr interface{})

type Pool struct {
	id                   uint64
	mode                 Mode
	getop                chan byte
	getrt                chan interface{}
	geter                chan error
	putop                chan interface{}
	putid                chan uint64
	putrt                chan error
	updateop             chan interface{}
	updateid             chan uint64
	updatert             chan error
	loopCancel           chan byte
	recycleCancel        chan byte
	cursor               int
	length               int
	availablelist        []uint64
	tempList             []uint64
	listprt              *hashmap.HM
	recycleInterval      time.Duration
	timeout              int64
	maxExpirationCounter int
	isUseId              bool
	allocFunc            AllocFunc // must return pointer
	allocWithIdFunc      AllocWithIdFunc
	RecycleUpdateFunc    UpdateFunc
	lock                 *sync.RWMutex
}

type Item struct {
	ptr     interface{} // an instance pointer
	counter int         // counter is used to delete items in the pool
	at      int64       // activity timestamp
}

func New(length int, allocFunc AllocFunc) *Pool {
	if length <= 0 {
		length = DEFAULT_LENGTH
	}
	var pool = newPool(length)
	pool.isUseId = false
	pool.allocFunc = allocFunc
	pool.allocWithIdFunc = nil
	return pool
}

func NewWithId(length int, allocWithIdFunc AllocWithIdFunc) *Pool {
	if length <= 0 {
		length = DEFAULT_LENGTH
	}

	var pool = newPool(length)
	pool.isUseId = true
	pool.allocWithIdFunc = allocWithIdFunc
	pool.allocFunc = nil

	return pool
}

func newPool(length int) *Pool {
	var pool = &Pool{
		id:                   0,
		mode:                 MODE_NONE,
		getop:                make(chan byte, length),
		getrt:                make(chan interface{}),
		geter:                make(chan error),
		putop:                make(chan interface{}, length),
		putid:                make(chan uint64, length),
		putrt:                make(chan error),
		updateop:             make(chan interface{}, length),
		updateid:             make(chan uint64, length),
		updatert:             make(chan error),
		loopCancel:           make(chan byte),
		recycleCancel:        make(chan byte),
		cursor:               length - 1,
		length:               length,
		availablelist:        make([]uint64, length),
		tempList:             make([]uint64, length),
		listprt:              hashmap.New(length),
		recycleInterval:      DEFAULT_TIMEOUT * time.Second,
		timeout:              DEFAULT_TIMEOUT,
		maxExpirationCounter: DEFAULT_EXPIRATION_COUNTER,
		RecycleUpdateFunc:    nil,
		lock:                 &sync.RWMutex{},
	}
	return pool
}

func (p *Pool) EnableQueue() {
	p.mode = MODE_QUEUE
	p.loop()
}

func (p *Pool) DisableQueue() {
	p.mode = MODE_NONE
	p.loopCancel <- CHAN_LOOP_CANCLE
}

func (p *Pool) EnableRecycle() {
	p.recycleLoop()
}

func (p *Pool) DisableRecycle() {
	p.recycleCancel <- CHAN_RECYCLE_CANCLE
}

func (p *Pool) SetRecycleInterval(t time.Duration) {
	p.recycleInterval = t
}

func (p *Pool) SetTimeout(t int64) {
	p.timeout = t
}

func (p *Pool) SetMaxExpirationCounter(n int) {
	p.maxExpirationCounter = n
}

func (p *Pool) Get() (interface{}, error) {
	if p.mode == MODE_NONE {
		return p.getItem()
	} else {
		p.getop <- CHAN_GET
		return <-p.getrt, <-p.geter
	}
}

func (p *Pool) Put(ptr interface{}) error {
	if p.mode == MODE_NONE {
		return p.putItem(ptr, 0)
	} else {
		p.putop <- ptr
		p.putid <- 0
		return <-p.putrt
	}
}

func (p *Pool) PutWithId(ptr interface{}, id uint64) error {
	if p.mode == MODE_NONE {
		return p.putItem(ptr, id)
	} else {
		p.putop <- ptr
		p.putid <- id
		return <-p.putrt
	}
}

func (p *Pool) Update(ptr interface{}) error {
	if p.mode == MODE_NONE {
		return p.updateItem(ptr, 0)
	} else {
		p.updateop <- ptr
		p.updateid <- 0
		return <-p.updatert
	}
}

func (p *Pool) UpdateWithId(ptr interface{}, id uint64) error {
	if p.mode == MODE_NONE {
		return p.updateItem(ptr, id)
	} else {
		p.updateop <- ptr
		p.updateid <- id
		return <-p.updatert
	}
}

func (p *Pool) GetCursor() int {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.cursor
}

func (p *Pool) GetLength() int {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.length
}

func (p *Pool) GetInstanceCount() int {
	return p.listprt.GetCount()
}

func (p *Pool) getItem() (interface{}, error) {
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.cursor < 0 {
		return nil, ErrorPoolFull
	}

	var err error
	var value interface{}

	var ok bool
	var item *Item

	var id = p.availablelist[p.cursor]
	if id > 0 {
		value = p.listprt.Get(id)
		item, ok = value.(*Item)
		if ok == true && item != nil {
			item.counter = 0
			item.at = time.Now().Unix()
		} else {
			if p.isUseId == false {
				id, item, err = p.createItem()
			} else {
				id, item, err = p.createItemWithId()
			}
			if err != nil {
				return nil, err
			}
			p.listprt.Put(id, item)
		}
	} else {
		if p.isUseId == false {
			id, item, err = p.createItem()
		} else {
			id, item, err = p.createItemWithId()
		}
		if err != nil {
			return nil, err
		}
		p.listprt.Put(id, item)
	}

	p.availablelist[p.cursor] = 0
	p.cursor--

	return item.ptr, nil
}

func (p *Pool) createItem() (uint64, *Item, error) {
	var ptr = p.allocFunc()
	var rv = reflect.ValueOf(ptr)
	if rv.Kind() != reflect.Ptr {
		return 0, nil, ErrorNewItemMustPointer
	}
	return *(*uint64)(unsafe.Pointer(rv.Pointer())), &Item{ptr: ptr, counter: 0, at: time.Now().Unix()}, nil
}

func (p *Pool) createItemWithId() (uint64, *Item, error) {
	p.id++
	var ptr = p.allocWithIdFunc(p.id)
	var rv = reflect.ValueOf(ptr)
	if rv.Kind() != reflect.Ptr {
		return 0, nil, ErrorNewItemMustPointer
	}
	return p.id, &Item{ptr: ptr, counter: 0, at: time.Now().Unix()}, nil
}

func (p *Pool) putItem(ptr interface{}, id uint64) error {
	var rv = reflect.ValueOf(ptr)
	if rv.Kind() != reflect.Ptr {
		return ErrorMustPointer
	}

	if p.isUseId == false {
		id = *(*uint64)(unsafe.Pointer(rv.Pointer()))
	}

	p.lock.Lock()
	defer p.lock.Unlock()

	if (p.cursor + 1) >= p.length {
		return ErrorLastBoundary
	}

	var value = p.listprt.Get(id)
	if value == nil {
		return ErrorItemNotExist
	}

	var item, ok = value.(*Item)
	if ok == false || item == nil {
		return ErrorItemNotExist
	}

	if item.at == 0 {
		return ErrorItemHaveBeenRecycled
	}

	p.cursor++
	p.availablelist[p.cursor] = id
	item.at = 0

	return nil
}

func (p *Pool) updateItem(ptr interface{}, id uint64) error {
	var rv = reflect.ValueOf(ptr)
	if rv.Kind() != reflect.Ptr {
		return ErrorMustPointer
	}

	if p.isUseId == false {
		id = *(*uint64)(unsafe.Pointer(rv.Pointer()))
	}

	p.lock.Lock()
	defer p.lock.Unlock()

	var value = p.listprt.Get(id)
	if value == nil {
		return ErrorItemNotExist
	}

	var item, ok = value.(*Item)
	if ok == false || item == nil {
		return ErrorItemNotExist
	}

	if item.at == 0 {
		return ErrorItemHaveBeenRecycled
	}

	item.at = time.Now().Unix()

	return nil
}

func (p *Pool) Recycle() {
	var key uint64

	var ok bool
	var item *Item

	var index = 0
	var inusecount = 0
	var deleted = false

	p.lock.Lock()
	defer p.lock.Unlock()

	var timestamp = time.Now().Unix()

	p.listprt.IterateAndUpdate(func(k interface{}, v interface{}) bool {
		key, ok = k.(uint64)
		if ok == false {
			return false
		}
		item, ok = v.(*Item)
		if ok == false {
			return false
		}
		if item.at > 0 {
			if timestamp-item.at > p.timeout {
				item.counter = 0
				item.at = 0
				p.cursor++
				p.availablelist[p.cursor] = key

				p.tempList[index] = key
				index++
				return true
			} else {
				inusecount++
				return true
			}
		} else {
			if item.counter >= p.maxExpirationCounter {
				if p.RecycleUpdateFunc != nil {
					p.RecycleUpdateFunc(item.ptr)
				}
				deleted = true
				return false
			} else {
				item.counter++
				p.tempList[index] = key
				index++
				return true
			}
		}
	})
	if deleted {
		var i, j int
		j = 0
		for i = p.length - index - inusecount; i < p.length-inusecount; i++ {
			p.availablelist[i] = p.tempList[j]
			j++
		}
		p.cursor = p.length - inusecount - 1
	}
}

func (p *Pool) loop() {
	go func() {
		var err error
		var ptr interface{}
		var id uint64
		for {
			select {
			case <-p.getop:
				ptr, err = p.getItem()
				p.getrt <- ptr
				p.geter <- err
			case ptr = <-p.putop:
				id = <-p.putid
				p.putrt <- p.putItem(ptr, id)
			case ptr = <-p.updateop:
				id = <-p.updateid
				p.putrt <- p.updateItem(ptr, id)
			case <-p.loopCancel:
				return
			}
		}
	}()
}

func (p *Pool) recycleLoop() {
	go func() {
		var timer = time.NewTicker(p.recycleInterval)
		defer timer.Stop()
		for {
			select {
			case <-timer.C:
				p.Recycle()
			case <-p.recycleCancel:
				return
			}
		}
	}()
}
