package pool

import (
	"errors"
	"reflect"
	"sync"
	"time"
	"unsafe"
)

const (
	DEFAULT_LENGTH          = 20480 // max size of the pool
	DEFAULT_TIMEOUT         = 120   // every 120 seconds to recycle the status
	DEFAULT_COUNTER_EXPIRET = 20    // it depends on the counter, expired data will be deleted
	CHAN_GET_G              = 'g'
)

type AllocFunc func() interface{}

type Pool struct {
	getop         chan byte
	getrt         chan interface{}
	geter         chan error
	putop         chan interface{}
	putrt         chan error
	cursor        int
	length        int
	availablelist []uint64
	tempList      []uint64
	listprt       map[uint64]*Item
	timeout       time.Duration
	allocFunc     AllocFunc // must return pointer
	lock          *sync.RWMutex
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
	var pool = &Pool{
		getop:         make(chan byte, length),
		getrt:         make(chan interface{}, 1),
		geter:         make(chan error, 1),
		putop:         make(chan interface{}, length),
		putrt:         make(chan error, 1),
		cursor:        length - 1,
		length:        length,
		availablelist: make([]uint64, length),
		tempList:      make([]uint64, length),
		listprt:       make(map[uint64]*Item),
		timeout:       DEFAULT_TIMEOUT * time.Second,
		allocFunc:     allocFunc,
		lock:          &sync.RWMutex{},
	}
	go pool.loop()
	return pool
}

func (p *Pool) Get() (interface{}, error) {
	p.getop <- CHAN_GET_G
	return <-p.getrt, <-p.geter
}

func (p *Pool) Put(ptr interface{}) error {
	p.putop <- ptr
	return <-p.putrt
}

func (p *Pool) GetCursor() int {
	return p.cursor
}

func (p *Pool) GetLength() int {
	return p.length
}

func (p *Pool) GetInstanceCount() int {
	return len(p.listprt)
}

func (p *Pool) getItem() (interface{}, error) {
	if p.cursor < 0 {
		return nil, errors.New("the pool is full")
	}

	var err error
	var item *Item
	var ok bool

	p.lock.Lock()
	defer p.lock.Unlock()

	var id = p.availablelist[p.cursor]
	if id > 0 {
		item, ok = p.listprt[id]
		if ok == true {
			item.counter = 0
			item.at = time.Now().Unix()
		} else {
			id, item, err = p.createItem()
			if err != nil {
				return nil, err
			}
			p.listprt[id] = item
		}
	} else {
		id, item, err = p.createItem()
		if err != nil {
			return nil, err
		}
		p.listprt[id] = item
	}

	p.availablelist[p.cursor] = 0
	p.cursor--

	return item.ptr, nil
}

func (p *Pool) createItem() (uint64, *Item, error) {
	var ptr = p.allocFunc()
	var rv = reflect.ValueOf(ptr)
	if rv.Kind() != reflect.Ptr {
		return 0, nil, errors.New("the new item must be a pointer")
	}
	return *(*uint64)(unsafe.Pointer(rv.Pointer())), &Item{ptr: ptr, counter: 0, at: time.Now().Unix()}, nil
}

func (p *Pool) putItem(ptr interface{}) error {
	if (p.cursor + 1) >= p.length {
		return errors.New("the cursor is now at the last boundary")
	}
	var rv = reflect.ValueOf(ptr)
	if rv.Kind() != reflect.Ptr {
		return errors.New("the item must be a pointer")
	}
	var id = *(*uint64)(unsafe.Pointer(rv.Pointer()))

	p.lock.Lock()
	defer p.lock.Unlock()

	var ok bool
	var item *Item
	if item, ok = p.listprt[id]; ok == false {
		return errors.New("the item does not exist in the pool")
	}
	if item.at == 0 {
		return errors.New("the item may have been recycled over time")
	}
	p.cursor++
	p.availablelist[p.cursor] = id
	item.at = 0
	return nil
}

func (p *Pool) Recycle() {
	var key uint64
	var item *Item

	var timestamp = time.Now().Unix()

	var index = 0
	var inusecount = 0
	var deleted = false

	p.lock.Lock()
	defer p.lock.Unlock()

	for key, item = range p.listprt {
		if item.at > 0 {
			if timestamp-item.at > DEFAULT_TIMEOUT {
				item.counter = 0
				item.at = 0
				p.cursor++
				p.availablelist[p.cursor] = key

				p.tempList[index] = key
				index++
			} else {
				inusecount++
			}
		} else {
			if item.counter > DEFAULT_COUNTER_EXPIRET {
				delete(p.listprt, key)
				deleted = true
			} else {
				item.counter++
				p.tempList[index] = key
				index++
			}
		}
	}
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
	var err error
	var ptr interface{}

	var timer = time.NewTimer(p.timeout)
	defer timer.Stop()

	for {
		timer.Reset(p.timeout)
		select {
		case <-p.getop:
			ptr, err = p.getItem()
			p.getrt <- ptr
			p.geter <- err
		case ptr = <-p.putop:
			p.putrt <- p.putItem(ptr)
		case <-timer.C:
			p.Recycle()
		}
	}
}
