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
	DEFAULT_LENGTH          = 20480 // max size of the pool
	DEFAULT_TIMEOUT         = 3600  // every 3600 seconds to recycle the status
	DEFAULT_COUNTER_EXPIRET = 1     // it depends on the counter, expired data will be deleted
	CHAN_GET_G              = 'g'
)

type Mode int

const (
	MODE_NONE  Mode = 1
	MODE_QUEUE Mode = 2
)

type AllocFunc func() interface{}
type AllocWithIdFunc func(id uint64) interface{}

type UpdateFunc func(ptr interface{})

type Pool struct {
	id                uint64
	mode              Mode
	getop             chan byte
	getrt             chan interface{}
	geter             chan error
	putop             chan interface{}
	putid             chan uint64
	putrt             chan error
	cursor            int
	length            int
	availablelist     []uint64
	tempList          []uint64
	listprt           *hashmap.HM
	recycleInterval   time.Duration
	timeout           int64
	isUseId           bool
	allocFunc         AllocFunc // must return pointer
	allocWithIdFunc   AllocWithIdFunc
	RecycleUpdateFunc UpdateFunc
	lock              *sync.RWMutex
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
	pool.loop()
	pool.recycleLoop()
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
	pool.loop()
	pool.recycleLoop()
	return pool
}

func newPool(length int) *Pool {
	var pool = &Pool{
		id:                0,
		mode:              MODE_NONE,
		getop:             make(chan byte, length),
		getrt:             make(chan interface{}, 1),
		geter:             make(chan error, 1),
		putop:             make(chan interface{}, length),
		putid:             make(chan uint64, length),
		putrt:             make(chan error, 1),
		cursor:            length - 1,
		length:            length,
		availablelist:     make([]uint64, length),
		tempList:          make([]uint64, length),
		listprt:           hashmap.New(length),
		recycleInterval:   DEFAULT_TIMEOUT * time.Second,
		timeout:           DEFAULT_TIMEOUT,
		RecycleUpdateFunc: nil,
		lock:              &sync.RWMutex{},
	}
	return pool
}

func (p *Pool) SetMode(mode Mode) {
	p.mode = mode
}

func (p *Pool) SetRecycleInterval(t time.Duration) {
	p.recycleInterval = t
}

func (p *Pool) SetTimeout(t int64) {
	p.timeout = t
}

func (p *Pool) Get() (interface{}, error) {
	if p.mode == MODE_NONE {
		return p.getItem()
	} else {
		p.getop <- CHAN_GET_G
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
	var err error
	var value interface{}
	var item *Item

	p.lock.Lock()
	defer p.lock.Unlock()

	if p.cursor < 0 {
		return nil, errors.New("the pool is full")
	}

	var id = p.availablelist[p.cursor]
	if id > 0 {
		value = p.listprt.Get(id)
		item = value.(*Item)
		if item != nil {
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
		return 0, nil, errors.New("the new item must be a pointer")
	}
	return *(*uint64)(unsafe.Pointer(rv.Pointer())), &Item{ptr: ptr, counter: 0, at: time.Now().Unix()}, nil
}

func (p *Pool) createItemWithId() (uint64, *Item, error) {
	p.id++
	var ptr = p.allocWithIdFunc(p.id)
	var rv = reflect.ValueOf(ptr)
	if rv.Kind() != reflect.Ptr {
		return 0, nil, errors.New("the new item must be a pointer")
	}
	return p.id, &Item{ptr: ptr, counter: 0, at: time.Now().Unix()}, nil
}

func (p *Pool) putItem(ptr interface{}, id uint64) error {
	var rv = reflect.ValueOf(ptr)
	if rv.Kind() != reflect.Ptr {
		return errors.New("the item must be a pointer")
	}

	if p.isUseId == false {
		id = *(*uint64)(unsafe.Pointer(rv.Pointer()))
	}

	p.lock.Lock()
	defer p.lock.Unlock()

	if (p.cursor + 1) >= p.length {
		return errors.New("the cursor is now at the last boundary")
	}

	var value interface{}
	var item *Item

	value = p.listprt.Get(id)
	item = value.(*Item)

	if item == nil {
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

	var index = 0
	var inusecount = 0
	var deleted = false

	p.lock.Lock()
	defer p.lock.Unlock()

	var timestamp = time.Now().Unix()

	p.listprt.Iterate(func(k interface{}, v interface{}) {
		key = k.(uint64)
		item = v.(*Item)
		if item.at > 0 {
			if timestamp-item.at > p.timeout {
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
				if p.RecycleUpdateFunc != nil {
					p.RecycleUpdateFunc(item.ptr)
				}
				p.listprt.Remove(key)
				deleted = true
			} else {
				item.counter++
				p.tempList[index] = key
				index++
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
			}
		}
	}()
}

func (p *Pool) recycleLoop() {
	go func() {
		var timer = time.NewTicker(p.recycleInterval)
		defer timer.Stop()
		for {
			<-timer.C
			p.Recycle()
		}
	}()
}
