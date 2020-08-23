# Golang high-performance object reuse pool to minimize the GC performance consumption caused by repeated object creation and release. It is thread safe

## Example
 

```go
package main

import (
    "fmt"
    "math/rand" 
	"github.com/wuyongjia/pool"
)

func main() {
	var p = pool.New(100, func() interface{} {
		var buf = make([]byte, 1024)
		return &buf
	})

	var err error
	var s1, s2 interface{}

	s1, err = p.Get()
	if err != nil {
		fmt.Println(err)
		return
	}
	var b1 = s1.(*[]byte)
	copy((*b1)[:], []byte(fmt.Sprintf("#-ABC-%d-*", rand.Intn(100))))
	if err = p.Put(b1); err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("pointer:%p, string:%s\n", b1, string(*b1))

	s2, err = p.Get()
	if err != nil {
		fmt.Println(err)
		return
	}
	var b2 = s2.(*[]byte)
	copy((*b2)[:], []byte(fmt.Sprintf("#-ABC-%d-*", rand.Intn(100))))
	if err = p.Put(b2); err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("pointer:%p, string:%s\n", b2, string(*b2))
}
```