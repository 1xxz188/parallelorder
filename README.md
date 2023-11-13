# parallelorder
require sequential processing of certain things at high concurrency. Solve the head blocking problem.<br>
The same key keeps the callbacks called sequentially, and different keys can be called by concurrently.<br>

需要以高并发顺序处理某些事情，解决头部阻塞问题。<br>
相同的键保持回调顺序处理，不同的键可以同时调用。<br>

## Install
```
go get github.com/1xxz188/parallelorder@v1.0.0
```
## Demo
```
package main

import (
	"fmt"
	"github.com/1xxz188/parallelorder"
)

func main() {
	fn := func(key string, data string) {
		fmt.Println(key, data)
	}

	entity, err := parallelorder.New[string](parallelorder.DefaultOptions(fn))
	if err != nil {
		panic(err)
	}

	err = entity.Push("key", "value")
	if err != nil {
		panic(err)
	}
	entity.Stop()
}
```
## 注意事项
Push和Stop建议遵循谁产生谁关闭原则，即调用接口为同一协程，避免排空消息出现残留。