# parq

高性能并行有序消息处理器，解决队头阻塞问题。(parq = **par**allel **q**ueue)

## 特性

- **有序处理**: 相同 key 的消息按顺序处理
- **并行执行**: 不同 key 并行处理
- **线程安全**: 所有操作都是协程安全的
- **优雅关闭**: Stop() 会等待所有消息处理完成
- **动态 Key 管理**: 支持运行时添加和删除 key
- **泛型支持**: 支持任意可比较的 key 类型和任意数据类型

## 安装

```bash
go get github.com/1xxz188/parq@latest
```

## 快速开始

### String Key（大多数场景推荐）

```go
package main

import (
    "fmt"
    "github.com/1xxz188/parq"
)

func main() {
    // 定义处理函数（第一个参数是 Parq 指针）
    fn := func(pq *parq.Parq[string, string], key string, data string) {
        fmt.Println(key, data)
    }

    // 使用 DefaultOptionsString 处理 string 类型的 key（内置 FNV-1a 哈希）
    entity, err := parq.New(parq.DefaultOptionsString(fn))
    if err != nil {
        panic(err)
    }

    // 推送消息
    entity.Push("player1", "login")
    entity.Push("player1", "move")    // 会在 "login" 之后处理
    entity.Push("player2", "login")   // 与 player1 并行处理

    // 优雅关闭
    entity.Stop()
}
```

### 自定义 Key 类型

```go
package main

import (
    "fmt"
    "github.com/1xxz188/parq"
)

func main() {
    // 使用 int64 作为 key 的处理函数（第一个参数是 Parq 指针）
    fn := func(pq *parq.Parq[int64, string], userID int64, data string) {
        fmt.Printf("User %d: %s\n", userID, data)
    }

    // int64 key 的自定义分片函数
    sharding := func(key int64) uint32 {
        return uint32(key) ^ uint32(key>>32)
    }

    entity, err := parq.New(parq.DefaultOptions(fn, sharding))
    if err != nil {
        panic(err)
    }

    entity.Push(int64(1001), "login")
    entity.Push(int64(1002), "login")

    entity.Stop()
}
```

## 配置选项

```go
// 为你的 key 类型定义分片函数
sharding := func(key string) uint32 {
    // 你的分片逻辑
    return uint32(len(key)) // 示例
}

// 自定义 key 类型和分片函数的默认配置
opt := parq.DefaultOptions(fn, sharding)

// 使用构建器模式自定义配置
opt = parq.DefaultOptions(fn, sharding).
    WithNodeNum(10240).      // 最大并发 key 数量 (默认: 10240)
    WithWorkNum(128).        // 工作协程数量 (默认: 128)
    WithMsgCapacity(8192).   // 每个 key 的消息队列容量 (默认: 8192)
    WithOneCallCnt(10)       // 每批处理的消息数量 (默认: 10)

entity, err := parq.New(opt)
```

| 选项 | 默认值 | 说明 |
|--------|---------|-------------|
| `nodeNum` | 10240 | 最大并发 key 数量，超出会阻塞 Push() |
| `workNum` | 128 | 工作协程数量 |
| `msgCapacity` | 8192 | 每个 key 的消息队列容量 |
| `oneCallCnt` | 10 | 每次处理多少消息后切换到其他 key |

## API 参考

### Handle

```go
type Handle[TKey comparable, TData any] func(pq *Parq[TKey, TData], key TKey, data TData)
```

处理函数类型。第一个参数 `pq` 是 Parq 实例指针，允许你在处理函数中调用 `Push`、`Remove`、`Has` 等方法。

### New

```go
func New[TKey comparable, TData any](opt Options[TKey, TData]) (*Parq[TKey, TData], error)
```

创建一个新的 Parq 实例。

### DefaultOptions

```go
func DefaultOptions[TKey comparable, TData any](fn Handle[TKey, TData], sharding func(key TKey) uint32) Options[TKey, TData]
```

使用自定义 key 类型和分片函数创建默认配置。

### Push

```go
func (pq *Parq[TKey, TData]) Push(key TKey, data TData) error
```

向指定 key 的队列推送消息。相同 key 的消息保证按顺序处理。

**错误:**
- `ErrWasExited` - 已经停止
- `ErrPutFail` - 消息队列已满
- `ErrKeyDeleted` - Key 已被删除

### Stop

```go
func (pq *Parq[TKey, TData]) Stop()
```

优雅停止处理器。会等待所有待处理的消息处理完成。

### Remove

```go
func (pq *Parq[TKey, TData]) Remove(key TKey) bool
```

删除一个 key 并丢弃其所有未处理的消息。如果 key 存在返回 true。

### Has

```go
func (pq *Parq[TKey, TData]) Has(key TKey) bool
```

检查 key 是否存在。

### Keys

```go
func (pq *Parq[TKey, TData]) Keys() []TKey
```

获取所有有效的 key 列表。

### Count

```go
func (pq *Parq[TKey, TData]) Count() int
```

获取有效 key 的数量。

## 错误类型

```go
var (
    ErrPutFail        = errors.New("put key fail maybe queue is full")  // 队列已满
    ErrWasExited      = errors.New("Parq was exited")                   // 已停止
    ErrPushNotFindKey = errors.New("push not find key")                 // 内部错误
    ErrKeyDeleted     = errors.New("key has been deleted")              // key 已删除
)
```

## 高级示例

```go
package main

import (
    "fmt"
    "sync"
    "github.com/1xxz188/parq"
)

type GameMessage struct {
    Action string
    Data   interface{}
}

func main() {
    var wg sync.WaitGroup

    handler := func(pq *parq.Parq[string, GameMessage], playerID string, msg GameMessage) {
        fmt.Printf("[%s] Action: %s, Data: %v\n", playerID, msg.Action, msg.Data)
        wg.Done()
    }

    // 使用 DefaultOptionsString 创建自定义配置
    opt := parq.DefaultOptionsString(handler).
        WithWorkNum(64).
        WithNodeNum(5000).
        WithMsgCapacity(1024)

    entity, err := parq.New(opt)
    if err != nil {
        panic(err)
    }

    // 模拟游戏事件
    players := []string{"player1", "player2", "player3"}
    actions := []string{"login", "move", "attack", "logout"}

    for _, player := range players {
        for _, action := range actions {
            wg.Add(1)
            entity.Push(player, GameMessage{Action: action, Data: nil})
        }
    }

    wg.Wait()

    // 检查活跃玩家
    fmt.Println("Active players:", entity.Keys())
    fmt.Println("Player count:", entity.Count())

    // 移除玩家
    entity.Remove("player1")
    fmt.Println("Has player1:", entity.Has("player1"))

    entity.Stop()
}
```

## 在处理函数中使用 Parq 指针

处理函数接收 Parq 实例指针，支持高级用法：

```go
handler := func(pq *parq.Parq[string, string], key string, data string) {
    // 在处理函数中推送新消息
    if data == "trigger" {
        pq.Push("another_key", "triggered_message")
    }
    
    // 检查另一个 key 是否存在
    if pq.Has("special_key") {
        // 执行某些操作
    }
    
    // 根据条件删除 key
    if data == "cleanup" {
        pq.Remove("old_key")
    }
}
```

## 使用场景

- **游戏服务器**: 按顺序处理玩家操作，同时并发处理多个玩家
- **消息队列消费者**: 确保每个分区/key 的有序处理
- **事件溯源**: 按顺序处理每个聚合的事件
- **用户级限流**: 按用户顺序处理请求

## 许可证

MIT License

