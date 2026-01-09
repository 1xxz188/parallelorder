# parallelorder

High-performance parallel ordered message processor with head-of-line blocking solution.

高性能并行有序消息处理器，解决队头阻塞问题。

## Features / 特性

- **Ordered Processing / 有序处理**: Same key messages are processed sequentially / 相同 key 的消息按顺序处理
- **Parallel Execution / 并行执行**: Different keys are processed concurrently / 不同 key 并行处理
- **Thread-Safe / 线程安全**: All operations are goroutine-safe / 所有操作都是协程安全的
- **Graceful Shutdown / 优雅关闭**: Stop() waits for all messages to be processed / Stop() 会等待所有消息处理完成
- **Dynamic Key Management / 动态 Key 管理**: Support adding and removing keys at runtime / 支持运行时添加和删除 key
- **Generic Support / 泛型支持**: Works with any data type / 支持任意数据类型

## Install / 安装

```bash
go get github.com/1xxz188/parallelorder@latest
```

## Quick Start / 快速开始

```go
package main

import (
    "fmt"
    "github.com/1xxz188/parallelorder"
)

func main() {
    // Define handler function / 定义处理函数
    fn := func(key string, data string) {
        fmt.Println(key, data)
    }

    // Create instance with default options / 使用默认配置创建实例
    entity, err := parallelorder.New(parallelorder.DefaultOptions(fn))
    if err != nil {
        panic(err)
    }

    // Push messages / 推送消息
    entity.Push("player1", "login")
    entity.Push("player1", "move")    // Will be processed after "login" / 会在 "login" 之后处理
    entity.Push("player2", "login")   // Processed concurrently with player1 / 与 player1 并行处理

    // Graceful shutdown / 优雅关闭
    entity.Stop()
}
```

## Configuration / 配置选项

```go
// Default options / 默认配置
opt := parallelorder.DefaultOptions(fn)

// Customize with builder pattern / 使用构建器模式自定义配置
opt = parallelorder.DefaultOptions(fn).
    WithNodeNum(10240).      // Max concurrent keys / 最大并发 key 数量 (default: 10240)
    WithWorkNum(128).        // Worker goroutine count / 工作协程数量 (default: 128)
    WithMsgCapacity(8192).   // Message queue capacity per key / 每个 key 的消息队列容量 (default: 8192)
    WithOneCallCnt(10)       // Messages processed per batch / 每批处理的消息数量 (default: 10)

entity, err := parallelorder.New(opt)
```

| Option | Default | Description |
|--------|---------|-------------|
| `nodeNum` | 10240 | Maximum number of concurrent keys. Exceeding this will block Push() / 最大并发 key 数量，超出会阻塞 Push() |
| `workNum` | 128 | Number of worker goroutines / 工作协程数量 |
| `msgCapacity` | 8192 | Message queue capacity per key / 每个 key 的消息队列容量 |
| `oneCallCnt` | 10 | Messages to process before yielding to other keys / 每次处理多少消息后切换到其他 key |

## API Reference / API 参考

### Push

```go
func (po *ParallelOrder[TData]) Push(key string, data TData) error
```

Push a message to the specified key's queue. Messages with the same key are guaranteed to be processed in order.

向指定 key 的队列推送消息。相同 key 的消息保证按顺序处理。

**Errors:**
- `ErrWasExited` - ParallelOrder has been stopped / 已经停止
- `ErrPutFail` - Message queue is full / 消息队列已满
- `ErrKeyDeleted` - Key has been deleted / Key 已被删除

### Stop

```go
func (po *ParallelOrder[TData]) Stop()
```

Gracefully stop the processor. Waits for all pending messages to be processed.

优雅停止处理器。会等待所有待处理的消息处理完成。

### Remove

```go
func (po *ParallelOrder[TData]) Remove(key string) bool
```

Remove a key and discard all its pending messages. Returns true if the key existed.

删除一个 key 并丢弃其所有未处理的消息。如果 key 存在返回 true。

### Has

```go
func (po *ParallelOrder[TData]) Has(key string) bool
```

Check if a key exists.

检查 key 是否存在。

### Keys

```go
func (po *ParallelOrder[TData]) Keys() []string
```

Get all active keys.

获取所有有效的 key 列表。

### Count

```go
func (po *ParallelOrder[TData]) Count() int
```

Get the number of active keys.

获取有效 key 的数量。

## Error Types / 错误类型

```go
var (
    ErrPutFail        = errors.New("put key fail maybe queue is full")  // 队列已满
    ErrWasExited      = errors.New("ParallelOrder was exited")          // 已停止
    ErrPushNotFindKey = errors.New("push not find key")                 // 内部错误
    ErrKeyDeleted     = errors.New("key has been deleted")              // key 已删除
)
```

## Advanced Example / 高级示例

```go
package main

import (
    "fmt"
    "sync"
    "github.com/1xxz188/parallelorder"
)

type GameMessage struct {
    Action string
    Data   interface{}
}

func main() {
    var wg sync.WaitGroup

    handler := func(playerID string, msg GameMessage) {
        fmt.Printf("[%s] Action: %s, Data: %v\n", playerID, msg.Action, msg.Data)
        wg.Done()
    }

    // Create with custom options / 使用自定义配置创建
    opt := parallelorder.DefaultOptions(handler).
        WithWorkNum(64).
        WithNodeNum(5000).
        WithMsgCapacity(1024)

    entity, err := parallelorder.New(opt)
    if err != nil {
        panic(err)
    }

    // Simulate game events / 模拟游戏事件
    players := []string{"player1", "player2", "player3"}
    actions := []string{"login", "move", "attack", "logout"}

    for _, player := range players {
        for _, action := range actions {
            wg.Add(1)
            entity.Push(player, GameMessage{Action: action, Data: nil})
        }
    }

    wg.Wait()

    // Check active players / 检查活跃玩家
    fmt.Println("Active players:", entity.Keys())
    fmt.Println("Player count:", entity.Count())

    // Remove a player / 移除玩家
    entity.Remove("player1")
    fmt.Println("Has player1:", entity.Has("player1"))

    entity.Stop()
}
```

## Use Cases / 使用场景

- **Game Server / 游戏服务器**: Process player actions in order while handling multiple players concurrently / 按顺序处理玩家操作，同时并发处理多个玩家
- **Message Queue Consumer / 消息队列消费者**: Ensure ordered processing per partition/key / 确保每个分区/key 的有序处理
- **Event Sourcing / 事件溯源**: Process events for each aggregate in order / 按顺序处理每个聚合的事件
- **Rate Limiting per User / 用户级限流**: Process requests per user sequentially / 按用户顺序处理请求

## License

MIT License