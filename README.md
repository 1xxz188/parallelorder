# parallelorder

High-performance parallel ordered message processor with head-of-line blocking solution.

## Features

- **Ordered Processing**: Same key messages are processed sequentially
- **Parallel Execution**: Different keys are processed concurrently
- **Thread-Safe**: All operations are goroutine-safe
- **Graceful Shutdown**: Stop() waits for all messages to be processed
- **Dynamic Key Management**: Support adding and removing keys at runtime
- **Generic Support**: Works with any comparable key type and any data type

## Install

```bash
go get github.com/1xxz188/parallelorder/v2@latest
```

## Quick Start

### String Key (Recommended for most cases)

```go
package main

import (
    "fmt"
    "github.com/1xxz188/parallelorder/v2"
)

func main() {
    // Define handler function (first parameter is the ParallelOrder pointer)
    fn := func(po *parallelorder.ParallelOrder[string, string], key string, data string) {
        fmt.Println(key, data)
    }

    // Use DefaultOptionsString for string keys (built-in FNV-1a hash)
    entity, err := parallelorder.New(parallelorder.DefaultOptionsString(fn))
    if err != nil {
        panic(err)
    }

    // Push messages
    entity.Push("player1", "login")
    entity.Push("player1", "move")    // Will be processed after "login"
    entity.Push("player2", "login")   // Processed concurrently with player1

    // Graceful shutdown
    entity.Stop()
}
```

### Custom Key Type

```go
package main

import (
    "fmt"
    "github.com/1xxz188/parallelorder/v2"
)

func main() {
    // Handler with int64 key (first parameter is the ParallelOrder pointer)
    fn := func(po *parallelorder.ParallelOrder[int64, string], userID int64, data string) {
        fmt.Printf("User %d: %s\n", userID, data)
    }

    // Custom sharding function for int64 keys
    sharding := func(key int64) uint32 {
        return uint32(key) ^ uint32(key>>32)
    }

    entity, err := parallelorder.New(parallelorder.DefaultOptions(fn, sharding))
    if err != nil {
        panic(err)
    }

    entity.Push(int64(1001), "login")
    entity.Push(int64(1002), "login")

    entity.Stop()
}
```

## Configuration

```go
// Define sharding function for your key type
sharding := func(key string) uint32 {
    // Your sharding logic here
    return uint32(len(key)) // Example
}

// Default options with custom key type and sharding
opt := parallelorder.DefaultOptions(fn, sharding)

// Customize with builder pattern
opt = parallelorder.DefaultOptions(fn, sharding).
    WithNodeNum(10240).      // Max concurrent keys (default: 10240)
    WithWorkNum(128).        // Worker goroutine count (default: 128)
    WithMsgCapacity(8192).   // Message queue capacity per key (default: 8192)
    WithOneCallCnt(10)       // Messages processed per batch (default: 10)

entity, err := parallelorder.New(opt)
```

| Option | Default | Description |
|--------|---------|-------------|
| `nodeNum` | 10240 | Maximum number of concurrent keys. Exceeding this will block Push() |
| `workNum` | 128 | Number of worker goroutines |
| `msgCapacity` | 8192 | Message queue capacity per key |
| `oneCallCnt` | 10 | Messages to process before yielding to other keys |

## API Reference

### Handle

```go
type Handle[TKey comparable, TData any] func(po *ParallelOrder[TKey, TData], key TKey, data TData)
```

The handler function type. The first parameter `po` is the ParallelOrder instance pointer, allowing you to call methods like `Push`, `Remove`, `Has` within the handler.

### New

```go
func New[TKey comparable, TData any](opt Options[TKey, TData]) (*ParallelOrder[TKey, TData], error)
```

Create a new ParallelOrder instance.

### DefaultOptions

```go
func DefaultOptions[TKey comparable, TData any](fn Handle[TKey, TData], sharding func(key TKey) uint32) Options[TKey, TData]
```

Create default options with custom key type and sharding function.

### Push

```go
func (po *ParallelOrder[TKey, TData]) Push(key TKey, data TData) error
```

Push a message to the specified key's queue. Messages with the same key are guaranteed to be processed in order.

**Errors:**
- `ErrWasExited` - ParallelOrder has been stopped
- `ErrPutFail` - Message queue is full
- `ErrKeyDeleted` - Key has been deleted

### Stop

```go
func (po *ParallelOrder[TKey, TData]) Stop()
```

Gracefully stop the processor. Waits for all pending messages to be processed.

### Remove

```go
func (po *ParallelOrder[TKey, TData]) Remove(key TKey) bool
```

Remove a key and discard all its pending messages. Returns true if the key existed.

### Has

```go
func (po *ParallelOrder[TKey, TData]) Has(key TKey) bool
```

Check if a key exists.

### Keys

```go
func (po *ParallelOrder[TKey, TData]) Keys() []TKey
```

Get all active keys.

### Count

```go
func (po *ParallelOrder[TKey, TData]) Count() int
```

Get the number of active keys.

## Error Types

```go
var (
    ErrPutFail        = errors.New("put key fail maybe queue is full")
    ErrWasExited      = errors.New("ParallelOrder was exited")
    ErrPushNotFindKey = errors.New("push not find key")
    ErrKeyDeleted     = errors.New("key has been deleted")
)
```

## Advanced Example

```go
package main

import (
    "fmt"
    "sync"
    "github.com/1xxz188/parallelorder/v2"
)

type GameMessage struct {
    Action string
    Data   interface{}
}

func main() {
    var wg sync.WaitGroup

    handler := func(po *parallelorder.ParallelOrder[string, GameMessage], playerID string, msg GameMessage) {
        fmt.Printf("[%s] Action: %s, Data: %v\n", playerID, msg.Action, msg.Data)
        wg.Done()
    }

    // Create with custom options using DefaultOptionsString
    opt := parallelorder.DefaultOptionsString(handler).
        WithWorkNum(64).
        WithNodeNum(5000).
        WithMsgCapacity(1024)

    entity, err := parallelorder.New(opt)
    if err != nil {
        panic(err)
    }

    // Simulate game events
    players := []string{"player1", "player2", "player3"}
    actions := []string{"login", "move", "attack", "logout"}

    for _, player := range players {
        for _, action := range actions {
            wg.Add(1)
            entity.Push(player, GameMessage{Action: action, Data: nil})
        }
    }

    wg.Wait()

    // Check active players
    fmt.Println("Active players:", entity.Keys())
    fmt.Println("Player count:", entity.Count())

    // Remove a player
    entity.Remove("player1")
    fmt.Println("Has player1:", entity.Has("player1"))

    entity.Stop()
}
```

## Using ParallelOrder Pointer in Handler

The handler receives a pointer to the ParallelOrder instance, enabling advanced patterns:

```go
handler := func(po *parallelorder.ParallelOrder[string, string], key string, data string) {
    // Push new message from within handler
    if data == "trigger" {
        po.Push("another_key", "triggered_message")
    }
    
    // Check if another key exists
    if po.Has("special_key") {
        // do something
    }
    
    // Remove a key based on condition
    if data == "cleanup" {
        po.Remove("old_key")
    }
}
```

## Use Cases

- **Game Server**: Process player actions in order while handling multiple players concurrently
- **Message Queue Consumer**: Ensure ordered processing per partition/key
- **Event Sourcing**: Process events for each aggregate in order
- **Rate Limiting per User**: Process requests per user sequentially

## License

MIT License
