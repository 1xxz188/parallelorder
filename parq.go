package parq

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/1xxz188/go-queue"
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/petermattis/goid"
)

var (
	ErrPutFail          = errors.New("put key fail maybe queue is full")
	ErrWasExited        = errors.New("Parq was exited")
	ErrPushNotFindKey   = errors.New("push not find key")
	ErrKeyDeleted       = errors.New("key has been deleted")
	ErrPushSelfInHandle = errors.New("push to self key is not allowed inside handle callback")
)

// getGoroutineID 获取当前 goroutine 的 ID（使用高性能 goid 库）
func getGoroutineID() int64 {
	return goid.Get()
}

type Handle[TKey comparable, TData any] func(pq *Parq[TKey, TData], key TKey, data TData)

type Options[TKey comparable, TData any] struct {
	nodeNum     uint32 //可并发元素数量,比如玩家数量，超出会被阻塞
	workNum     uint32 //并发处理协程数
	oneCallCnt  uint32
	msgCapacity uint32 //单元素的消息缓存个数
	fn          Handle[TKey, TData]
	sharding    func(key TKey) uint32 //key 的分片函数
}

func DefaultOptions[TKey comparable, TData any](fn Handle[TKey, TData], sharding func(key TKey) uint32) Options[TKey, TData] {
	return Options[TKey, TData]{
		nodeNum:     10240,
		workNum:     128,
		oneCallCnt:  10,
		fn:          fn,
		msgCapacity: 1024 * 8,
		sharding:    sharding,
	}
}

// DefaultOptionsString 便捷函数，用于 string 类型的 key
func DefaultOptionsString[TData any](fn Handle[string, TData]) Options[string, TData] {
	return DefaultOptions(fn, fnv32)
}

// fnv32 is FNV-1a hash for string keys
func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	keyLength := len(key)
	for i := 0; i < keyLength; i++ {
		hash ^= uint32(key[i])
		hash *= prime32
	}
	return hash
}

func (opt Options[TKey, TData]) WithWorkNum(workNum uint32) Options[TKey, TData] {
	opt.workNum = workNum
	return opt
}

func (opt Options[TKey, TData]) WithNodeNum(nodeNum uint32) Options[TKey, TData] {
	opt.nodeNum = nodeNum
	return opt
}

func (opt Options[TKey, TData]) WithMsgCapacity(msgCapacity uint32) Options[TKey, TData] {
	opt.msgCapacity = msgCapacity
	return opt
}

func (opt Options[TKey, TData]) WithOneCallCnt(oneCallCnt uint32) Options[TKey, TData] {
	opt.oneCallCnt = oneCallCnt
	return opt
}

type node[TKey comparable, TData any] struct {
	key TKey

	rwLock        sync.RWMutex
	isInReadyChan bool
	exit          bool         //退出包
	deleted       atomic.Bool  //已删除标记，使用原子操作避免数据竞争
	processingGID atomic.Int64 //正在处理此 node 的 goroutine ID，0 表示未被处理
	msgQueue      *queue.EsQueue[TData]
}

type Parq[TKey comparable, TData any] struct {
	readyChan   chan *node[TKey, TData]
	nodeMap     cmap.ConcurrentMap[TKey, *node[TKey, TData]]
	fn          Handle[TKey, TData]
	nodeNum     uint32
	workNum     uint32
	oneCallCnt  uint32
	msgCapacity uint32
	workGp      sync.WaitGroup
	pushWg      sync.WaitGroup // 追踪正在进行的 Push 操作
	exit        atomic.Bool    // 使用 atomic.Bool 避免数据竞争
}

func New[TKey comparable, TData any](opt Options[TKey, TData]) (*Parq[TKey, TData], error) {
	if opt.nodeNum <= 0 {
		return nil, errors.New("init nodeNum <= 0")
	}
	if opt.workNum <= 0 {
		return nil, errors.New("init workNum <= 0")
	}
	if opt.oneCallCnt <= 0 {
		return nil, errors.New("init oneCallCnt <= 0")
	}
	if opt.fn == nil {
		return nil, errors.New("init opt.fn == nil")
	}
	if opt.sharding == nil {
		return nil, errors.New("init opt.sharding == nil")
	}

	pq := &Parq[TKey, TData]{
		readyChan:   make(chan *node[TKey, TData], opt.nodeNum),
		nodeMap:     cmap.NewWithCustomShardingFunction[TKey, *node[TKey, TData]](opt.sharding),
		fn:          opt.fn,
		nodeNum:     opt.nodeNum,
		workNum:     opt.workNum,
		oneCallCnt:  opt.oneCallCnt,
		msgCapacity: opt.msgCapacity,
	}
	pq.workGp.Add(int(pq.workNum))
	for i := uint32(0); i < pq.workNum; i++ {
		go func() {
			defer pq.workGp.Done()

			gid := getGoroutineID()
			for item := range pq.readyChan {
				if item.exit {
					break
				}
				// 检查是否已删除，如果已删除则丢弃该节点的所有消息
				if item.deleted.Load() {
					continue
				}
				//only one coroutine call the unique ID at the same time
				var quantity uint32
				var ok bool
				var msg TData

				// 标记当前 item 正在被哪个 goroutine 处理
				item.processingGID.Store(gid)
				for i := uint32(0); i < pq.oneCallCnt; i++ {
					msg, ok, quantity = item.msgQueue.Get()
					if !ok {
						break
					}
					// 处理消息前再次检查删除标记
					if item.deleted.Load() {
						break
					}
					pq.fn(pq, item.key, msg)
					if quantity <= 0 {
						break
					}
				}
				item.processingGID.Store(0) // 清除标记
				if quantity <= 0 {          //maybe still have value
					item.rwLock.Lock()
					// 已删除的节点不再放回 readyChan
					if item.deleted.Load() {
						item.rwLock.Unlock()
						continue
					}
					if item.msgQueue.Quantity() > 0 {
						pq.readyChan <- item
					} else {
						item.isInReadyChan = false
					}
					item.rwLock.Unlock()
				} else { //still have value, so push again
					// 已删除的节点不再放回
					if !item.deleted.Load() {
						pq.readyChan <- item
					}
				}
			}
		}()
	}
	return pq, nil
}

func (pq *Parq[TKey, TData]) Push(key TKey, data TData) error {
	if pq.exit.Load() {
		return ErrWasExited
	}

	pq.pushWg.Add(1)
	defer pq.pushWg.Done()

	// 再次检查，避免在 Add 之前 Stop 已经开始
	if pq.exit.Load() {
		return ErrWasExited
	}

	var item *node[TKey, TData]
	item, ok := pq.nodeMap.Get(key)
	if !ok || item.deleted.Load() {
		// key 不存在或已被删除，创建新节点
		newItem := &node[TKey, TData]{
			key:      key,
			msgQueue: queue.NewQueue[TData](pq.msgCapacity),
		}
		if ok := pq.nodeMap.SetIfAbsent(key, newItem); ok {
			item = newItem
		} else {
			// SetIfAbsent 失败，说明其他 goroutine 已创建，重新获取
			item, ok = pq.nodeMap.Get(key)
			if !ok {
				return ErrPushNotFindKey
			}
			// 再次检查是否被删除（可能刚被另一个 goroutine 删除）
			if item.deleted.Load() {
				return ErrKeyDeleted
			}
		}
	}

	// 检查是否在 handle 回调中向同一个 key Push（禁止此操作以避免死锁）
	gid := getGoroutineID()
	if item.processingGID.Load() == gid {
		return ErrPushSelfInHandle
	}

	item.rwLock.Lock()
	defer item.rwLock.Unlock()

	// 获取锁后再次检查删除标记
	if item.deleted.Load() {
		return ErrKeyDeleted
	}

	ok, _ = item.msgQueue.Put(data)
	if !ok {
		return ErrPutFail
	}
	if item.isInReadyChan {
		return nil
	}
	item.isInReadyChan = true
	pq.readyChan <- item
	return nil
}

func (pq *Parq[TKey, TData]) Stop() {
	// 使用 CompareAndSwap 保证只有一个 goroutine 能执行 Stop 逻辑
	if !pq.exit.CompareAndSwap(false, true) {
		return
	}

	// 等待所有正在进行的 Push 操作完成
	pq.pushWg.Wait()

	// 抛入退出事件
	var item node[TKey, TData]
	item.exit = true
	for i := uint32(0); i < pq.workNum; i++ {
		pq.readyChan <- &item
	}
	pq.workGp.Wait()

	// 最后排空处理
	gid := getGoroutineID()
	for {
		select {
		case item, ok := <-pq.readyChan:
			if !ok {
				return
			}
			// 跳过 exit 节点，避免 nil pointer panic
			if item.exit {
				continue
			}
			var quantity uint32
			var msg TData
			// 标记当前 item 正在被处理
			item.processingGID.Store(gid)
			for {
				msg, ok, quantity = item.msgQueue.Get()
				if !ok {
					break
				}
				pq.fn(pq, item.key, msg)
				if quantity <= 0 {
					break
				}
			}
			item.processingGID.Store(0)
		default:
			return
		}
	}
}

// Remove 删除指定 key，会丢弃该 key 所有未处理的消息
// 如果 key 正在被处理，会等待当前消息处理完毕后停止
// 删除后可以重新 Push 相同的 key，会创建新的节点
func (pq *Parq[TKey, TData]) Remove(key TKey) bool {
	if pq.exit.Load() {
		return false
	}

	item, ok := pq.nodeMap.Get(key)
	if !ok {
		return false
	}

	item.rwLock.Lock()
	defer item.rwLock.Unlock()

	// 已经被删除
	if item.deleted.Load() {
		return false
	}

	// 标记删除
	item.deleted.Store(true)

	// 从 nodeMap 中移除
	pq.nodeMap.Remove(key)

	return true
}

// Has 检查 key 是否存在
func (pq *Parq[TKey, TData]) Has(key TKey) bool {
	item, ok := pq.nodeMap.Get(key)
	if !ok {
		return false
	}
	return !item.deleted.Load()
}

// Keys 返回所有有效的 key 列表
func (pq *Parq[TKey, TData]) Keys() []TKey {
	keys := pq.nodeMap.Keys()
	result := make([]TKey, 0, len(keys))
	for _, key := range keys {
		if item, ok := pq.nodeMap.Get(key); ok && !item.deleted.Load() {
			result = append(result, key)
		}
	}
	return result
}

// Count 返回有效的 key 数量
func (pq *Parq[TKey, TData]) Count() int {
	return pq.nodeMap.Count()
}

// PendingCounts 返回所有 key 的消息队列剩余数量
// 返回一个 map，key 是 TKey，value 是该 key 的消息队列中剩余的消息数量
func (pq *Parq[TKey, TData]) PendingCounts() map[TKey]uint32 {
	result := make(map[TKey]uint32)
	for _, key := range pq.nodeMap.Keys() {
		if item, ok := pq.nodeMap.Get(key); ok && !item.deleted.Load() {
			if quantity := item.msgQueue.Quantity(); quantity > 0 {
				result[key] = quantity
			}
		}
	}
	return result
}

// PendingCount 返回指定 key 的消息队列剩余数量
// 如果 key 不存在或已被删除，返回 0
func (pq *Parq[TKey, TData]) PendingCount(key TKey) uint32 {
	item, ok := pq.nodeMap.Get(key)
	if !ok || item.deleted.Load() {
		return 0
	}
	return item.msgQueue.Quantity()
}
