package parallelorder

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/1xxz188/go-queue"
	cmap "github.com/orcaman/concurrent-map/v2"
)

var (
	ErrPutFail        = errors.New("put key fail maybe queue is full")
	ErrWasExited      = errors.New("ParallelOrder was exited")
	ErrPushNotFindKey = errors.New("push not find key")
)

type Handle[TData any] func(key string, data TData)

type Options[TData any] struct {
	nodeNum     uint32 //可并发元素数量,比如玩家数量，超出会被阻塞
	workNum     uint32 //并发处理协程数
	oneCallCnt  uint32
	msgCapacity uint32 //单元素的消息缓存个数
	fn          Handle[TData]
}

func DefaultOptions[TData any](fn Handle[TData]) Options[TData] {
	return Options[TData]{
		nodeNum:     10240,
		workNum:     128,
		oneCallCnt:  10,
		fn:          fn,
		msgCapacity: 1024 * 8,
	}
}
func (opt Options[TData]) WithWorkNum(workNum uint32) Options[TData] {
	opt.workNum = workNum
	return opt
}

func (opt Options[TData]) WithNodeNum(nodeNum uint32) Options[TData] {
	opt.nodeNum = nodeNum
	return opt
}

func (opt Options[TData]) WithMsgCapacity(msgCapacity uint32) Options[TData] {
	opt.msgCapacity = msgCapacity
	return opt
}

func (opt Options[TData]) WithOneCallCnt(oneCallCnt uint32) Options[TData] {
	opt.oneCallCnt = oneCallCnt
	return opt
}

type node[TData any] struct {
	key string

	rwLock        sync.RWMutex
	isInReadyChan bool
	exit          bool //退出包
	msgQueue      *queue.EsQueue[TData]
}

type ParallelOrder[TData any] struct {
	readyChan   chan *node[TData]
	nodeMap     cmap.ConcurrentMap[string, *node[TData]]
	fn          Handle[TData]
	nodeNum     uint32
	workNum     uint32
	oneCallCnt  uint32
	msgCapacity uint32
	workGp      sync.WaitGroup
	pushWg      sync.WaitGroup // 追踪正在进行的 Push 操作
	exit        atomic.Bool    // 使用 atomic.Bool 避免数据竞争
}

func New[TData any](opt Options[TData]) (*ParallelOrder[TData], error) {
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

	po := &ParallelOrder[TData]{
		readyChan:   make(chan *node[TData], opt.nodeNum),
		nodeMap:     cmap.New[*node[TData]](), //<ID, *node>
		fn:          opt.fn,
		nodeNum:     opt.nodeNum,
		workNum:     opt.workNum,
		oneCallCnt:  opt.oneCallCnt,
		msgCapacity: opt.msgCapacity,
	}
	po.workGp.Add(int(po.workNum))
	for i := uint32(0); i < po.workNum; i++ {
		go func() {
			defer po.workGp.Done()

			for item := range po.readyChan {
				if item.exit {
					break
				}
				//only one coroutine call the unique ID at the same time
				var quantity uint32
				var ok bool
				var msg TData
				for i := uint32(0); i < po.oneCallCnt; i++ {
					msg, ok, quantity = item.msgQueue.Get()
					if !ok {
						break
					}
					po.fn(item.key, msg)
					if quantity <= 0 {
						break
					}
				}
				if quantity <= 0 { //maybe still have value
					item.rwLock.Lock()
					if item.msgQueue.Quantity() > 0 {
						po.readyChan <- item
					} else {
						item.isInReadyChan = false
					}
					item.rwLock.Unlock()
				} else { //still have value, so push again
					po.readyChan <- item
				}
			}
		}()
	}
	return po, nil
}

func (po *ParallelOrder[TData]) Push(key string, data TData) error {
	if po.exit.Load() {
		return ErrWasExited
	}

	po.pushWg.Add(1)
	defer po.pushWg.Done()

	// 再次检查，避免在 Add 之前 Stop 已经开始
	if po.exit.Load() {
		return ErrWasExited
	}

	var item *node[TData]
	item, ok := po.nodeMap.Get(key)
	if !ok {
		item = &node[TData]{
			key:      key,
			msgQueue: queue.NewQueue[TData](po.msgCapacity),
		}
		if ok := po.nodeMap.SetIfAbsent(key, item); !ok {
			item, ok = po.nodeMap.Get(key)
			if !ok {
				return ErrPushNotFindKey
			}
		}
	}

	item.rwLock.Lock()
	defer item.rwLock.Unlock()

	ok, _ = item.msgQueue.Put(data)
	if !ok {
		return ErrPutFail
	}
	if item.isInReadyChan {
		return nil
	}
	item.isInReadyChan = true
	po.readyChan <- item
	return nil
}

func (po *ParallelOrder[TData]) Stop() {
	// 使用 CompareAndSwap 保证只有一个 goroutine 能执行 Stop 逻辑
	if !po.exit.CompareAndSwap(false, true) {
		return
	}

	// 等待所有正在进行的 Push 操作完成
	po.pushWg.Wait()

	// 抛入退出事件
	var item node[TData]
	item.exit = true
	for i := uint32(0); i < po.workNum; i++ {
		po.readyChan <- &item
	}
	po.workGp.Wait()

	// 最后排空处理
	for {
		select {
		case item, ok := <-po.readyChan:
			if !ok {
				goto END // 修复: 使用 goto 跳出循环
			}
			// 修复: 跳过 exit 节点，避免 nil pointer panic
			if item.exit {
				continue
			}
			var quantity uint32
			var msg TData
			for {
				msg, ok, quantity = item.msgQueue.Get()
				if !ok {
					break
				}
				po.fn(item.key, msg)
				if quantity <= 0 {
					break
				}
			}
		default:
			goto END
		}
	}
END:
}
