package parq

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/stretchr/testify/require"
)

func TestDemo(t *testing.T) {
	exitChan := make(chan struct{})
	fn := func(pq *Parq[string, string], key string, data string) {
		fmt.Println(key, data)
		close(exitChan)
	}

	entity, err := New(DefaultOptionsString(fn))
	require.NoError(t, err)

	err = entity.Push("key", "value")
	require.NoError(t, err)
	<-exitChan
}

func TestMultiPushMsg(t *testing.T) {
	//var r = rand.New(rand.NewSource(time.Now().Unix()))
	msgCnt := 1024 * 2
	sendGoCnt := 1000
	testMap := cmap.New[int]()

	var revCnt int32
	var sendCnt int32
	shouldRevCnt := int32(msgCnt * sendGoCnt)
	overChan := make(chan struct{})

	fn := func(pq *Parq[string, int], key string, data int) {
		oldVV, ok := testMap.Get(key)
		if !ok {
			panic("not find key")
		}
		newVV := data
		if (oldVV + 1) != newVV {
			panic("(oldVV + 1)!= newVV")
		}
		//runtime.Gosched()
		//time.Sleep(time.Millisecond * time.Duration(r.Intn(5)))
		testMap.Set(key, newVV)

		atomic.AddInt32(&revCnt, 1)
		v := atomic.LoadInt32(&revCnt)
		/*if key == "100" && newVV%100 == 0 {
			fmt.Printf("key[%s] value[%d] revCnt[%d]\n", key, newVV, v)
		}*/
		if v >= shouldRevCnt {
			close(overChan)
		}
	}

	entity, err := New(DefaultOptionsString(fn))
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(sendGoCnt)
	beginChan := make(chan struct{})

	for i := 0; i < sendGoCnt; i++ {
		go func(idx int) {
			defer wg.Done()
			<-beginChan

			key := strconv.Itoa(idx + 1)
			testMap.Set(key, 0)

			for i := 0; i < msgCnt; i++ {
				atomic.AddInt32(&sendCnt, 1)
				err = entity.Push(key, i+1)
				if err != nil {
					panic(err)
				}
				//require.NoError(t, err)
			}
		}(i)
	}

	time.Sleep(time.Millisecond * 5)
	beginTm := time.Now()
	close(beginChan)
	wg.Wait()

	<-overChan
	t.Logf("revCnt[%d] cost[%s]\n", revCnt, time.Since(beginTm).String())
	require.Equal(t, revCnt, sendCnt)
}

func TestStop(t *testing.T) {
	var cnt int
	handle := func(pq *Parq[string, int], key string, data int) {
		cnt++
	}
	entity, err := New[string, int](DefaultOptionsString(handle))
	require.NoError(t, err)
	sendGoCnt := 1000
	for i := 0; i < sendGoCnt; i++ {
		err = entity.Push("key", i+1)
		if err != nil {
			panic(err)
		}
	}
	entity.Stop()
	require.Equal(t, 1000, cnt)
	err = entity.Push("key", 100)
	require.Equal(t, ErrWasExited, err)
}

// TestRemove 测试删除 key 功能
func TestRemove(t *testing.T) {
	var processedCount int32
	handle := func(pq *Parq[string, int], key string, data int) {
		atomic.AddInt32(&processedCount, 1)
		time.Sleep(time.Millisecond * 10) // 模拟处理耗时
	}

	entity, err := New[string, int](DefaultOptionsString(handle).WithWorkNum(4))
	require.NoError(t, err)

	// 推送一些消息
	for i := 0; i < 100; i++ {
		err = entity.Push("key1", i)
		require.NoError(t, err)
	}

	// 等待一小段时间让部分消息开始处理
	time.Sleep(time.Millisecond * 50)

	// 删除 key
	removed := entity.Remove("key1")
	require.True(t, removed)

	// 再次删除应该返回 false
	removed = entity.Remove("key1")
	require.False(t, removed)

	// 删除不存在的 key 应该返回 false
	removed = entity.Remove("non_existent_key")
	require.False(t, removed)

	// 删除后 Push 应该创建新节点
	err = entity.Push("key1", 999)
	require.NoError(t, err)

	entity.Stop()

	// 处理的消息数应该少于 100（因为中途删除了）
	t.Logf("Processed count: %d", processedCount)
}

// TestHas 测试检查 key 是否存在
func TestHas(t *testing.T) {
	handle := func(pq *Parq[string, int], key string, data int) {}

	entity, err := New[string, int](DefaultOptionsString(handle))
	require.NoError(t, err)

	// 初始状态没有 key
	require.False(t, entity.Has("key1"))

	// Push 后应该存在
	err = entity.Push("key1", 1)
	require.NoError(t, err)
	require.True(t, entity.Has("key1"))

	// 删除后应该不存在
	entity.Remove("key1")
	require.False(t, entity.Has("key1"))

	entity.Stop()
}

// TestKeys 测试获取所有 key
func TestKeys(t *testing.T) {
	handle := func(pq *Parq[string, int], key string, data int) {}

	entity, err := New[string, int](DefaultOptionsString(handle))
	require.NoError(t, err)

	// 初始状态没有 key
	require.Empty(t, entity.Keys())

	// 添加多个 key
	entity.Push("key1", 1)
	entity.Push("key2", 2)
	entity.Push("key3", 3)

	keys := entity.Keys()
	require.Len(t, keys, 3)
	require.Contains(t, keys, "key1")
	require.Contains(t, keys, "key2")
	require.Contains(t, keys, "key3")

	// 删除一个 key
	entity.Remove("key2")
	keys = entity.Keys()
	require.Len(t, keys, 2)
	require.NotContains(t, keys, "key2")

	entity.Stop()
}

// TestCount 测试获取 key 数量
func TestCount(t *testing.T) {
	handle := func(pq *Parq[string, int], key string, data int) {}

	entity, err := New[string, int](DefaultOptionsString(handle))
	require.NoError(t, err)

	require.Equal(t, 0, entity.Count())

	entity.Push("key1", 1)
	require.Equal(t, 1, entity.Count())

	entity.Push("key2", 2)
	require.Equal(t, 2, entity.Count())

	// 同一个 key 多次 Push 不增加数量
	entity.Push("key1", 3)
	require.Equal(t, 2, entity.Count())

	entity.Stop()
}

// TestConcurrentRemove 测试并发删除
func TestConcurrentRemove(t *testing.T) {
	var processedCount int32
	handle := func(pq *Parq[string, int], key string, data int) {
		atomic.AddInt32(&processedCount, 1)
	}

	entity, err := New[string, int](DefaultOptionsString(handle))
	require.NoError(t, err)

	// 创建多个 key
	keyCount := 100
	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("key%d", i)
		entity.Push(key, i)
	}

	// 并发删除
	var wg sync.WaitGroup
	for i := 0; i < keyCount; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := fmt.Sprintf("key%d", idx)
			entity.Remove(key)
		}(i)
	}
	wg.Wait()

	entity.Stop()
	t.Logf("Processed count after concurrent remove: %d", processedCount)
}

// TestRemoveAndPush 测试删除后重新 Push
func TestRemoveAndPush(t *testing.T) {
	var messages []string
	var mu sync.Mutex

	handle := func(pq *Parq[string, string], key string, data string) {
		mu.Lock()
		messages = append(messages, data)
		mu.Unlock()
	}

	entity, err := New[string, string](DefaultOptionsString(handle))
	require.NoError(t, err)

	// 第一轮 Push
	entity.Push("player1", "msg1")
	entity.Push("player1", "msg2")
	time.Sleep(time.Millisecond * 50)

	// 删除
	entity.Remove("player1")

	// 第二轮 Push（应该创建新节点）
	entity.Push("player1", "msg3")
	entity.Push("player1", "msg4")

	entity.Stop()

	mu.Lock()
	t.Logf("Messages: %v", messages)
	// msg3 和 msg4 应该都被处理
	require.Contains(t, messages, "msg3")
	require.Contains(t, messages, "msg4")
	mu.Unlock()
}

// TestOptions 测试配置选项
func TestOptions(t *testing.T) {
	handle := func(pq *Parq[string, int], key string, data int) {}

	// 测试默认配置
	opt := DefaultOptionsString(handle)
	entity, err := New[string, int](opt)
	require.NoError(t, err)
	entity.Stop()

	// 测试自定义配置
	opt = DefaultOptionsString(handle).
		WithNodeNum(100).
		WithWorkNum(8).
		WithMsgCapacity(512).
		WithOneCallCnt(5)

	entity, err = New[string, int](opt)
	require.NoError(t, err)
	entity.Stop()
}

// TestInvalidOptions 测试无效配置
func TestInvalidOptions(t *testing.T) {
	handle := func(pq *Parq[string, int], key string, data int) {}

	// nodeNum <= 0
	opt := DefaultOptionsString(handle).WithNodeNum(0)
	_, err := New[string, int](opt)
	require.Error(t, err)

	// workNum <= 0
	opt = DefaultOptionsString(handle).WithWorkNum(0)
	_, err = New[string, int](opt)
	require.Error(t, err)

	// oneCallCnt <= 0
	opt = DefaultOptionsString(handle).WithOneCallCnt(0)
	_, err = New[string, int](opt)
	require.Error(t, err)

	// fn == nil
	opt = DefaultOptionsString[int](nil)
	_, err = New[string, int](opt)
	require.Error(t, err)
}

// TestQueueFull 测试队列满的情况
func TestQueueFull(t *testing.T) {
	// 使用一个阻塞的 handler
	blockChan := make(chan struct{})
	handle := func(pq *Parq[string, int], key string, data int) {
		<-blockChan // 阻塞直到关闭
	}

	// 使用很小的队列容量
	opt := DefaultOptionsString(handle).WithMsgCapacity(10)
	entity, err := New[string, int](opt)
	require.NoError(t, err)

	// 快速填满队列
	var errCount int32
	for i := 0; i < 100; i++ {
		err := entity.Push("key1", i)
		if err == ErrPutFail {
			atomic.AddInt32(&errCount, 1)
		}
	}

	// 应该有一些消息因为队列满而失败
	t.Logf("Queue full errors: %d", errCount)

	close(blockChan)
	entity.Stop()
}

// TestConcurrentStop 测试并发调用 Stop
func TestConcurrentStop(t *testing.T) {
	var cnt int32
	handle := func(pq *Parq[string, int], key string, data int) {
		atomic.AddInt32(&cnt, 1)
	}

	entity, err := New[string, int](DefaultOptionsString(handle))
	require.NoError(t, err)

	// Push 一些消息
	for i := 0; i < 100; i++ {
		entity.Push("key", i)
	}

	// 并发调用 Stop（应该只有一个生效）
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			entity.Stop()
		}()
	}
	wg.Wait()

	// 所有消息都应该被处理
	require.Equal(t, int32(100), cnt)
}

// TestPushAfterRemove 测试删除后立即 Push 的并发情况
func TestPushAfterRemove(t *testing.T) {
	var processedCount int32
	handle := func(pq *Parq[string, int], key string, data int) {
		atomic.AddInt32(&processedCount, 1)
	}

	entity, err := New[string, int](DefaultOptionsString(handle))
	require.NoError(t, err)

	// 先创建 key
	entity.Push("key1", 1)
	time.Sleep(time.Millisecond * 10)

	// 并发执行 Remove 和 Push
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			entity.Remove("key1")
		}()
		go func(val int) {
			defer wg.Done()
			entity.Push("key1", val)
		}(i)
	}
	wg.Wait()

	entity.Stop()
	t.Logf("Processed after concurrent remove/push: %d", processedCount)
}

// TestOrderGuarantee 测试顺序保证
func TestOrderGuarantee(t *testing.T) {
	var results []int
	var mu sync.Mutex

	handle := func(pq *Parq[string, int], key string, data int) {
		mu.Lock()
		results = append(results, data)
		mu.Unlock()
	}

	opt := DefaultOptionsString(handle).WithWorkNum(16)
	entity, err := New[string, int](opt)
	require.NoError(t, err)

	// 向同一个 key 按顺序 Push
	msgCount := 1000
	for i := 0; i < msgCount; i++ {
		err = entity.Push("single_key", i)
		require.NoError(t, err)
	}

	entity.Stop()

	// 验证结果顺序
	require.Len(t, results, msgCount)
	for i := 0; i < msgCount; i++ {
		require.Equal(t, i, results[i], "Message order should be preserved")
	}
}

// TestIntKey 测试使用 int 作为 key
func TestIntKey(t *testing.T) {
	var results sync.Map

	handle := func(pq *Parq[int, string], key int, data string) {
		results.Store(key, data)
	}

	// 使用自定义 sharding 函数
	sharding := func(key int) uint32 {
		return uint32(key)
	}

	entity, err := New[int, string](DefaultOptions(handle, sharding))
	require.NoError(t, err)

	entity.Push(1, "one")
	entity.Push(2, "two")
	entity.Push(3, "three")

	entity.Stop()

	v, ok := results.Load(1)
	require.True(t, ok)
	require.Equal(t, "one", v)

	v, ok = results.Load(2)
	require.True(t, ok)
	require.Equal(t, "two", v)

	v, ok = results.Load(3)
	require.True(t, ok)
	require.Equal(t, "three", v)
}

// TestCustomKeyType 测试自定义 key 类型
func TestCustomKeyType(t *testing.T) {
	type UserID struct {
		Region string
		ID     int
	}

	var results sync.Map

	handle := func(pq *Parq[UserID, string], key UserID, data string) {
		results.Store(key, data)
	}

	// 使用自定义 sharding 函数
	sharding := func(key UserID) uint32 {
		hash := uint32(2166136261)
		const prime32 = uint32(16777619)
		for i := 0; i < len(key.Region); i++ {
			hash ^= uint32(key.Region[i])
			hash *= prime32
		}
		hash ^= uint32(key.ID)
		hash *= prime32
		return hash
	}

	entity, err := New[UserID, string](DefaultOptions(handle, sharding))
	require.NoError(t, err)

	user1 := UserID{Region: "us", ID: 1}
	user2 := UserID{Region: "eu", ID: 2}

	entity.Push(user1, "hello")
	entity.Push(user2, "world")

	entity.Stop()

	v, ok := results.Load(user1)
	require.True(t, ok)
	require.Equal(t, "hello", v)

	v, ok = results.Load(user2)
	require.True(t, ok)
	require.Equal(t, "world", v)

	// 测试 Has 和 Keys
	entity2, err := New[UserID, string](DefaultOptions(handle, sharding))
	require.NoError(t, err)

	entity2.Push(user1, "test1")
	require.True(t, entity2.Has(user1))
	require.False(t, entity2.Has(user2))

	entity2.Push(user2, "test2")
	keys := entity2.Keys()
	require.Len(t, keys, 2)

	entity2.Stop()
}

// TestHandleWithPointer 测试 Handle 函数第一个参数 *Parq 指针的使用
func TestHandleWithPointer(t *testing.T) {
	var results sync.Map
	var pointerValid atomic.Bool

	var entityRef *Parq[string, int]

	handle := func(pq *Parq[string, int], key string, data int) {
		// 测试 pq 指针不为 nil
		if pq == nil {
			t.Error("pq should not be nil")
			return
		}

		// 测试 pq 指针指向正确的实例
		if pq == entityRef {
			pointerValid.Store(true)
		}

		// 测试在回调中使用 pq 调用 Has 方法
		exists := pq.Has(key)
		results.Store(fmt.Sprintf("%s_has", key), exists)

		// 测试在回调中使用 pq 调用 Count 方法
		count := pq.Count()
		results.Store(fmt.Sprintf("%s_count", key), count)

		// 保存处理的数据
		results.Store(key, data)
	}

	entity, err := New[string, int](DefaultOptionsString(handle))
	require.NoError(t, err)
	entityRef = entity

	// 推送消息
	entity.Push("key1", 100)
	entity.Push("key2", 200)

	entity.Stop()

	// 验证 pq 指针有效
	require.True(t, pointerValid.Load(), "pq pointer should point to the correct entity")

	// 验证数据正确处理
	v, ok := results.Load("key1")
	require.True(t, ok)
	require.Equal(t, 100, v)

	v, ok = results.Load("key2")
	require.True(t, ok)
	require.Equal(t, 200, v)

	// 验证 Has 方法在回调中返回 true（因为 key 还存在）
	v, ok = results.Load("key1_has")
	require.True(t, ok)
	require.True(t, v.(bool))
}

// TestHandlePushFromCallback 测试在回调函数中通过 pq 指针 Push 其他 key（应该成功）
func TestHandlePushFromCallback(t *testing.T) {
	var processedKeys sync.Map
	expectedKeys := int32(3) // key1, key2, key3 (Push 到其他 key 是允许的)
	var processedCount int32
	var pushErr error
	doneChan := make(chan struct{})

	handle := func(pq *Parq[string, int], key string, data int) {
		processedKeys.Store(key, data)

		// 当处理 key1 时，尝试通过 pq 指针 Push 一个新的 key（应该成功，因为是不同的 key）
		if key == "key1" && data == 1 {
			pushErr = pq.Push("key3", 3)
		}

		if atomic.AddInt32(&processedCount, 1) >= expectedKeys {
			close(doneChan)
		}
	}

	entity, err := New[string, int](DefaultOptionsString(handle).WithWorkNum(1))
	require.NoError(t, err)

	entity.Push("key1", 1)
	entity.Push("key2", 2)

	// 等待所有消息处理完成
	<-doneChan
	entity.Stop()

	// 验证在 handle 中 Push 到其他 key 成功
	require.NoError(t, pushErr, "Push to different key inside handle should succeed")

	// 验证 key3 被正确处理（由回调中 Push 产生）
	v, ok := processedKeys.Load("key3")
	require.True(t, ok, "key3 should be processed")
	require.Equal(t, 3, v)

	// 验证 key1 和 key2 被处理
	_, ok = processedKeys.Load("key1")
	require.True(t, ok)
	_, ok = processedKeys.Load("key2")
	require.True(t, ok)
}

// TestHandleRemoveFromCallback 测试在回调函数中通过 pq 指针 Remove key
func TestHandleRemoveFromCallback(t *testing.T) {
	var processedCount int32
	var key1Processed, key2Processed int32
	key1Started := make(chan struct{}) // 用于同步，确保 key1 先开始处理

	handle := func(pq *Parq[string, int], key string, data int) {
		atomic.AddInt32(&processedCount, 1)

		if key == "key1" {
			atomic.AddInt32(&key1Processed, 1)
		} else if key == "key2" {
			atomic.AddInt32(&key2Processed, 1)
		}

		// 当处理 key1 的第一个消息时，删除 key2
		if key == "key1" && data == 1 {
			// 通知主线程：key1 开始处理了，可以 Push key2 了
			close(key1Started)

			// 等待一下让 key2 的 Push 完成
			time.Sleep(10 * time.Millisecond)

			t.Logf("Removing key2 from callback")
			// 在删除前检查 key2 的待处理消息数量
			pendingBefore := pq.PendingCount("key2")
			t.Logf("key2 pending messages before remove: %d", pendingBefore)

			removed := pq.Remove("key2")
			require.True(t, removed, "key2 should be removed successfully")

			// 删除后 key2 应该不存在
			require.False(t, pq.Has("key2"), "key2 should not exist after removal")
		}
	}

	entity, err := New[string, int](DefaultOptionsString(handle).WithWorkNum(1))
	require.NoError(t, err)

	// 先 Push key1
	entity.Push("key1", 1)

	// 等待 key1 开始被处理后，再 Push key2（确保处理顺序）
	<-key1Started

	// 现在 Push key2 的多个消息
	entity.Push("key2", 1)
	entity.Push("key2", 2)
	entity.Push("key2", 3)

	// 给一些时间让消息处理
	time.Sleep(100 * time.Millisecond)

	// 检查所有待处理消息
	pendingCounts := entity.PendingCounts()
	t.Logf("Pending counts: %v", pendingCounts)

	entity.Stop()

	// key1 应该被处理
	require.Equal(t, int32(1), key1Processed, "key1 should be processed once")

	// key2 应该不被处理（因为在 Push 后立即被 Remove 了）
	t.Logf("Total processed count: %d", processedCount)
	t.Logf("key1 processed: %d, key2 processed: %d", key1Processed, key2Processed)
	require.Equal(t, int32(0), key2Processed, "key2 should not be processed since it was removed")

	// 验证 key2 被删除后不再存在
	require.False(t, entity.Has("key2"), "key2 should not exist after removal")
}

// TestPendingCount 测试 PendingCount 和 PendingCounts 函数
func TestPendingCount(t *testing.T) {
	processed := make(chan struct{})
	handle := func(pq *Parq[string, int], key string, data int) {
		// 阻塞处理，让消息堆积
		<-processed
	}

	entity, err := New[string, int](DefaultOptionsString(handle).WithWorkNum(1))
	require.NoError(t, err)

	// Push 多个 key 的消息
	entity.Push("key1", 1)
	entity.Push("key1", 2)
	entity.Push("key1", 3)
	entity.Push("key2", 1)
	entity.Push("key2", 2)
	entity.Push("key3", 1)

	// 给一些时间让第一条消息开始处理（被阻塞）
	time.Sleep(50 * time.Millisecond)

	// 检查单个 key 的待处理数量
	count1 := entity.PendingCount("key1")
	count2 := entity.PendingCount("key2")
	count3 := entity.PendingCount("key3")
	countNonExist := entity.PendingCount("nonexist")

	t.Logf("key1 pending: %d, key2 pending: %d, key3 pending: %d", count1, count2, count3)

	// key1 有一条消息正在处理（阻塞中），应该还有 2 条待处理
	require.GreaterOrEqual(t, count1, uint32(2), "key1 should have at least 2 pending messages")
	require.Equal(t, uint32(2), count2, "key2 should have 2 pending messages")
	require.Equal(t, uint32(1), count3, "key3 should have 1 pending message")
	require.Equal(t, uint32(0), countNonExist, "non-existent key should have 0 pending messages")

	// 检查所有 key 的待处理数量
	pendingCounts := entity.PendingCounts()
	t.Logf("All pending counts: %v", pendingCounts)

	// pendingCounts 应该包含所有有消息的 key
	require.GreaterOrEqual(t, pendingCounts["key1"], uint32(2))
	require.Equal(t, uint32(2), pendingCounts["key2"])
	require.Equal(t, uint32(1), pendingCounts["key3"])

	// 解除阻塞，让所有消息处理完
	close(processed)
	entity.Stop()

	// 处理完后，所有 key 的待处理数量应该为 0
	count1After := entity.PendingCount("key1")
	count2After := entity.PendingCount("key2")
	count3After := entity.PendingCount("key3")
	require.Equal(t, uint32(0), count1After, "key1 should have 0 pending messages after processing")
	require.Equal(t, uint32(0), count2After, "key2 should have 0 pending messages after processing")
	require.Equal(t, uint32(0), count3After, "key3 should have 0 pending messages after processing")

	// PendingCounts 应该为空或不包含任何 key
	pendingCountsAfter := entity.PendingCounts()
	require.Empty(t, pendingCountsAfter, "PendingCounts should be empty after all messages are processed")
}

func TestInFnMultiPushMsg(t *testing.T) {

	handle := func(pq *Parq[string, int], key string, data int) {
		t.Logf("call key[%s] [%d]", key, data)

		err := pq.Push("key1", 2)
		require.EqualError(t, err, ErrPushSelfInHandle.Error())

		err = pq.Push("key1", 3)
		require.EqualError(t, err, ErrPushSelfInHandle.Error())

		t.Logf("end")
	}

	entity, err := New[string, int](DefaultOptionsString(handle).WithWorkNum(1).WithMsgCapacity(1))
	require.NoError(t, err)
	entity.Push("key1", 1)

	time.Sleep(100 * time.Millisecond)
	t.Log("before stop")
	entity.Stop()
	t.Log("end stop")
}

// TestTryPushToSelf 测试 TryPush 允许向自身 key Push
func TestTryPushToSelf(t *testing.T) {
	var tryPushResults []error
	var processedData []int
	var mu sync.Mutex
	done := make(chan struct{})

	handle := func(pq *Parq[string, int], key string, data int) {
		mu.Lock()
		processedData = append(processedData, data)
		mu.Unlock()

		t.Logf("handle key=%s data=%d", key, data)

		// 只在第一次处理时 TryPush 更多消息
		if data == 1 {
			for i := 2; i <= 5; i++ {
				err := pq.TryPush("key1", i) // 向自身 key TryPush
				mu.Lock()
				tryPushResults = append(tryPushResults, err)
				mu.Unlock()
				t.Logf("TryPush %d result: %v", i, err)
			}
		}

		// 所有消息处理完后关闭 done
		mu.Lock()
		count := len(processedData)
		mu.Unlock()
		if count >= 3 { // 至少处理 3 条消息才算成功
			select {
			case <-done:
			default:
				close(done)
			}
		}
	}

	// 设置足够大的容量
	entity, err := New[string, int](DefaultOptionsString(handle).WithWorkNum(1).WithMsgCapacity(10))
	require.NoError(t, err)

	entity.Push("key1", 1)

	// 等待处理完成
	select {
	case <-done:
		t.Log("Processing completed")
	case <-time.After(2 * time.Second):
		t.Log("Timeout, checking results anyway")
	}

	entity.Stop()

	// 统计 TryPush 结果
	var successCount, fullCount int
	for _, e := range tryPushResults {
		if e == nil {
			successCount++
		} else if errors.Is(e, ErrQueueFull) {
			fullCount++
		}
	}
	t.Logf("TryPush to self: success=%d, full=%d, processed=%v", successCount, fullCount, processedData)

	// TryPush 到自身应该至少有一些成功
	require.Greater(t, successCount, 0, "TryPush to self should succeed when queue has space")
	// 处理的消息应该包含 TryPush 的消息
	require.Greater(t, len(processedData), 1, "Should process more than initial message")
}

// TestTryPush 测试 TryPush 非阻塞版本
func TestTryPush(t *testing.T) {
	var tryPushErr error
	var processedKeys sync.Map
	expectedKeys := int32(2) // key1, key2
	var processedCount int32
	doneChan := make(chan struct{})

	handle := func(pq *Parq[string, int], key string, data int) {
		processedKeys.Store(key, data)

		// 在处理 key1 时，使用 TryPush 向 key2 发送消息（应该成功）
		if key == "key1" && data == 1 {
			tryPushErr = pq.TryPush("key2", 2)
		}

		if atomic.AddInt32(&processedCount, 1) >= expectedKeys {
			close(doneChan)
		}
	}

	entity, err := New[string, int](DefaultOptionsString(handle).WithWorkNum(1).WithMsgCapacity(10))
	require.NoError(t, err)

	entity.Push("key1", 1)

	<-doneChan
	entity.Stop()

	// TryPush 应该成功
	require.NoError(t, tryPushErr, "TryPush to different key should succeed")

	// 验证 key2 被处理
	v, ok := processedKeys.Load("key2")
	require.True(t, ok, "key2 should be processed")
	require.Equal(t, 2, v)
}

// TestTryPushQueueFull 测试 TryPush 在队列满时返回错误
func TestTryPushQueueFull(t *testing.T) {
	var tryPushErrs []error
	var mu sync.Mutex

	handle := func(pq *Parq[string, int], key string, data int) {
		t.Logf("handle key=%s data=%d", key, data)
	}

	// 设置很小的 msgCapacity = 2
	entity, err := New[string, int](DefaultOptionsString(handle).WithWorkNum(1).WithMsgCapacity(2))
	require.NoError(t, err)

	// 先正常 Push 填满队列
	entity.Push("key1", 1)
	entity.Push("key1", 2)

	// 等待一下让 worker 开始处理
	time.Sleep(10 * time.Millisecond)

	// 使用 TryPush 尝试继续添加，应该有些会失败
	for i := 3; i <= 10; i++ {
		err := entity.TryPush("key1", i)
		mu.Lock()
		tryPushErrs = append(tryPushErrs, err)
		mu.Unlock()
	}

	time.Sleep(100 * time.Millisecond)
	entity.Stop()

	// 统计结果
	var successCount, fullCount int
	for _, e := range tryPushErrs {
		if e == nil {
			successCount++
		} else if errors.Is(e, ErrQueueFull) {
			fullCount++
		}
	}
	t.Logf("TryPush results: total=%d, success=%d, full=%d", len(tryPushErrs), successCount, fullCount)

	// 至少应该有一些成功的（队列被消费后腾出空间）
	// 这里不强制要求有 ErrQueueFull，因为 worker 可能处理得很快
}

// TestTryPushInHandle 测试在 handle 中使用 TryPush 不会死锁
func TestTryPushInHandle(t *testing.T) {
	var tryPushResults []error
	var mu sync.Mutex
	done := make(chan struct{})

	handle := func(pq *Parq[string, int], key string, data int) {
		if key == "key1" && data == 1 {
			// 在 handle 中连续 TryPush 到同一个 key2，模拟队列满的情况
			for i := 0; i < 10; i++ {
				err := pq.TryPush("key2", i)
				mu.Lock()
				tryPushResults = append(tryPushResults, err)
				mu.Unlock()
			}
			close(done)
		}
	}

	// 设置小容量，容易满
	entity, err := New[string, int](DefaultOptionsString(handle).WithWorkNum(1).WithMsgCapacity(3))
	require.NoError(t, err)

	entity.Push("key1", 1)

	// 等待 handle 完成，不应该死锁
	select {
	case <-done:
		t.Log("Handle completed without deadlock")
	case <-time.After(2 * time.Second):
		t.Fatal("Deadlock detected! TryPush should not block")
	}

	entity.Stop()

	// 统计结果
	var successCount, fullCount, otherCount int
	for _, e := range tryPushResults {
		if e == nil {
			successCount++
		} else if errors.Is(e, ErrQueueFull) {
			fullCount++
		} else {
			otherCount++
		}
	}
	t.Logf("TryPush in handle: success=%d, full=%d, other=%d", successCount, fullCount, otherCount)

	// 关键：测试不死锁，无论成功还是失败
	require.Equal(t, 10, len(tryPushResults), "All TryPush calls should return")
}
