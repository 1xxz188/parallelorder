package parallelorder

import (
	"fmt"
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/stretchr/testify/require"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDemo(t *testing.T) {
	exitChan := make(chan struct{})
	fn := func(key string, data string) {
		fmt.Println(key, data)
		close(exitChan)
	}

	entity, err := New(DefaultOptions(fn))
	require.NoError(t, err)

	err = entity.Push("key", "value")
	require.NoError(t, err)
	<-exitChan
}

func TestMultiPushMsg(t *testing.T) {
	//var r = rand.New(rand.NewSource(time.Now().Unix()))
	cmap.SHARD_COUNT = 256

	msgCnt := 1024 * 2
	sendGoCnt := 1000
	testMap := cmap.New[int]()

	var revCnt int32
	var sendCnt int32
	shouldRevCnt := int32(msgCnt * sendGoCnt)
	overChan := make(chan struct{})

	fn := func(key string, data int) {
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

	entity, err := New(DefaultOptions(fn))
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
