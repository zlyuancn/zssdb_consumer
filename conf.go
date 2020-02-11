/*
-------------------------------------------------
   Author :       Zhang Fan
   date：         2020/1/2
   Description :
-------------------------------------------------
*/

package zssdb_consumer

import (
    "time"

    "github.com/seefan/gossdb"
    "github.com/zlyuancn/zlog2"
)

var DefaultSsdbConsumerConfig = SsdbConsumerConfig{
    SsdbConnectors: nil,
    Queue:          nil,
    CacheCount:     DefaultCacheCount,
    PopBatchSize:   DefaultPopBatchSize,
    Consumer:       nil,
    EmptyWait:      DefaultEmptyWait,
    ErrWait:        DefaultErrWait,
    Log:            zlog2.DefaultLogger,
}

type SsdbConsumerConfig struct {
    SsdbConnectors *gossdb.Connectors // ssdb客户端连接器
    Queue          []*Queue           // 消费队列
    CacheCount     int                // 缓存数
    PopBatchSize   int64              // 每次pop批量数
    Consumer       Consumer           // 消费者
    EmptyWait      time.Duration      // 空队列等待时间
    ErrWait        time.Duration      // 错误等待时间
    Log            Loger
}
