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
)

const (
    DefaultCacheCount   = 100                    // 默认缓存数
    DefaultPopBatchSize = 10                     // 每次pop批量数
    DefaultEmptyWait    = time.Second * 60       // 默认空队列等待时间
    DefaultErrWait      = time.Second * 5        // 默认错误等待时间
    DefaultTimeAccuracy = time.Millisecond * 500 // 时间精度
)

type Consumer interface {
    Process(entry *Entry) error
}
