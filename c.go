/*
-------------------------------------------------
   Author :       Zhang Fan
   date：         2020/1/2
   Description :
-------------------------------------------------
*/

package zssdb_consumer

import (
    "fmt"
    "time"

    "github.com/seefan/gossdb"
    "github.com/zlyuancn/zerrors"
    "github.com/zlyuancn/zlog2"
)

type SsdbConsumer struct {
    exit      chan struct{} // 退出消费者
    done      chan struct{} // 完成退出
    cache     chan *Entry
    old_queue *Queue // 上一次取出成功的队列

    c          *gossdb.Connectors
    queue      []*Queue
    batch_size int64
    consumer   Consumer
    empty_wait time.Duration
    err_wait   time.Duration
    log        Loger
}

func New(conf SsdbConsumerConfig) *SsdbConsumer {
    if conf.CacheCount <= 0 {
        conf.CacheCount = DefaultCacheCount
    }
    if conf.PopBatchSize <= 0 {
        conf.PopBatchSize = DefaultPopBatchSize
    }
    if conf.Consumer == nil {
        panic("消费者是空的")
    }
    if conf.EmptyWait <= 0 {
        conf.EmptyWait = DefaultEmptyWait
    }
    if conf.ErrWait <= 0 {
        conf.ErrWait = DefaultErrWait
    }
    if conf.Log == nil {
        conf.Log = zlog2.DefaultLogger
    }

    m := &SsdbConsumer{
        exit:      make(chan struct{}),
        done:      make(chan struct{}),
        cache:     make(chan *Entry, conf.CacheCount),
        old_queue: nil,

        c:          conf.SsdbConnectors,
        queue:      conf.Queue,
        batch_size: conf.PopBatchSize,
        consumer:   conf.Consumer,
        empty_wait: conf.EmptyWait,
        err_wait:   conf.ErrWait,
        log:        conf.Log,
    }
    return m
}

func (m *SsdbConsumer) Start() {
    go m.start()
    go m.consume()
}

func (m *SsdbConsumer) _pop(c *gossdb.Client, queue *Queue) ([]*Entry, error) {
    vs, err := c.QpopArray(queue.Name, m.batch_size, queue.PopReverse)
    if err != nil {
        return nil, zerrors.WrapSimple(err, "pop失败")
    }
    if len(vs) == 0 {
        return nil, nil
    }

    out := make([]*Entry, len(vs))
    for i, v := range vs {
        out[i] = &Entry{
            Queue: queue.Name,
            Data:  v.Bytes(),
        }
    }

    return out, nil
}

func (m *SsdbConsumer) pop() ([]*Entry, error) {
    client, err := m.c.NewClient()
    if err != nil {
        return nil, zerrors.WrapSimple(err, "获取ssdb客户端失败")
    }
    defer client.Close()

    if m.old_queue != nil {
        datas, err := m._pop(client, m.old_queue)
        if err != nil {
            return nil, err
        }
        if len(datas) > 0 {
            return datas, nil
        }
    }

    for _, q := range m.queue {
        if q == m.old_queue {
            continue
        }

        datas, err := m._pop(client, q)
        if err != nil {
            return nil, err
        }
        if len(datas) > 0 {
            m.old_queue = q
            return datas, nil
        }
    }
    return nil, nil
}

func (m *SsdbConsumer) start() {
    effective_time := time.Now().Unix()

    for {
        select {
        case <-m.done:
            m.exit <- struct{}{}
            <-m.exit
            m._return()
            m.done <- struct{}{}
            return
        default:
            // 检查生效时间
            if time.Now().Unix() < effective_time {
                time.Sleep(DefaultTimeAccuracy)
                continue
            }

            datas, err := m.pop()
            if err != nil {
                effective_time = time.Now().Unix() + int64(m.err_wait/1e9)
                m.log.Warn(err)
                continue
            }

            if len(datas) == 0 {
                effective_time = time.Now().Unix() + int64(m.empty_wait/1e9)
                m.log.Info("空队列")
            } else {
                for _, v := range datas {
                    m.cache <- v
                }
            }
        }
    }
}

// 返还未消费的数据
func (m *SsdbConsumer) _return() {
    client, err := m.c.NewClient()
    if err != nil {
        m.log.Error(zerrors.ToDetailString(zerrors.WrapSimple(err, "获取ssdb客户端失败")))
        return
    }
    defer client.Close()

    queues := make(map[string]bool)
    for _, q := range m.queue {
        queues[q.Name] = q.PopReverse
    }

    var datas []*Entry
    for {
        select {
        case data := <-m.cache:
            datas = append(datas, data)
        default:
            if len(datas) == 0 {
                return
            }

            // 尽可能保证pop之前的顺序
            // todo 优化批量返还减少接口调用次数
            m.log.Info(fmt.Sprintf("准备归还数据, 共计 %d 条", len(datas)))
            errcount := 0
            for i := len(datas) - 1; i >= 0; i-- {
                data := datas[i]
                var err error

                reverse, _ := queues[data.Queue]
                if reverse {
                    _, err = client.Qpush_back(data.Queue, data.Data)
                } else {
                    _, err = client.Qpush_front(data.Queue, data.Data)
                }

                if err != nil {
                    errcount++
                    m.log.Error(fmt.Errorf("归还数据失败: %s", err))
                }
            }

            if errcount > 0 {
                m.log.Error(fmt.Errorf("归还数据失败 %d 条", errcount))
                return
            }

            m.log.Info("数据归还完毕")
            return
        }
    }
}

func (m *SsdbConsumer) consume() {
    for {
        select {
        case data := <-m.cache:
            m.process(data)
        case <-m.exit:
            m.exit <- struct{}{}
            return
        }
    }
}

func (m *SsdbConsumer) process(data *Entry) {
    if err := m.consumer.Process(data); err != nil {
        m.log.Warn(zerrors.ToDetailString(zerrors.WrapSimplef(err, "[%s]处理失败", data.Queue)))
    }

    if data.ret == nil {
        return
    }

    client, err := m.c.NewClient()
    if err != nil {
        m.log.Error(zerrors.ToDetailString(zerrors.WrapSimple(err, "获取ssdb客户端失败")))
        return
    }
    defer client.Close()

    if data.ret.Front {
        _, err = client.Qpush_front(data.Queue, data.Data)
    } else {
        _, err = client.Qpush_back(data.Queue, data.Data)
    }

    if err != nil {
        m.log.Warn(zerrors.ToDetailString(zerrors.WrapSimplef(err, "[%s]归还数据失败", data.Queue)))
    } else {
        m.log.Warn(fmt.Sprintf("[%s]归还数据成功", data.Queue))
    }
}

func (m *SsdbConsumer) Close() {
    m.done <- struct{}{}
    <-m.done
}
