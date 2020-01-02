/*
-------------------------------------------------
   Author :       Zhang Fan
   date：         2020/1/2
   Description :
-------------------------------------------------
*/

package zssdb_consumer

type retQueue struct {
    Queue string
    Front bool
}

type Entry struct {
    Queue string // 队列名
    Data  []byte // 数据
    ret   *retQueue
}

// 归还
func (m *Entry) Return(queue string, front bool) {
    m.ret = &retQueue{
        Queue: queue,
        Front: front,
    }
}
