/*
-------------------------------------------------
   Author :       Zhang Fan
   date：         2020/1/2
   Description :
-------------------------------------------------
*/

package zssdb_consumer

type Queue struct {
    Name       string // 队列名
    PopReverse bool   // 是否从尾部开始pop
}
