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

func MakeQueue(queue ...string) []*Queue {
    out := []*Queue{}
    for _, field := range queue {
        reverse := false
        if field != "" {
            switch field[0] {
            case '+':
                field = field[1:]
            case '-':
                reverse = true
                field = field[1:]
            }
        }
        if field == "" {
            panic("空队列名")
        }
        out = append(out, &Queue{Name: field, PopReverse: reverse})
    }
    return out
}
