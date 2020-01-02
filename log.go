/*
-------------------------------------------------
   Author :       Zhang Fan
   date：         2020/1/2
   Description :
-------------------------------------------------
*/

package zssdb_consumer

type Loger interface {
    Info(v ...interface{})
    Warn(v ...interface{})
    Error(v ...interface{})
}
