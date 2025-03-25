package oo

import (
	"github.com/garyburd/redigo/redis"
)

type DistributedLock struct {
	conn RedisConn
	lock *RedisLock
}

func NewDistributedLock(key string) (ret *DistributedLock, conn RedisConn, err error) {
	conn = GRedisPool.GetConn()

	lock, err := InitRedisLock(conn, key)
	if nil != err {
		err = NewError("InitRedisLock key[%s] %v", key, err)
		return
	}

	ret = &DistributedLock{
		conn: conn,
		lock: lock,
	}

	return
}

func (this *DistributedLock) Unlock() {
	this.lock.UnLock()
	GRedisPool.UnGetConn(this.conn)
}

// redis utils
func RedisExec(cmd string, args ...interface{}) (reply interface{}, err error) {
	rconn := GRedisPool.GetConn()
	defer GRedisPool.UnGetConn(rconn)

	reply, err = rconn.Do(cmd, args...)
	if nil != err {
		return
	}

	return
}

func RedisExecInt64(cmd string, args ...interface{}) (ret int64, err error) {
	ret, err = redis.Int64(RedisExec(cmd, args...))
	return
}

func RedisExecString(cmd string, args ...interface{}) (ret string, err error) {
	ret, err = redis.String(RedisExec(cmd, args...))
	return
}

func RedisExecStrings(cmd string, args ...interface{}) (ret []string, err error) {
	ret, err = redis.Strings(RedisExec(cmd, args...))
	return
}

func RedisExecInt64Map(cmd string, args ...interface{}) (ret map[string]int64, err error) {
	ret, err = redis.Int64Map(RedisExec(cmd, args...))
	return
}

func RedisExecParse(val interface{}, cmd string, args ...interface{}) (err error) {
	str, err := redis.String(RedisExec(cmd, args...))
	if nil != err {
		return
	}
	err = JsonUnmarshal([]byte(str), val)

	return
}
