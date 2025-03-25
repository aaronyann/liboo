package oo

import (
	"errors"
	"fmt"
	"time"

	"github.com/garyburd/redigo/redis"
)

type RedisPool redis.Pool
type RedisConn redis.Conn
type RedisLock struct {
	conn   RedisConn
	strkey string
}

var GRedisPool *RedisPool

var ErrNil error = redis.ErrNil

var (
	RedisDialUseTLS = false
)

type RedisCfg struct {
	Host string `toml:"host,omitzero"`
	Port int64  `toml:"port,omitzero"`
	Pass string `toml:"pass,omitzero"`
}

func InitRedisPool(redisHost string, redisPort int32, redisPassword string) (p *RedisPool, e error) {
	cfg := RedisCfg{
		Host: redisHost,
		Port: int64(redisPort),
		Pass: redisPassword,
	}
	return InitRedisPool2(&cfg)
}

func InitRedisPool2(cfg *RedisCfg) (p *RedisPool, e error) {
	redisServer := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	p = (*RedisPool)(&redis.Pool{
		MaxIdle:     100,
		MaxActive:   0, //when zero,there's no limit. https://godoc.org/github.com/garyburd/redigo/redis#Pool
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", redisServer,
				redis.DialConnectTimeout(time.Second*2),
				redis.DialReadTimeout(time.Second*3),
				redis.DialWriteTimeout(time.Second*3),
				redis.DialUseTLS(RedisDialUseTLS))
			if err != nil {
				return nil, err
			}
			if cfg.Pass != "" {
				if _, err := c.Do("AUTH", cfg.Pass); err != nil {
					c.Close()
					return nil, err
				}
			}

			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	})

	if GRedisPool == nil {
		GRedisPool = p
	}

	return p, nil
}

func (p *RedisPool) GetConn() RedisConn {

	return (*redis.Pool)(p).Get().(RedisConn)
}

func (p *RedisPool) UnGetConn(c RedisConn) {
	if c != nil {
		c.Close()
	}
}

func (p *RedisPool) SingleHmset(key string, vals interface{}) (err error) {
	c := p.GetConn()
	_, err = c.Do("HMSET", redis.Args{}.Add(key).AddFlat(vals)...)
	p.UnGetConn(c)
	return
}

func (p *RedisPool) SingleHgetall(key string, pret interface{}) (err error) {
	c := p.GetConn()
	v, err := redis.Values(c.Do("HGETALL", key))
	p.UnGetConn(c)
	if err == nil {
		if len(v) == 0 {
			err = redis.ErrNil
		} else {
			err = redis.ScanStruct(v, pret)
		}
	}
	return
}

func (p *RedisPool) RedisExec(cmd string, args ...interface{}) (reply interface{}, err error) {
	c := p.GetConn()
	defer p.UnGetConn(c)

	reply, err = c.Do(cmd, args...)
	if nil != err {
		err = NewError("do key[%s] args%v", cmd, args)
		return
	}

	return
}

func (p *RedisPool) SingleDel(key string) (err error) {
	c := p.GetConn()
	_, err = c.Do("DEL", key)
	p.UnGetConn(c)
	return
}

func InitRedisLock(c RedisConn, strkey string) (*RedisLock, error) {
	if c.Err() != nil {
		return nil, NewError("conn is nil")
	}
	loops := uint32(3)

	// strkey := fmt.Sprintf("public.redislock."+lockKey, args...)

	//LogD("Begin lock for player: %s", strkey)
	nowtime := time.Now().Unix()
	lockret := false
	for loop := uint32(0); loop < loops && lockret == false; loop++ {
		//LogD("Begin lock for player: %s, now:%d, loop:%d", strkey, nowtime, loop)
		if loop > 0 {
			time.Sleep(10 * time.Nanosecond)
		}
		ret, err := redis.Int(c.Do("SETNX", strkey, nowtime))
		if err != nil {
			//logs.ERRORLOG("conn.Do SETNX %s Failed: %s, loop:%d", strkey, err.Error(), loop)
			continue
		}
		if ret == 1 {
			//	LogD("Lock for %s as first setnx, loop:%d", strkey, loop)
			lockret = true
			break
		}
		oldval, err := redis.Int64(c.Do("GET", strkey))
		if err != nil {
			//logs.ERRORLOG("conn.Do GET %s Failed: %s, loop:%d", strkey, err.Error(), loop)
			continue
		}
		//LogD("lock %s is locked by other at:%d", strkey, oldval)
		if oldval+60 > nowtime {
			//return NewError("lock %s is locked by other and not timeout:%d, loop:%d", strkey, oldval, loop)
			continue
		}
		oldval2, err := redis.Int64(c.Do("GETSET", strkey, nowtime))
		if err != nil {
			return nil, NewError("conn.Do GETSET %s Failed: %s, loop:%d", strkey, err.Error(), loop)
		}
		if oldval != oldval2 {
			//return NewError( "lock %s is locked by other at :%d, loop:%d", strkey, oldval2, loop)
			continue
		}
		lockret = true
		break
	}
	if !lockret {
		return nil, errors.New(fmt.Sprintf("try 3 times fail."))
	}

	//LogD("end lock for player: %s", strkey)
	return &RedisLock{c, strkey}, nil
}

func (l *RedisLock) UnLock() error {
	if l.conn.Err() != nil {
		return NewError("conn is nil")
	}
	//LogD("Begin unlock for player:%s", l.strkey)
	_, err := redis.Int64(l.conn.Do("DEL", l.strkey))
	if err != nil {
		//logs.ERRORLOG("conn.Do DEL %s Failed: %s", strkey, err.Error())
	}
	//LogD("end unlock for player:%s", strkey)
	return nil
}

type RedisPipeline struct {
	r    redis.Conn
	cmds []string
	args [][]interface{}
}

func NewRedisPipeline(r redis.Conn) (ret *RedisPipeline) {
	if nil == r {
		r = GRedisPool.GetConn()
	}
	return &RedisPipeline{
		r: r,
	}
}

func (this *RedisPipeline) AddCommand(cmd string, args ...interface{}) {
	this.cmds = append(this.cmds, cmd)
	this.args = append(this.args, args)
}

func (this *RedisPipeline) Exec() (ret []interface{}, err error) {
	_, err = this.r.Do("MULTI")
	if nil != err {
		return
	}
	for i, cmd := range this.cmds {
		_, err = this.r.Do(cmd, this.args[i]...)
		if nil != err {
			return
		}
	}
	ret, err = redis.Values(this.r.Do("EXEC"))
	return
}
