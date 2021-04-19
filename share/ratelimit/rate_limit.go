package ratelimit

import (
	"errors"
	"time"
)

type RateLimiter interface {
	// Acquire 获得许可，会阻塞直到获得许可
	Acquire()
	// TryAcquire 尝试获取许可，如果不成功会立即返回 false，而不是一直阻塞
	TryAcquire() bool
	// AcquireWithTimeout 获取许可，会阻塞直到获得许可或者超时，超时会返回一个超时异常，成功返回 nil
	AcquireWithTimeout(timeout time.Duration) error
}

type DefaultRateLimiter struct {
	Num int64
	// 漏桶法，按照一定的速率向通道中投放数据，后面通过从通道中取数据获得许可
	rateLimiter chan time.Time
}

func NewRateLimiter(numPerSecond int64) RateLimiter {
	r := new(DefaultRateLimiter)
	r.Num = numPerSecond
	r.rateLimiter = make(chan time.Time)
	go func() {
		d := time.Duration(numPerSecond)
		ticker := time.NewTicker(time.Second / d)
		for t := range ticker.C {
			r.rateLimiter <- t
		}
	}()

	// goroutine 无法执行 bug ：休眠一秒钟使 goroutine 能够进行调度
	time.Sleep(time.Second)

	return r
}

func (r *DefaultRateLimiter) Acquire() {
	// 阻塞直到 r.rateLimiter 中有数据
	<-r.rateLimiter
}

func (r *DefaultRateLimiter) TryAcquire() bool {
	select {
	case <-r.rateLimiter:
		return true
	default:
		return false
	}
}

func (r *DefaultRateLimiter) AcquireWithTimeout(timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	select {
	case <-r.rateLimiter:
		return nil
	case <-timer.C:
		return errors.New("acquire timeout")
	}
}
