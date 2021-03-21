package client

import (
	"sync/atomic"
	"time"
)

// CircuitBreaker 熔断器
type CircuitBreaker interface {
	AllowRequest() bool
	Success()
	Fail()
}

// DefaultCircuitBreaker 实现了简单的基于时间窗口的熔断器
type DefaultCircuitBreaker struct {
	lastFail  time.Time
	fails     uint64
	threshold uint64
	window    time.Duration
}

func NewDefaultCircuitBreaker(threshold uint64, window time.Duration) *DefaultCircuitBreaker {
	return &DefaultCircuitBreaker{
		threshold: threshold,
		window:    window,
	}
}

func (d *DefaultCircuitBreaker) AllowRequest() bool {
	if time.Since(d.lastFail) > d.window {
		d.reset()
		return true
	}

	failures := atomic.LoadUint64(&d.fails)
	return failures < d.threshold
}

func (d *DefaultCircuitBreaker) Success() {
	d.reset()
}

func (d *DefaultCircuitBreaker) Fail() {
	atomic.AddUint64(&d.fails, 1)
	d.lastFail = time.Now()
}

func (d *DefaultCircuitBreaker) reset() {
	atomic.StoreUint64(&d.fails, 0)
	d.lastFail = time.Now()
}
