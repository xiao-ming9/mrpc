package client

import (
	"context"
	"errors"
	"mrpc/share/ratelimit"
)

type RateLimitWrapper struct {
	// 内嵌 defaultClientWrapper，其实现了 Wrapper 的所有方法，所以只需要覆盖要实现的方法即可
	defaultClientWrapper
	Limit ratelimit.RateLimiter
}

func NewRateLimitWrapper(numPerSecond int64) Wrapper {
	return &RateLimitWrapper{
		Limit: ratelimit.NewRateLimiter(numPerSecond),
	}
}

var ErrRateLimited = errors.New("request limited")

func (r *RateLimitWrapper) WrapCall(option *SGOption, callFunc CallFunc) CallFunc {
	return func(ctx context.Context, ServiceMethod string, arg, reply interface{}) error {
		if r.Limit != nil {
			if r.Limit.TryAcquire() {
				return callFunc(ctx, ServiceMethod, arg, reply)
			} else {
				return ErrRateLimited
			}
		} else {
			return callFunc(ctx, ServiceMethod, arg, reply)
		}
	}

}

func (r *RateLimitWrapper) WrapGo(option *SGOption, goFunc GoFunc) GoFunc {
	return func(ctx context.Context, ServiceMethod string, arg, reply interface{}, done chan *Call) *Call {
		if r.Limit != nil {
			if r.Limit.TryAcquire() {
				return goFunc(ctx, ServiceMethod, arg, reply, done)
			} else {
				call := &Call{
					ServiceMethod: ServiceMethod,
					Args:          arg,
					Reply:         reply,
					Error:         ErrRateLimited,
					Done:          done,
				}
				done <- call
				return call
			}
		} else {
			return goFunc(ctx, ServiceMethod, arg, reply, done)
		}
	}
}
