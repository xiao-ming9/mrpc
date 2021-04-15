package server

import (
	"context"
	"mrpc/protocol"
	"mrpc/share/ratelimit"
	"mrpc/transport"
)

type RequestRateLimitWrapper struct {
	defaultServerWrapper
	Limiter ratelimit.RateLimiter
}

func (rl *RequestRateLimitWrapper) WrapHandleRequest(s *SGServer,
	requestFunc HandleRequestFunc) HandleRequestFunc {
	return func(ctx context.Context, request *protocol.Message, response *protocol.Message, tr transport.Transport) {
		if rl.Limiter != nil {
			// 尝试进行获取，获取失败则直接返回限流异常
			if rl.Limiter.TryAcquire() {
				requestFunc(ctx, request, response, tr)
			} else {
				s.writeErrorResponse(response, tr, "request limited")
			}
		} else {
			// 如果限流器为 nil 则直接返回
			requestFunc(ctx, request, response, tr)
		}
	}
}
