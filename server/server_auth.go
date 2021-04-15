package server

import (
	"context"
	"mrpc/protocol"
	"mrpc/transport"
)

type ServerAuthWrapper struct {
	defaultServerWrapper
	authFunc AuthFunc
}

func (sa *ServerAuthWrapper) WrapHandleRequest(s *SGServer,
	requestFunc HandleRequestFunc) HandleRequestFunc {
	return func(ctx context.Context, request *protocol.Message, response *protocol.Message, tr transport.Transport) {
		if auth, ok := ctx.Value(protocol.AuthKey).(string); ok {
			// 鉴权成功则执行业务逻辑
			if sa.authFunc(auth) {
				requestFunc(ctx, request, response, tr)
				return
			}
		}
		// 鉴权失败则返回异常
		s.writeErrorResponse(response, tr, "auth failed")
	}
}
