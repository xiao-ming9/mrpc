package server

import (
	"context"
	"mrpc/protocol"
	"mrpc/transport"
)

// 服务端切面

type ServeFunc func(network, addr string, meta map[string]interface{}) error
type ServeTransportFunc func(tr transport.Transport)
type HandleRequestFunc func(ctx context.Context, request *protocol.Message,
	response *protocol.Message, tr transport.Transport)
type CloseFunc func() error
type AuthFunc func(key string) bool

// Wrapper 服务端拦截器
type Wrapper interface {
	WrapServe(s *SGServer, serverFunc ServeFunc) ServeFunc
	WrapServeTransport(s *SGServer, transportFunc ServeTransportFunc) ServeTransportFunc
	WrapHandleRequest(s *SGServer, requestFunc HandleRequestFunc) HandleRequestFunc
	WrapClose(s *SGServer, closeFunc CloseFunc) CloseFunc
}

// 默认的服务端拦截器
type defaultServerWrapper struct {
}

func (d *defaultServerWrapper) WrapServe(s *SGServer, serverFunc ServeFunc) ServeFunc {
	return serverFunc
}

func (d *defaultServerWrapper) WrapServeTransport(s *SGServer, transportFunc ServeTransportFunc) ServeTransportFunc {
	return transportFunc
}

func (d *defaultServerWrapper) WrapHandleRequest(s *SGServer, requestFunc HandleRequestFunc) HandleRequestFunc {
	return requestFunc
}

func (d *defaultServerWrapper) WrapClose(s *SGServer, closeFunc CloseFunc) CloseFunc {
	return closeFunc
}
