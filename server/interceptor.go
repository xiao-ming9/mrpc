package server

import (
	"context"
	"log"
	"mrpc/protocol"
	"mrpc/registry"
	"mrpc/share/metadata"
	"mrpc/transport"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
)

// DefaultServerWrapper 实现一个默认的过滤器
type DefaultServerWrapper struct {
	defaultServerWrapper
}

func (w *DefaultServerWrapper) WrapServe(s *SGServer, serveFunc ServeFunc) ServeFunc {
	return func(network, addr string, meta map[string]interface{}) error {
		// 注册 shutdownHook
		go func(s *SGServer) {
			ch := make(chan os.Signal, 1)
			// 将系统调用的信号发送到信道
			signal.Notify(ch, syscall.SIGTERM)
			// 此处会阻塞直到接收到信号为止，用于监听退出信号
			sig := <-ch
			if sig.String() == "terminated" {
				for _, hook := range s.Option.ShutDownHooks {
					hook(s)
				}
				os.Exit(0)
			}
		}(s)

		if meta == nil {
			meta = make(map[string]interface{})
		}
		// 注入 tags
		if len(s.Option.Tags) > 0 {
			meta["tags"] = s.Option.Tags
		}
		meta["services"] = s.Service()

		provider := registry.Provider{
			ProviderKey: network + "@" + addr,
			Network:     network,
			Addr:        addr,
			Meta:        meta,
		}
		r := s.Option.Registry
		rOpt := s.Option.RegisterOption

		// 注册到注册中心
		r.Register(rOpt, provider)
		log.Printf("registered provider %v for app %s", provider, rOpt)

		// 启动 gateway
		s.startGateway()
		return serveFunc(network, addr, meta)
	}
}

func (w *DefaultServerWrapper) WrapServeTransport(s *SGServer,
	transportFunc ServeTransportFunc) ServeTransportFunc {
	return transportFunc
}

func (w *DefaultServerWrapper) WrapHandleRequest(s *SGServer, requestFunc HandleRequestFunc) HandleRequestFunc {
	return func(ctx context.Context, request *protocol.Message, response *protocol.Message, tr transport.Transport) {
		ctx = metadata.WithMeta(ctx, request.MetaData)
		// 请求计数
		atomic.AddInt64(&s.RequestInProcess, 1)
		requestFunc(ctx, request, response, tr)
		atomic.AddInt64(&s.RequestInProcess, -1)
	}
}

func (w *DefaultServerWrapper) WrapClose(s *SGServer, closeFunc CloseFunc) CloseFunc {
	return func() error {
		provider := registry.Provider{
			ProviderKey: s.Network + "@" + s.Addr,
			Network:     s.Network,
			Addr:        s.Addr,
		}
		r := s.Option.Registry
		rOpt := s.Option.RegisterOption
		r.Unregister(rOpt, provider)
		//log.Printf("unregistered provider %v for app %s", provider, rOpt)
		return closeFunc()
	}
}
