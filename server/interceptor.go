package server

import (
	"context"
	"log"
	"mrpc/protocol"
	"mrpc/registry"
	"mrpc/transport"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
)

// 实现一个默认的过滤器

type DefaultServerWrapper struct {
}

func (w *DefaultServerWrapper) WrapServe(s *SGServer, serveFunc ServeFunc) ServeFunc {
	return func(network, addr string) error {
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

		provider := registry.Provider{
			ProviderKey: network + "@" + addr,
			Network:     network,
			Addr:        addr,
			Meta:        map[string]interface{}{"services": s.Service()},
		}
		r := s.Option.Registry
		rOpt := s.Option.RegisterOption

		r.Register(rOpt, provider)
		log.Printf("registered provider %v for app %s", provider, rOpt)

		return serveFunc(network, addr)

	}
}

func (w *DefaultServerWrapper) WrapServeTransport(s *SGServer, transportFunc ServeTransportFunc) ServeTransportFunc {
	return transportFunc
}

func (w *DefaultServerWrapper) WrapHandleRequest(s *SGServer, requestFunc HandleRequestFunc) HandleRequestFunc {
	return func(ctx context.Context, request *protocol.Message, response *protocol.Message, tr transport.Transport) {
		// 请求计数
		atomic.AddInt64(&s.requestInProcess, 1)
		requestFunc(ctx, request, response, tr)
		atomic.AddInt64(&s.requestInProcess, -1)
	}
}

func (w *DefaultServerWrapper) WrapClose(s *SGServer, closeFunc CloseFunc) CloseFunc {
	return func() error {
		provider := registry.Provider{
			ProviderKey: s.network + "@" + s.addr,
			Network:     s.network,
			Addr:        s.addr,
		}
		r := s.Option.Registry
		rOpt := s.Option.RegisterOption
		r.Unregister(rOpt, provider)
		log.Printf("unregistered provider %v for app %s", provider, rOpt)
		return closeFunc()
	}
}

type ServiceAuthWrapper struct {
	authFunc AuthFunc
}

func (saw *ServiceAuthWrapper) WrapServe(s *SGServer, serverFunc ServeFunc) ServeFunc {
	return serverFunc
}

func (saw *ServiceAuthWrapper) WrapServeTransport(s *SGServer, transportFunc ServeTransportFunc) ServeTransportFunc {
	return transportFunc
}

func (saw *ServiceAuthWrapper) WrapHandleRequest(s *SGServer, requestFunc HandleRequestFunc) HandleRequestFunc {
	return func(ctx context.Context, request *protocol.Message, response *protocol.Message, tr transport.Transport) {
		if auth, ok := ctx.Value(protocol.AuthKey).(string); ok {
			// 鉴权通过则则执行业务逻辑
			if saw.authFunc(auth) {
				requestFunc(ctx, request, response, tr)
				return
			}
		}
		// 鉴权失败则返回异常
		s.writeErrorResponse(response, tr, "auth failed")
	}
}

func (saw *ServiceAuthWrapper) WrapClose(s *SGServer, closeFunc CloseFunc) CloseFunc {
	return closeFunc
}
