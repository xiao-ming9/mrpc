package server

import (
	"context"
	"encoding/json"
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
			ch := make(chan os.Signal,1)
			// 将系统调用的信号发送到信道
			signal.Notify(ch,syscall.SIGTERM)
			// 此处会阻塞直到接收到信号为止，用于监听退出信号
			sig := <-ch
			if sig.String() == "terminated" {
				for _,hook := range s.Option.ShutDownHooks {
					hook(s)
				}
				os.Exit(0)
			}
		}(s)

		serviceInfo, _ := json.Marshal(s.Service())
		provider := registry.Provider{
			ProviderKey: network+"@"+addr,
			Network: network,
			Addr:addr,
			Meta:map[string]string{"services":string(serviceInfo)},
		}
		r := s.Option.Registry
		rOpt := s.Option.RegisterOption

		r.Register(rOpt,provider)
		log.Printf("registered provider %v for app %s", provider, rOpt)

		return serveFunc(network,addr)

	}
}

func (w *DefaultServerWrapper) WrapServeTransport(s *SGServer, transportFunc ServeTransportFunc) ServeTransportFunc {
	return transportFunc
}

func (w *DefaultServerWrapper) WrapHandleRequest(s *SGServer, requestFunc HandleRequestFunc) HandleRequestFunc {
	return func(ctx context.Context, request *protocol.Message, response *protocol.Message, tr transport.Transport) {
		// 请求计数
		atomic.AddInt64(&s.requestInProcess,1)
		requestFunc(ctx,request,response,tr)
		atomic.AddInt64(&s.requestInProcess,-1)
	}
}

