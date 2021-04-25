package client

import (
	"context"
	"errors"
	"log"
	"mrpc/protocol"
	"mrpc/registry"
	"mrpc/selector"
	"sync"
	"time"
)

// SGClient SG(service governance) 表示服务治理
type SGClient interface {
	Go(ctx context.Context, ServiceMethod string, arg, reply interface{}, done chan *Call) (*Call, error)
	Call(ctx context.Context, ServiceMethod string, arg, reply interface{}) error
	Close() error
}

type sgClient struct {
	shutdown bool
	option   SGOption

	mu                   sync.Mutex // 主要用于 clients 的相关操作(心跳，close等)
	clients              sync.Map   // clients 维护了客户端到服务端的长链接,map[string]RPCClient
	clientsHeartbeatFail map[string]int
	breakers             sync.Map // map[string]CircuitBreaker
	watcher              registry.Watcher

	serversMu sync.RWMutex
	servers   []registry.Provider
}

func NewSGClient(option SGOption) SGClient {
	s := new(sgClient)
	s.option = option

	providers := s.option.Registry.GetServiceList()
	s.watcher = s.option.Registry.Watch()
	go s.watchService(s.watcher)

	s.serversMu.Lock()
	defer s.serversMu.Unlock()
	for _, p := range providers {
		s.servers = append(s.servers, p)
	}
	if s.option.Heartbeat {
		go s.heartbeat()
		s.option.SelectOption.Filters = append(s.option.SelectOption.Filters, selector.DegradeProviderFilter())
	}

	if s.option.Tagged && s.option.Tags != nil {
		s.option.SelectOption.Filters = append(s.option.SelectOption.Filters,
			selector.TaggedProviderFilter(s.option.Tags))
	}

	return s
}

func (s *sgClient) Go(ctx context.Context, ServiceMethod string, arg, reply interface{}, done chan *Call) (*Call, error) {
	if s.shutdown {
		return nil, ErrorShutdown
	}

	_, client, err := s.selectClient(ctx, ServiceMethod, arg)

	if err != nil {
		return nil, err
	}
	return s.wrapGo(client.Go)(ctx, ServiceMethod, arg, reply, done), nil
}

// ServiceError is an error from server
type ServiceError string

func (e ServiceError) Error() string {
	return string(e)
}

func (s *sgClient) Call(ctx context.Context, ServiceMethod string, arg, reply interface{}) error {
	provider, rpcClient, err := s.selectClient(ctx, ServiceMethod, arg)

	if err != nil && s.option.FailMode == FailFast {
		return err
	}

	var connectErr error
	switch s.option.FailMode {
	case FailRetry:
		retries := s.option.Retries
		for retries > 0 {
			retries--

			if rpcClient != nil {
				err = s.wrapCall(rpcClient.Call)(ctx, ServiceMethod, arg, reply)
				if err == nil {
					s.updateBreaker(provider.ProviderKey, true)
					return err
				} else {
					if _, ok := err.(ServiceError); ok {
						s.updateBreaker(provider.ProviderKey, false)
						return err
					}
				}
			}

			s.removeClient(provider.ProviderKey, rpcClient)
			rpcClient, connectErr = s.getClient(provider)
		}

		if err == nil {
			err = connectErr
		}
		if err == nil {
			s.updateBreaker(provider.ProviderKey, true)
		} else {
			s.updateBreaker(provider.ProviderKey, false)
		}

		return err

	case FailOver:
		retries := s.option.Retries
		for retries > 0 {
			retries--

			if rpcClient != nil {
				err = s.wrapCall(rpcClient.Call)(ctx, ServiceMethod, arg, reply)
				if err == nil {
					s.updateBreaker(provider.ProviderKey, true)
					return err
				} else {
					if _, ok := err.(ServiceError); ok {
						s.updateBreaker(provider.ProviderKey, false)
						return err
					}
				}
			}

			s.removeClient(provider.ProviderKey, rpcClient)
			provider, rpcClient, connectErr = s.selectClient(ctx, ServiceMethod, arg)
		}
		if err == nil {
			err = connectErr
		}

		if err == nil {
			s.updateBreaker(provider.ProviderKey, true)
		} else {
			s.updateBreaker(provider.ProviderKey, false)
		}
		return err

	default: // FailFast or FailSafe
		err = s.wrapCall(rpcClient.Call)(ctx, ServiceMethod, arg, reply)
		if err != nil {
			if _, ok := err.(ServiceError); !ok {
				s.removeClient(provider.ProviderKey, rpcClient)
			}
		}

		if s.option.FailMode == FailSafe {
			err = nil
		}

		if err == nil {
			s.updateBreaker(provider.ProviderKey, true)
		} else {
			s.updateBreaker(provider.ProviderKey, false)
		}
		return err
	}
}

func (s *sgClient) Close() error {
	s.shutdown = true
	s.mu.Lock()
	s.clients.Range(func(k, v interface{}) bool {
		if client, ok := v.(simpleClient); ok {
			s.removeClient(k.(string), &client)
		}
		return true
	})
	s.mu.Unlock()

	go func() {
		s.option.Registry.UnWatch(s.watcher)
		s.watcher.Close()
	}()

	return nil
}

func (s *sgClient) updateBreaker(providerKey string, success bool) {
	if breaker, ok := s.breakers.Load(providerKey); ok {
		if success {
			breaker.(CircuitBreaker).Success()
		} else {
			breaker.(CircuitBreaker).Fail()
		}
	}
}

// selectClient 根据负载均衡策略决定调用的服务提供者
func (s *sgClient) selectClient(ctx context.Context, ServiceMethod string, arg interface{}) (provider registry.Provider,
	client RPCClient, err error) {
	provider, err = s.option.Selector.Next(s.providers(), ctx, ServiceMethod, arg, s.option.SelectOption)
	if err != nil {
		return
	}

	client, err = s.getClient(provider)
	return
}

func (s *sgClient) providers() []registry.Provider {
	s.serversMu.RLock()
	defer s.serversMu.RUnlock()
	return s.servers
}

var ErrBreakerOpen = errors.New("breaker open")

// getClient 首先查看缓存是否有 key 对应的 provider，没有的话再加载一个新的并放入缓存
func (s *sgClient) getClient(provider registry.Provider) (client RPCClient, err error) {
	key := provider.ProviderKey
	breaker, ok := s.breakers.Load(key)
	if ok && !breaker.(CircuitBreaker).AllowRequest() {
		return nil, ErrBreakerOpen
	}
	rc, ok := s.clients.Load(key)
	if ok {
		client = rc.(RPCClient)
		if !client.IsShutDown() {
			// 如果已经失效则清除掉
			return
		} else {
			s.removeClient(key, client)
		}
	}

	// 再次检索
	rc, ok = s.clients.Load(key)
	if ok {
		// 已经有缓存，返回缓存的 RPCClient
		client = rc.(RPCClient)
	} else {
		// 没有缓存，新建一个然后更新到缓存并返回
		client, err = NewRPCClient(provider.Network, provider.Addr, s.option.Option)
		if err != nil {
			return
		}
		s.clients.Store(key, client)

		if s.option.CircuitBreakerThreshold > 0 && s.option.CircuitBreakerWindow > 0 {
			s.breakers.Store(key, NewDefaultCircuitBreaker(s.option.CircuitBreakerThreshold, s.option.CircuitBreakerWindow))
		}
	}
	return
}

func (s *sgClient) wrapGo(goFunc GoFunc) GoFunc {
	for _, wrapper := range s.option.Wrappers {
		goFunc = wrapper.WrapGo(&s.option, goFunc)
	}
	return goFunc
}

func (s *sgClient) wrapCall(callFunc CallFunc) CallFunc {
	for _, wrapper := range s.option.Wrappers {
		callFunc = wrapper.WrapCall(&s.option, callFunc)
	}
	return callFunc
}

func (s *sgClient) removeClient(clientKey string, client RPCClient) {
	s.clients.Delete(clientKey)
	if client != nil {
		client.Close()
	}
	s.breakers.Delete(clientKey)
}

func (s *sgClient) watchService(watcher registry.Watcher) {
	if watcher == nil {
		return
	}

	for {
		event, err := watcher.Next()
		if err != nil {
			log.Println("watch service error:" + err.Error())
			break
		}

		s.serversMu.Lock()
		s.servers = event.Providers
		s.serversMu.Unlock()
	}
}

// heartbeat 客户端心跳，客户端可以定时向服务端发送心跳请求
func (s *sgClient) heartbeat() {
	s.mu.Lock()
	if s.clientsHeartbeatFail == nil {
		s.clientsHeartbeatFail = make(map[string]int)
	}
	s.mu.Unlock()

	if s.option.HeartbeatInterval <= 0 {
		return
	}
	// 根据指定的时间间隔发送心跳
	t := time.NewTicker(s.option.HeartbeatInterval)
	for range t.C {
		if s.shutdown {
			t.Stop()
			return
		}
		// 遍历每个 RPCClient 进行心跳检测
		s.clients.Range(func(k, v interface{}) bool {
			err := v.(RPCClient).Call(context.Background(), "", "", nil)
			s.mu.Lock()
			if err != nil {
				// 对心跳失败进行计数
				if fail, ok := s.clientsHeartbeatFail[k.(string)]; ok {
					fail++
					s.clientsHeartbeatFail[k.(string)] = fail
				} else {
					s.clientsHeartbeatFail[k.(string)] = 1
				}
			} else {
				// 心跳成功则进行恢复
				s.clientsHeartbeatFail[k.(string)] = 0
				s.serversMu.Lock()
				for i, provider := range s.servers {
					if provider.ProviderKey == k {
						delete(s.servers[i].Meta, protocol.ProviderDegradeKey)
					}
				}
				s.serversMu.Unlock()
			}
			s.mu.Unlock()
			// 心跳失败次数超过阈值的，需要进行降级处理
			if s.clientsHeartbeatFail[k.(string)] > s.option.HeartbeatDegradeThreshold {
				s.serversMu.Lock()
				for i, provider := range s.servers {
					if provider.ProviderKey == k {
						s.servers[i].Meta[protocol.ProviderDegradeKey] = true
					}
				}
				s.serversMu.Unlock()
			}
			return true
		})
	}
}
