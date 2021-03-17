package client

import (
	"context"
	"log"
	"mrpc/registry"
	"sync"
)

type SGClient interface {
	Go(ctx context.Context, ServiceMethod string, arg, reply interface{}, done chan *Call) (*Call, error)
	Call(ctx context.Context, ServiceMethod string, arg, reply interface{}) error
}

type sgClient struct {
	shutdown bool
	option   SGOption

	// clients 维护了客户端到服务端的长链接
	clients   sync.Map // map[string]RPCClient
	serversMu sync.RWMutex
	servers   []registry.Provider
}

func NewSGClient(option SGOption) SGClient {
	s := new(sgClient)
	s.option = option
	AddWrapper(&s.option, NewMetaDataWrapper(), NewLogWrapper())

	providers := s.option.Registry.GetServiceList()
	watcher := s.option.Registry.Watch()
	go s.watchService(watcher)

	s.serversMu.Lock()
	defer s.serversMu.Unlock()
	for _, p := range providers {
		s.servers = append(s.servers, p)
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
					return err
				} else {
					if _, ok := err.(ServiceError); ok {
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
		return err

	case FailOver:
		retries := s.option.Retries
		for retries > 0 {
			retries--

			if rpcClient != nil {
				err = s.wrapCall(rpcClient.Call)(ctx, ServiceMethod, arg, reply)
				if err == nil {
					return err
				} else {
					if _, ok := err.(ServiceError); ok {
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
		return err
	}
}

// selectClient 根据负载均衡策略决定调用的客户端
func (s *sgClient) selectClient(ctx context.Context, ServiceMethod string, arg interface{}) (

	provider registry.Provider, client RPCClient, err error) {
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

// getClient 首先查看缓存是否有 key 对应的 provider，没有的话再加载一个新的并放入缓存
func (s *sgClient) getClient(provider registry.Provider) (client RPCClient, err error) {
	key := provider.ProviderKey
	rc, ok := s.clients.Load(key)
	if ok {
		client = rc.(RPCClient)
		if !client.IsShutDown() {
			// 如果已经失效则清除掉
			return
		} else {
			s.clients.Delete(key)
			client.Close()
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

		if event.AppKey == s.option.AppKey {
			switch event.Action {
			case registry.Create:
				s.serversMu.Lock()
				for _, ep := range event.Providers {
					exists := false
					for _, p := range s.servers {
						if p.ProviderKey == ep.ProviderKey {
							exists = true
						}
					}
					if !exists {
						s.servers = append(s.servers, ep)
					}
				}

				s.serversMu.Unlock()

			case registry.Update:
				s.serversMu.Lock()
				for _, ep := range event.Providers {
					for i := range s.servers {
						if s.servers[i].ProviderKey == ep.ProviderKey {
							s.servers[i] = ep
						}
					}
				}
				s.serversMu.Unlock()
			case registry.Delete:
				s.serversMu.Lock()
				var newList []registry.Provider
				for _, p := range s.servers {
					for _, ep := range event.Providers {
						if p.ProviderKey != ep.ProviderKey {
							newList = append(newList, p)
						}
					}
				}
				s.servers = newList
				s.serversMu.Unlock()
			}
		}
	}
}
