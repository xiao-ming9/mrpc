package libkv

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/docker/libkv"
	"github.com/docker/libkv/store"
	"github.com/docker/libkv/store/boltdb"
	"github.com/docker/libkv/store/consul"
	"github.com/docker/libkv/store/etcd"
	"github.com/docker/libkv/store/zookeeper"
	"log"
	"mrpc/registry"
	"mrpc/share"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Watcher struct {
	event chan *registry.Event
	exit  chan struct{}
}

func (w *Watcher) Next() (*registry.Event, error) {
	for {
		select {
		case r := <-w.event:
			return r, nil
		case <-w.exit:
			return nil, errors.New("watcher stopped")
		}
	}
}

func (w *Watcher) Close() {
	select {
	case <-w.exit:
		return
	default:
		close(w.exit)
	}
}

// 支持多种数据源的 Registry
type KVRegistry struct {
	AppKey         string        // KVRegistry
	ServicePath    string        // 数据存储的基本路径位置，比如 /service/providers
	UpdateInterval time.Duration // 定时拉去数据的时间间隔

	kv store.Store // store 实例是一个封装过的客户端

	providersMu sync.RWMutex
	providers   []registry.Provider // 本地缓存列表

	watchersMu sync.Mutex
	watchers   []*Watcher // watcher 列表
}

// NewKVRegistry 初始化逻辑：根据 backend 参数的不同支持不同的底层数据源
func NewKVRegistry(backend store.Backend, addrs []string, AppKey, ServicePath string,
	updateInterval time.Duration, cfg *store.Config) registry.Registry {
	switch backend {
	case store.ZK:
		zookeeper.Register()
	case store.ETCD:
		etcd.Register()
	case store.CONSUL:
		consul.Register()
	case store.BOLTDB:
		boltdb.Register()
	}

	r := new(KVRegistry)
	r.AppKey = AppKey

	// 路径不能以 "/" 开头
	if ServicePath[0] == '/' {
		r.ServicePath = ServicePath[1:]
	} else {
		r.ServicePath = ServicePath
	}
	r.UpdateInterval = updateInterval

	// 生成实际的数据源
	kv, err := libkv.NewStore(backend, addrs, cfg)
	if err != nil {
		log.Fatalf("cannot create kv registry: %v", err)
	}
	r.kv = kv

	basePath := constructServiceBasePath(r.ServicePath, r.AppKey)
	// 先创建基本路径
	err = r.kv.Put(basePath, []byte("base path"), &store.WriteOptions{IsDir: true})
	if err != nil {
		log.Fatalf("cannot create registry paht %s: %v", r.ServicePath, err)
	}

	// TODO:显式拉取一次，存在路径不存在的 bug(已修复)
	err = r.doGetServiceList()
	if err != nil {
		log.Fatalf("first get service list error %s", err)
	} else {
		log.Printf("first get service list!")
	}

	// 定时拉去数据
	go func() {
		t := time.NewTicker(updateInterval)

		for range t.C {
			_ = r.doGetServiceList()
		}
	}()

	// watch数据
	go func() {
		time.Sleep(updateInterval)
		r.watch()
	}()
	return r
}

func (r *KVRegistry) Register(option registry.RegisterOption, providers ...registry.Provider) {
	serviceBasePath := constructServiceBasePath(r.ServicePath, r.AppKey)

	for _, provider := range providers {
		if provider.Addr[0] == ':' {
			provider.Addr = share.LocalIpv4() + provider.Addr
		}
		key := serviceBasePath + provider.Network + "@" + provider.Addr
		data, _ := json.Marshal(provider.Meta)
		err := r.kv.Put(key, data, nil)
		if err != nil {
			log.Printf("libkv register error: %v,provider %v", err, provider)
		}

		// 注册时更新父级目录触发 watch
		lastUpdate := strconv.Itoa(int(time.Now().UnixNano()))
		err = r.kv.Put(serviceBasePath, []byte(lastUpdate), nil)
		if err != nil {
			log.Printf("libkv register modify lastupdate error: %v, provider: %v", err, provider)
		}
	}
}

func (r *KVRegistry) Unregister(option registry.RegisterOption, providers ...registry.Provider) {
	serviceBasePath := constructServiceBasePath(r.ServicePath, option.AppKey)

	for _, provider := range providers {
		if provider.Addr[0] == ':' {
			provider.Addr = share.LocalIpv4() + provider.Addr
		}
		key := serviceBasePath + provider.Network + "@" + provider.Addr
		err := r.kv.Delete(key)
		if err != nil {
			log.Printf("libkv unregister error: %v, provider %v", err, provider)
		}

		// 注销时更父级目录触发 watch
		lastUpdate := strconv.Itoa(int(time.Now().UnixNano()))
		err = r.kv.Put(serviceBasePath, []byte(lastUpdate), nil)
		if err != nil {
			log.Printf("libkv register modify lastupdate error: %v, provider: %v", err, provider)
		}
	}
}

func (r *KVRegistry) GetServiceList() []registry.Provider {
	r.providersMu.RLock()
	defer r.providersMu.RUnlock()
	return r.providers
}

func (r *KVRegistry) Watch() registry.Watcher {
	w := &Watcher{
		event: make(chan *registry.Event, 10),
		exit:  make(chan struct{}, 10),
	}
	r.watchersMu.Lock()
	r.watchers = append(r.watchers, w)
	r.watchersMu.Unlock()
	return w
}

func (r *KVRegistry) UnWatch(watcher registry.Watcher) {
	var list []*Watcher
	r.watchersMu.Lock()
	defer r.watchersMu.Unlock()
	for _, w := range r.watchers {
		if w != watcher {
			list = append(list, w)
		}
	}
	r.watchers = list
}

func (r *KVRegistry) doGetServiceList() error {
	path := constructServiceBasePath(r.ServicePath, r.AppKey)
	kvPairs, err := r.kv.List(path)

	var list []registry.Provider
	if err != nil {
		log.Printf("error get service list %v", err)
		return err
	}

	for _, pair := range kvPairs {
		provider := kv2Provider(pair)
		list = append(list, provider)
	}

	r.providersMu.Lock()
	r.providers = list
	r.providersMu.Unlock()
	return nil
}

func constructServiceBasePath(servicePath, appKey string) string {
	serviceBasePathBuffer := bytes.NewBufferString(servicePath)
	serviceBasePathBuffer.WriteString("/")
	serviceBasePathBuffer.WriteString(appKey)
	serviceBasePathBuffer.WriteString("/")
	return serviceBasePathBuffer.String()
}

func kv2Provider(kv *store.KVPair) registry.Provider {
	provider := registry.Provider{}
	provider.ProviderKey = kv.Key

	networkAndAddr := strings.SplitN(kv.Key, "@", 2)
	provider.Network = networkAndAddr[0]
	provider.Addr = networkAndAddr[1]

	meta := make(map[string]interface{}, 0)
	_ = json.Unmarshal(kv.Value, &meta)
	provider.Meta = meta

	return provider
}

func (r *KVRegistry) watch() {
	// 每次 watch 到数据后都需要重新 watch，所以是一个死循环
	for {
		// 监听 appKey 对应的目录，一旦父级目录的数据有变更就重新读取服务列表
		appKeyPath := constructServiceBasePath(r.ServicePath, r.AppKey)

		// 监听时需要先检测路径是否存在
		if exist, _ := r.kv.Exists(appKeyPath); exist {
			// 记录最后一次更新的时间
			lastUpdate := strconv.Itoa(int(time.Now().UnixNano()))
			err := r.kv.Put(appKeyPath, []byte(lastUpdate), &store.WriteOptions{IsDir: true})
			if err != nil {
				log.Printf("create path before watch error,key %v", appKeyPath)
			}
		}
		// 只需要监听父级目录
		ch, err := r.kv.Watch(appKeyPath, nil)
		if err != nil {
			log.Fatalf("error watch %v", err)
		}

		watchFinish := false
		for !watchFinish {
			// 循环读取 watch 到的数据
			select {
			case pairs := <-ch:
				if pairs == nil {
					log.Printf("read finish")
					// watch 数据结束，跳出这次循环
					watchFinish = true
				}

				// 重新读取数据列表

				// 个人认为的实现逻辑
				err = r.doGetServiceList()
				if err != nil {
					watchFinish = true
				}

				// TODO 源代码的实现逻辑
				//latestPairs, err := r.kv.List(appKeyPath)
				//if err != nil {
				//	watchFinish = true
				//}
				//
				//r.providersMu.RLock()
				//list := r.providers
				//r.providersMu.RUnlock()
				//for _, p := range latestPairs {
				//	log.Printf("got provider %v", kv2Provider(p))
				//	list = append(list, kv2Provider(p))
				//}
				//
				//r.providersMu.Lock()
				//r.providers = list
				//r.providersMu.Unlock()

				r.providersMu.RLock()
				list := r.providers
				r.providersMu.RUnlock()
				// 通知 watcher
				for _, w := range r.watchers {
					w.event <- &registry.Event{AppKey: r.AppKey, Providers: list}
				}
			}
		}
	}
}
