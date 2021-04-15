package zookeeper

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/docker/libkv"
	"github.com/docker/libkv/store"
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

type ZookeeperRegistry struct {
	AppKey         string        // 一个 ZookeeperRegistry 实例和一个 AppKey 关联
	ServicePath    string        // 数据存储的基本路径位置，比如 /service/providers
	UpdateInterval time.Duration // 定时拉取数据的时间间隔

	kv store.Store // store 实例是一个封装过的 zk 客户端

	providersMu sync.RWMutex
	providers   []registry.Provider // 本地缓存列表

	watchersMu sync.RWMutex
	watchers   []*Watcher // watcher 列表
}

func NewZookeeperRegistry(AppKey, ServicePath string, zkAddrs []string,
	updateInterval time.Duration, cfg *store.Config) registry.Registry {
	zookeeper.Register()
	zk := new(ZookeeperRegistry)
	zk.AppKey = AppKey
	zk.ServicePath = ServicePath
	zk.UpdateInterval = updateInterval

	kv, err := libkv.NewStore(store.ZK, zkAddrs, cfg)
	if err != nil {
		log.Fatalf("cannot create zk registry: %v", err)
	}
	zk.kv = kv

	basePath := zk.ServicePath
	if basePath[0] == '/' {
		// 路径不能以"/"开头
		basePath = basePath[1:]
		zk.ServicePath = basePath
	}

	// 先创建基本路径
	err = zk.kv.Put(basePath, []byte("base path"), &store.WriteOptions{IsDir: true})
	if err != nil {
		log.Fatalf("cannot create zk path %s: %v", zk.ServicePath, err)
	}

	// 定时拉取数据到列表
	go func() {
		t := time.NewTicker(updateInterval)

		for range t.C {
			// 定时拉取数据
			zk.doGetServiceList()
		}
	}()

	go func() {
		// TODO:此处需要阻塞一段时间等创建完成后才可以watch到
		time.Sleep(updateInterval)
		// watch 数据
		zk.watch()
	}()

	return zk

}

func (zk *ZookeeperRegistry) doGetServiceList() {
	path := constructServiceBasePath(zk.ServicePath, zk.AppKey)
	log.Printf("zk.ServicePath: %s,zk.AppKey:%s,path:%s", zk.ServicePath, zk.AppKey, path)
	kvPairs, err := zk.kv.List(path)
	if err != nil {
		log.Printf("error get service list %v", err)
		return
	}

	var list []registry.Provider
	for _, pair := range kvPairs {
		log.Printf("got provider %v", kv2Provider(pair))
		provider := kv2Provider(pair)
		list = append(list, provider)
	}
	log.Printf("get service list %v", list)

	zk.providersMu.Lock()
	zk.providers = list
	zk.providersMu.Unlock()
}

func constructServiceBasePath(basePath, appKey string) string {
	serviceBasePathBuffer := bytes.NewBufferString(basePath)
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
	json.Unmarshal(kv.Value, &meta)
	provider.Meta = meta

	return provider
}

func (zk *ZookeeperRegistry) watch() {
	// 每次 watch 到数据后都需要重新 watch，所以这里是一个死循环
	for {
		appKeyPath := constructServiceBasePath(zk.ServicePath, zk.AppKey)
		// 监听时先检测路径是否存在
		if exists, _ := zk.kv.Exists(appKeyPath); exists {
			lastUpdate := strconv.Itoa(int(time.Now().UnixNano()))
			// 记录一下最后的更新时间
			err := zk.kv.Put(appKeyPath, []byte(lastUpdate), &store.WriteOptions{IsDir: true})
			if err != nil {
				log.Printf("create path before watch error,key %v", appKeyPath)
			}
		}

		// 只需要监听父级目录
		ch, err := zk.kv.Watch(appKeyPath, nil)
		if err != nil {
			log.Fatalf("error watch %v", err)
		}
		watchFinish := false
		for !watchFinish {
			//循环读取 watch 到的数据
			select {
			case pairs := <-ch:
				if pairs == nil {
					log.Printf("read finish")
					// watch 数据读取结束，跳出本次循环
					watchFinish = true
				}
				// 父级目录有更新，重新拉取服务提供者列表
				zk.doGetServiceList()

				zk.providersMu.RLock()
				list := zk.providers
				zk.providersMu.RUnlock()
				// 通知 watcher
				for _, w := range zk.watchers {
					w.event <- &registry.Event{AppKey: zk.AppKey, Providers: list}
				}
			}
		}
	}
}

func (zk *ZookeeperRegistry) Register(option registry.RegisterOption, providers ...registry.Provider) {
	serviceBasePath := constructServiceBasePath(zk.ServicePath, option.AppKey)
	for _, provider := range providers {
		if provider.Addr[0] == ':' {
			provider.Addr = share.LocalIpv4() + provider.Addr
		}
		key := serviceBasePath + provider.Network + "@" + provider.Addr
		data, _ := json.Marshal(provider.Meta)
		err := zk.kv.Put(key, data, nil)
		if err != nil {
			log.Printf("zookeeper register err: %v, provider: %v", err, provider)
		}

		// 注册时更新父级目录触发 watch
		lastUpdate := strconv.Itoa(int(time.Now().UnixNano()))
		err = zk.kv.Put(serviceBasePath, []byte(lastUpdate), nil)
		if err != nil {
			log.Printf("zookeeper register modify lastupdate error: %v, provider: %v", err, provider)
		}
	}
}

func (zk *ZookeeperRegistry) Unregister(option registry.RegisterOption, providers ...registry.Provider) {
	serviceBasePath := constructServiceBasePath(zk.ServicePath, option.AppKey)
	for _, provider := range providers {
		if provider.Addr[0] == ':' {
			provider.Addr = share.LocalIpv4() + provider.Addr
		}
		key := serviceBasePath + provider.Network + "@" + provider.Addr
		err := zk.kv.Delete(key)
		if err != nil {
			log.Printf("zookeeper unregister error: %v, provider: %v", err, provider)
		}

		// 注销的时候更新父级目录触发 watch
		lastUpdate := strconv.Itoa(int(time.Now().UnixNano()))
		err = zk.kv.Put(serviceBasePath, []byte(lastUpdate), nil)
		if err != nil {
			log.Printf("zookeeper register modify lastupdate error: %v, provider %v", err, provider)
		}
	}
}

func (zk *ZookeeperRegistry) GetServiceList() []registry.Provider {
	zk.providersMu.RLock()
	defer zk.providersMu.RUnlock()
	return zk.providers
}

func (zk *ZookeeperRegistry) Watch() registry.Watcher {
	w := &Watcher{
		event: make(chan *registry.Event, 10),
		exit:  make(chan struct{}, 10),
	}
	zk.watchersMu.Lock()
	zk.watchers = append(zk.watchers, w)
	zk.watchersMu.Unlock()
	return w
}

func (zk *ZookeeperRegistry) UnWatch(watcher registry.Watcher) {
	var list []*Watcher
	zk.watchersMu.Lock()
	defer zk.watchersMu.Unlock()
	for _, w := range zk.watchers {
		if w != watcher {
			list = append(list, w)
		}
	}
	zk.watchers = list
}
