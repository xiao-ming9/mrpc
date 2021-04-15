// 基于内存的服务注册
package memory

import (
	"errors"
	"github.com/google/uuid"
	"mrpc/registry"
	"sync"
	"time"
)

var timeout = time.Millisecond * 10

// 实现 registry.Registry 接口
type Registry struct {
	mu        sync.RWMutex // 读写锁
	providers []registry.Provider
	watchers  map[string]*Watcher
}

func (r *Registry) Register(option registry.RegisterOption, providers ...registry.Provider) {
	r.mu.Lock()
	defer r.mu.Unlock()
	go r.sendWatcherEvent(option.AppKey, providers...)

	var providers2Register []registry.Provider
	for _, p := range providers {
		exist := false
		for _, cp := range r.providers {
			if cp.ProviderKey == p.ProviderKey {
				exist = true
				break
			}
		}
		if !exist {
			providers2Register = append(providers2Register, p)
		}
	}

	r.providers = append(r.providers, providers2Register...)

}

func (r *Registry) Unregister(option registry.RegisterOption, providers ...registry.Provider) {
	r.mu.Lock()
	defer r.mu.Unlock()
	go r.sendWatcherEvent(option.AppKey, providers...)

	var newList []registry.Provider
	for _, p := range r.providers {
		remain := true
		for _, up := range providers {
			if p.ProviderKey != up.ProviderKey {
				remain = false
			}
		}
		if remain {
			newList = append(newList, p)
		}
	}
	r.providers = newList
}

func (r *Registry) GetServiceList() []registry.Provider {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.providers
}

func (r *Registry) Watch() registry.Watcher {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.watchers == nil {
		r.watchers = make(map[string]*Watcher)
	}
	event := make(chan *registry.Event)
	exit := make(chan bool)
	id := uuid.New().String()

	w := &Watcher{
		id:   id,
		res:  event,
		exit: exit,
	}

	r.watchers[id] = w
	return w
}

func (r *Registry) UnWatch(watcher registry.Watcher) {
	// 断言，判断是否是 *Watcher类型
	target, ok := watcher.(*Watcher)
	if !ok {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	var newWatcherList []registry.Watcher
	for _, w := range r.watchers {
		if w.id != target.id {
			// TODO: 不知道这个有啥用。。。
			newWatcherList = append(newWatcherList, w)
		} else {
			// TODO:此处手动添加，感觉需要 delete 掉才能实现 Unwatch
			delete(r.watchers, w.id)
		}
	}

}

func (r *Registry) sendWatcherEvent(AppKey string, providers ...registry.Provider) {
	var watchers []*Watcher
	event := &registry.Event{
		AppKey:    AppKey,
		Providers: providers,
	}
	r.mu.RLock()
	for _, w := range r.watchers {
		watchers = append(watchers, w)
	}
	r.mu.RUnlock()

	for _, w := range watchers {
		select {
		case <-w.exit:
			r.mu.Lock()
			delete(r.watchers, w.id)
			r.mu.Unlock()
		default:
			select {
			case w.res <- event:
			case <-time.After(timeout):
			}
		}
	}
}

// 实现了 registry.Watcher 接口
type Watcher struct {
	id  string
	res chan *registry.Event
	// TODO： exit 到底是干嘛用的，还没搞明白
	exit chan bool
}

func (w *Watcher) Next() (*registry.Event, error) {
	for {
		select {
		case r := <-w.res:
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

func NewInMemoryRegistry() registry.Registry {
	r := &Registry{}
	return r
}
