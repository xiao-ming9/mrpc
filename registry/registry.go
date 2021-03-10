// 基于直连的服务注册
package registry

// Registry 包含两部分功能：服务注册（用于服务端）和服务发现（用于客户端）
type Registry interface {
	Register(option RegisterOption, providers ...Provider)   // 注册
	Unregister(option RegisterOption, providers ...Provider) // 注销
	GetServiceList() []Provider                              // 获取服务列表
	Watch() Watcher                                          // 监听服务列表的变化
	UnWatch(watcher Watcher)                                 // 取消监听
}

type RegisterOption struct {
	AppKey string // AppKey 用于唯一标识某个应用,如：com.silverming.demo.rpc.server
}

// Provider 某个具体的服务提供者
type Provider struct {
	ProviderKey string // Network+"@"+Addr
	Network     string
	Addr        string
	Meta        map[string]string
}

type Watcher interface {
	Next() (*Event, error) // 获取下一次服务列表的更新
	Close()
}

type EventAction byte

const (
	Create EventAction = iota
	Update
	Delete
)

// Event 表示一次更新
type Event struct {
	Action    EventAction
	AppKey    string
	Providers []Provider // 具体变化的服务提供者（增量而不是全量）
}

type Peer2PeerDiscovery struct {
	providers []Provider
}

func (p *Peer2PeerDiscovery) Register(option RegisterOption, providers ...Provider) {
	p.providers = providers
}

func (p *Peer2PeerDiscovery) Unregister(option RegisterOption, providers ...Provider) {
	p.providers = []Provider{}
}

func (p *Peer2PeerDiscovery) GetServiceList() []Provider {
	return p.providers
}

func (p *Peer2PeerDiscovery) Watch() Watcher {
	return nil
}

func (p *Peer2PeerDiscovery) UnWatch(watcher Watcher) {
	return
}

func (p *Peer2PeerDiscovery) WithProvider(provider Provider) *Peer2PeerDiscovery {
	p.providers = append(p.providers, provider)
	return p
}

func (p *Peer2PeerDiscovery) WithProviders(providers []Provider) *Peer2PeerDiscovery {
	for _, provider := range providers {
		p.providers = append(p.providers, provider)
	}
	return p
}

func NewPeer2PeerRegistry() *Peer2PeerDiscovery {
	r := &Peer2PeerDiscovery{}
	return r
}
