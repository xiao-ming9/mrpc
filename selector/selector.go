package selector

import (
	"context"
	"errors"
	"math/rand"
	"mrpc/protocol"
	"mrpc/registry"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

var ErrEmptyProviderList = errors.New("provider list is empty")

// Filter 用于自定义规则过滤某个节点，对于需要过滤的节点，返回 false
type Filter func(provider registry.Provider, ctx context.Context, ServiceMethod string, arg interface{}) bool

type SelectOption struct {
	Filters []Filter
}

// DegradeProviderFilter 过滤降级节点
func DegradeProviderFilter() Filter {
	return func(provider registry.Provider, ctx context.Context, ServiceMethod string, arg interface{}) bool {
		_, degrade := provider.Meta[protocol.ProviderDegradeKey]
		return !degrade
	}
}

// TaggedProviderFilter 基于标签的路由策略，根据 tags 进行过滤
func TaggedProviderFilter(tags map[string]string) Filter {
	return func(provider registry.Provider, ctx context.Context, ServiceMethod string, arg interface{}) bool {
		if tags == nil {
			return false
		}
		if provider.Meta == nil {
			return false
		}
		providerTags, ok := provider.Meta["tags"].(map[string]string)
		if !ok || len(providerTags) <= 0 {
			return true
		}

		for k, v := range tags {
			if tag, ok := providerTags[k]; ok {
				if tag != v {
					return false
				}
			} else {
				return false
			}
		}
		return true
	}
}

type Selector interface {
	Next(providers []registry.Provider, ctx context.Context, ServiceMethod string,
		arg interface{}, opt SelectOption) (registry.Provider, error)
}

type RandomSelector struct {
}

var RandomSelectorInstance = RandomSelector{}

// Next 只实现了随机负载均衡
func (r RandomSelector) Next(providers []registry.Provider, ctx context.Context, ServiceMethod string,
	arg interface{}, opt SelectOption) (p registry.Provider, err error) {
	filters := combineFilter(opt.Filters)
	list := make([]registry.Provider, 0)
	for _, p := range providers {
		if filters(p, ctx, ServiceMethod, arg) {
			list = append(list, p)
		}
	}

	if len(list) == 0 {
		err = ErrEmptyProviderList
		return
	}
	// 随机一个 provider
	i := rand.Intn(len(list))
	p = list[i]
	return
}

func combineFilter(filters []Filter) Filter {
	return func(provider registry.Provider, ctx context.Context, ServiceMethod string, arg interface{}) bool {
		for _, f := range filters {
			// 此处可以自定义规则过滤某些节点
			if !f(provider, ctx, ServiceMethod, arg) {
				return false
			}
		}
		return true
	}
}

// NewRandomSelector 单例模式
func NewRandomSelector() Selector {
	return RandomSelectorInstance
}
