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

// Filter 用于自定义规则过滤某个节点
type Filter func(provider registry.Provider, ctx context.Context, ServiceMethod string, arg interface{}) bool

type SelectOption struct {
	Filters []Filter
}

func DegradeProviderFilter(provider registry.Provider, ctx context.Context, ServiceMethod string, arg interface{}) bool {
	_, degrade := provider.Meta[protocol.ProviderDegradeKey]
	return degrade
}

type Selector interface {
	Next(providers []registry.Provider, ctx context.Context, ServiceMethod string,
		arg interface{}, opt SelectOption) (registry.Provider, error)
}

type RandomSelector struct {
}

var RandomSelectorInstance = RandomSelector{}

// 只实现了随机负载均衡
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

func NewRandomSelector() Selector {
	return RandomSelectorInstance
}
