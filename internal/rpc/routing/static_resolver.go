package routing

import (
	"google.golang.org/grpc/resolver"
)

const schemeStatic = "static"

func init() {
	resolver.Register(&staticResolverBuilder{})
}

type staticResolverBuilder struct{}

func (b *staticResolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	addrs, err := ParseTargetStatic(target.URL)
	if err != nil {
		return nil, err
	}

	r := &staticResolver{
		addrs: addrs,
		cc:    cc,
	}

	r.resolveNow()
	return r, nil
}

func (b *staticResolverBuilder) Scheme() string {
	return schemeStatic
}

type staticResolver struct {
	addrs []string
	cc    resolver.ClientConn
}

func (r *staticResolver) ResolveNow(o resolver.ResolveNowOptions) {
	r.resolveNow()
}

func (r *staticResolver) resolveNow() {
	addrs := make([]resolver.Address, len(r.addrs))
	for i, a := range r.addrs {
		addrs[i] = resolver.Address{Addr: a}
	}

	r.cc.UpdateState(resolver.State{
		Addresses: addrs,
	})
}

func (*staticResolver) Close() {}
