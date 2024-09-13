package discovery

import (
	"net/url"
	"strings"

	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/rpc/routing"
)

const (
	Scheme = "graph"
)

type DiscoveryTarget = string

type Target struct {
	ClusterName string
	Discovery   DiscoveryTarget
}

func NewTarget(clusterName string, discovery DiscoveryTarget) Target {
	return Target{
		ClusterName: clusterName,
		Discovery:   discovery,
	}
}

// DNS uses dns to discover the control plane cluster.
// The expected target format is 'dns:[//authority/]host[:port]'.
func DNS(target string) DiscoveryTarget {
	if !strings.HasPrefix(target, "dns:") {
		target = "dns:" + target
	}
	return target
}

// Static uses a static list of addresses to discover the control plane cluster.
func Static(addresses ...string) DiscoveryTarget {
	return routing.FormatTargetStatic(addresses)
}

// String returns the corresponding gRPC target string.
func (t Target) String() string {
	q := url.Values{}
	q.Add("cluster ", t.ClusterName)
	q.Add("discovery", t.Discovery)

	u := url.URL{
		Scheme:   Scheme,
		Opaque:   "graph",
		RawQuery: q.Encode(),
	}

	return u.String()
}

func (t Target) IsValid() bool {
	return t.ClusterName != "" && t.Discovery != ""
}

// ParseTarget parses the URL to extract discovery target.
func ParseTarget(u url.URL) (Target, error) {
	if u.Scheme != Scheme {
		return Target{}, errors.Errorf("unknown target scheme %s", u.Scheme)
	}

	values, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return Target{}, errors.Wrap(err, "failed to parse query")
	}

	clusterName, err1 := getQueryValue(values, "cluster")
	discovery, err2 := getQueryValue(values, "discovery")
	if err := errors.Join(err1, err2); err != nil {
		return Target{}, err
	}

	return Target{
		ClusterName: clusterName,
		Discovery:   discovery,
	}, nil
}

func getQueryValue(values url.Values, name string) (string, error) {
	v, ok := values[name]
	if !ok || len(v) == 0 {
		return "", errors.Errorf("URL is missing %s query param", name)
	} else if len(v) != 1 {
		return "", errors.Errorf("URL has multiple %s query param values", name)
	}

	return v[0], nil
}
