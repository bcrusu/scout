package discovery

import (
	"net/url"
	"strings"

	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/rpc/routing"
	"github.com/bcrusu/graph/internal/validation"
)

const (
	Scheme = "graph"
)

var (
	_ validation.CanValidate = (*Target)(nil)
	_ validation.CanValidate = (*Discovery)(nil)
)

type Discovery struct {
	Servers []string `yaml:"servers"`
	DNS     string   `yaml:"dns"`
}

type Target struct {
	ClusterName string
	discovery   string
}

func NewTarget(clusterName string, discovery Discovery) Target {
	return Target{
		ClusterName: clusterName,
		discovery:   discovery.String(),
	}
}

// String returns the corresponding gRPC target string.
func (t Target) String() string {
	q := url.Values{}
	q.Add("cluster", t.ClusterName)
	q.Add("discovery", t.discovery)

	u := url.URL{
		Scheme:   Scheme,
		Opaque:   "graph",
		RawQuery: q.Encode(),
	}

	return u.String()
}

func (t Target) DiscoveryTarget() string {
	return t.discovery
}

func (t Target) Validate() error {
	if t.ClusterName == "" {
		return errors.Error("cluster name is missing")
	}
	if t.discovery == "" {
		return errors.Error("discovery is missing")
	}
	return nil
}

func (d Discovery) String() string {
	if len(d.Servers) > 0 {
		return routing.FormatTargetStatic(d.Servers)
	}

	target := d.DNS
	if !strings.HasPrefix(target, "dns:") {
		target = "dns:" + target
	}
	return target
}

func (d Discovery) Validate() error {
	if len(d.Servers) > 0 && d.DNS != "" {
		return errors.Error("multiple discovery methods are not supported")
	}
	if len(d.Servers) == 0 && d.DNS == "" {
		return errors.Error("is empty")
	}
	return nil
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
		discovery:   discovery,
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
