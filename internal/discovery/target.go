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
	_ validation.CanValidate = (*Discovery)(nil)
)

type Discovery struct {
	Servers []string `yaml:"servers"`
	DNS     string   `yaml:"dns"`
}

// String returns the corresponding gRPC target string.
func (d Discovery) String() string {
	q := url.Values{}
	q.Add("discovery", d.getTarget())

	u := url.URL{
		Scheme:   Scheme,
		Opaque:   "graph",
		RawQuery: q.Encode(),
	}

	return u.String()
}

func (d Discovery) getTarget() string {
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

// GetDiscoveryTarget parses the URL to extract discovery target.
func GetDiscoveryTarget(u url.URL) (string, error) {
	if u.Scheme != Scheme {
		return "", errors.Errorf("unknown discovery scheme %s", u.Scheme)
	}

	values, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return "", errors.Wrap(err, "failed to parse query")
	}

	discovery, err := getQueryValue(values, "discovery")
	if err != nil {
		return "", err
	}

	return discovery, nil
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
