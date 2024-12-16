package discovery

import (
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/validation"
)

var (
	_ validation.CanValidate = (*Discovery)(nil)
)

type Discovery struct {
	Servers []string `yaml:"servers,omitempty"`
	DNS     string   `yaml:"dns,omitempty"`
}

func DNS(target string) Discovery {
	return Discovery{
		DNS: target,
	}
}

func Servers(addrs ...string) Discovery {
	return Discovery{
		Servers: addrs,
	}
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
