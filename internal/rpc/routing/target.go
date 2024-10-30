package routing

import (
	"net/url"

	"github.com/bcrusu/scout/internal/errors"
)

func FormatTargetStatic(addrs ...string) string {
	q := url.Values{}
	for _, a := range addrs {
		q.Add("a", a)
	}

	u := url.URL{
		Scheme:   schemeStatic,
		Opaque:   "scout",
		RawQuery: q.Encode(),
	}

	return u.String()
}

func ParseTargetStatic(target url.URL) ([]string, error) {
	if target.RawQuery == "" {
		return nil, errors.Error("target is missing query params")
	}

	values, err := url.ParseQuery(target.RawQuery)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse target query params")
	}

	addrs, ok := values["a"]
	if !ok || len(values) == 0 {
		return nil, errors.Error("target is missing addr query param")
	}

	return addrs, nil
}
