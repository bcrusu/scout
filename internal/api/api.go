package api

import "github.com/bcrusu/scout/internal/errors"

func (r *DiscoverRequest) Validate() error {
	if r == nil {
		return errors.Error("DiscoverRequest is nil")
	}
	return nil
}

func (r *DiscoverResponse) Validate() error {
	if r == nil {
		return errors.Error("DiscoverResponse is nil")
	}
	if r.ETag == "" || r.ServiceConfigJson == "" {
		return errors.Error("DiscoverResponse has missing fields")
	}
	return nil
}
