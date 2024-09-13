package control

import "github.com/bcrusu/graph/internal/errors"

func (x *Hello) Validate() error {
	if x == nil {
		return errors.Error("Hello is nil")
	}

	if x.ServerId == 0 || x.Address == "" || x.ClusterName == "" {
		return errors.Error("Hello has missing fields")
	}

	return nil
}

func (x *HelloDataServer) Validate() error {
	if x == nil {
		return errors.Error("HelloDataServer is nil")
	}

	err1 := x.Config.Validate()
	err2 := x.DataServers.Validate()

	if err := errors.Join(err1, err2); err != nil {
		return errors.Wrap(err, "HelloDataServer has invalid fields")
	}

	return nil
}

func (x *HelloApiServer) Validate() error {
	if x == nil {
		return errors.Error("HelloApiServer is nil")
	}

	err1 := x.Config.Validate()
	err2 := x.DataServers.Validate()
	err3 := x.ApiServers.Validate()

	if err := errors.Join(err1, err2, err3); err != nil {
		return errors.Wrap(err, "HelloApiServer has invalid fields")
	}

	return nil
}

func (x *DataServerConfig) Validate() error {
	if x == nil {
		return errors.Error("DataServerConfig is nil")
	}

	if x.Version == 0 {
		return errors.Error("DataServerConfig has missing fields")
	}

	return nil
}

func (x *ApiServerConfig) Validate() error {
	if x == nil {
		return errors.Error("ApiServerConfig is nil")
	}

	if x.Version == 0 {
		return errors.Error("ApiServerConfig has missing fields")
	}

	return nil
}

func (x *DataServers) Validate() error {
	if x == nil {
		return errors.Error("DataServers is nil")
	}

	if x.Version == 0 || len(x.Servers) == 0 || len(x.Partitions) == 0 || x.PartitionCount == 0 || x.ServiceConfigJson == "" {
		return errors.Error("DataServers has missing fields")
	}

	for id, server := range x.Servers {
		if err := server.Validate(); err != nil {
			return errors.Wrap(err, "DataServers.Servers is invalid")
		}

		if id != server.Id {
			return errors.Error("DataServers.Servers.Id does not match")
		}
	}

	if len(x.Partitions) != int(x.PartitionCount) {
		return errors.Error("DataServers.Partitions count does not match PartitionCount")
	}

	for id, part := range x.Partitions {
		if err := part.Validate(); err != nil {
			return errors.Wrap(err, "DataServers.Partitions is invalid")
		}

		if id != part.Id {
			return errors.Error("DataServers.Partitions.Id does not match")
		}

		if _, ok := x.Servers[part.WriteServerId]; !ok {
			return errors.Error("DataServers.Partitions.WriteServer not found in server list")
		}

		for _, serverID := range part.ReadServerIds {
			if _, ok := x.Servers[serverID]; !ok {
				return errors.Error("DataServers.Partitions.ReadServer not found in server list")
			}
		}
	}

	return nil
}

func (x *ApiServers) Validate() error {
	if x == nil {
		return errors.Error("ApiServers is nil")
	}

	if x.Version == 0 || len(x.Servers) == 0 || x.ServiceConfigJson == "" {
		return errors.Error("ApiServers has missing fields")
	}

	for id, server := range x.Servers {
		if err := server.Validate(); err != nil {
			return errors.Wrap(err, "ApiServers.Servers is invalid")
		}

		if id != server.Id {
			return errors.Error("ApiServers.Servers.Id does not match")
		}
	}

	return nil
}

func (x *DataServers_Server) Validate() error {
	if x == nil {
		return errors.Error("Server is nil")
	}

	if x.Id == 0 || x.Address == "" {
		return errors.Error("Server has missing fields")
	}

	return nil
}

func (x *ApiServers_Server) Validate() error {
	if x == nil {
		return errors.Error("Server is nil")
	}

	if x.Id == 0 || x.Address == "" {
		return errors.Error("Server has missing fields")
	}

	return nil
}

func (x *DataServers_Partition) Validate() error {
	if x == nil {
		return errors.Error("Partition is nil")
	}

	if x.Id == 0 || len(x.ReadServerIds) == 0 || x.WriteServerId == 0 {
		return errors.Error("Partition has missing fields")
	}

	return nil
}

func (x *DataServerStatus) Validate() error {
	if x == nil {
		return errors.Error("DataServerStatus is nil")
	}

	if len(x.Partitions) == 0 {
		return errors.Error("DataServerStatus has missing fields")
	}

	for id, part := range x.Partitions {
		if err := part.Validate(); err != nil {
			return errors.Wrap(err, "DataServerStatus.Partitions is invalid")
		}

		if id != part.Id {
			return errors.Error("DataServerStatus.Partitions.Id does not match")
		}
	}

	return nil
}

func (x *DataServerStatus_Partition) Validate() error {
	if x == nil {
		return errors.Error("Partition is nil")
	}

	if x.Id == 0 || x.LeaderTerm == 0 {
		return errors.Error("Partition has missing fields")
	}

	return nil
}

func (x *ApiServerStatus) Validate() error {
	if x == nil {
		return errors.Error("ApiServerStatus is nil")
	}

	return nil
}

func (x *GetConfig) Validate() error {
	if x == nil {
		return errors.Error("GetConfig is nil")
	}

	return nil
}

func (x *GetDataServers) Validate() error {
	if x == nil {
		return errors.Error("GetDataServers is nil")
	}

	return nil
}

func (x *GetApiServers) Validate() error {
	if x == nil {
		return errors.Error("GetApiServers is nil")
	}

	return nil
}
