package nodes

import (
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/logging"
)

const (
	nodeIdPrefix      = "scout"
	apiSocketFileName = "firecracker.socket"
	logFileName       = "firecracker.log"
	ipFileName        = "ip"
	workFSFileName    = "workfs.ext4"
)

var (
	log = logging.New("nodes")
)

func (x *Id) Validate() error {
	if x == nil {
		return errors.Error("Id is nil")
	}

	if x.Id == "" {
		return errors.Error("Id is missing")
	}

	return nil
}

func (x *Ids) Validate() error {
	if x == nil {
		return errors.Error("Ids is nil")
	}
	return nil
}

func (x *Node) Validate() error {
	if x == nil {
		return errors.Error("Node is nil")
	}

	if x.Id == "" {
		return errors.Error("Node.Id is missing")
	}

	return nil
}

func (x *Nodes) Validate() error {
	if x == nil {
		return errors.Error("Nodes is nil")
	}

	for _, node := range x.Nodes {
		if err := node.Validate(); err != nil {
			return err
		}
	}

	return nil
}
