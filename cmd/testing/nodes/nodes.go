package nodes

import (
	"fmt"
	"os"
	"os/exec"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/utils"
)

var (
	log = logging.New("nodes")
)

func ListNodes(nodesDir string) ([]Node, error) {
	entries, err := os.ReadDir(nodesDir)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read dir contents")
	}

	var result []Node

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		nodePath := path.Join(nodesDir, entry.Name())
		result = append(result, NewNode(nodePath))
	}

	slices.SortFunc(result, func(a, b Node) int { return strings.Compare(a.ID, b.ID) })
	return result, nil
}

func AddNodes(config Config, count int) error {
	idx := time.Now().Unix()

	getNext := func() string {
		for {
			id := fmt.Sprintf("%s%d", nodeIdPrefix, idx)
			nodePath := path.Join(config.NodesDir, id)
			idx++

			if exists, err := utils.PathExists(nodePath); err != nil {
				log.WithError(err).Error("Could not determine path status.", "path", nodePath)
			} else if !exists {
				return nodePath
			}
		}
	}

	for range count {
		nodePath := getNext()
		if err := utils.MkdirAll(nodePath); err != nil {
			return err
		}

		dest := path.Join(nodePath, "workfs.ext4")
		cmd := exec.Command("cp", "--sparse=always", config.WorkFS, dest)
		if err := cmd.Run(); err != nil {
			return errors.Wrapf(err, "failed to copy work filesystem to %s", dest)
		}
	}

	return nil
}

func RemoveNodes(config Config, ids ...string) error {
	var errs []error
	for _, id := range ids {
		if err := RemoveNode(config, id); err != nil {
			errs = append(errs, errors.Wrapf(err, "failed to remove node %s", id))
		}
	}

	return errors.Join(errs...)
}

func RemoveNode(config Config, id string) error {
	nodePath := path.Join(config.NodesDir, id)
	node := NewNode(nodePath)

	if err := node.Stop(); err != nil {
		return errors.Wrap(err, "failed to stop")
	}

	return os.RemoveAll(nodePath)
}
