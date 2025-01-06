package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/bcrusu/scout/internal/control"
	"github.com/bcrusu/scout/internal/utils"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func formatInt[T utils.Signed](val T) string {
	return strconv.FormatInt(int64(val), 10)
}

func formatUint[T utils.Unsigned](val T) string {
	return strconv.FormatUint(uint64(val), 10)
}

func formatTime(val *timestamppb.Timestamp) string {
	if val == nil || val.Seconds == 0 {
		return "-"
	}
	return val.AsTime().Format(time.RFC3339)
}

func formatTrue(val bool) string {
	if val {
		return "TRUE"
	}
	return "✗"
}

func formatFlase(val bool) string {
	if !val {
		return "FALSE"
	}
	return "✓"
}

func formatServer(cluster *control.Cluster, serverID uint64) string {
	server := cluster.Servers.Items[serverID]
	if server == nil {
		return ""
	}
	return fmt.Sprintf("%s (%d)", server.Name, serverID)
}

func formatTags(tags ...string) string {
	sort.Strings(tags)
	return strings.Join(tags, ",")
}

func highlight(value string, enabled bool) string {
	if !enabled {
		return value
	}
	return fmt.Sprintf("\x1b[31m%s\x1b[0m", value)
}
