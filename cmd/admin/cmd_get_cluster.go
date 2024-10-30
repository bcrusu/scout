package main

import (
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/emptypb"
)

func newGetClusterCmd() *cobra.Command {
	c := &cobra.Command{
		Use:     "cluster",
		Aliases: []string{"c"},
		Short:   "Get servers.",
		RunE: func(c *cobra.Command, args []string) error {
			info, err := controlClient.GetClusterInfo(c.Context(), &emptypb.Empty{})
			if err != nil {
				return err
			}

			table := newTable([]string{"Group", "Property", "Value"},
				[][]string{
					{"Cluster", "Name", info.Cluster.Name},
					{"", "Index", formatUint(info.Cluster.Index)},
					{"", "Version", formatUint(info.Cluster.Version)},
					{"", "CreatedTime", formatTime(info.Cluster.CreatedTime)},
					{"", "PartitionCount", formatUint(info.Cluster.PartitionCount)},
					{"", "Leader", formatServer(info.Cluster, info.ControlLeaderId)},
					{"Servers", "Count", formatInt(len(info.Cluster.Servers.Items))},
					{"", "Version", formatUint(info.Cluster.Servers.Version)},
					{"", "RegisterVersion", formatUint(info.Cluster.Servers.RegisterVersion)},
					{"Partitions", "Count", formatInt(len(info.Cluster.Partitions.Items))},
					{"", "Version", formatUint(info.Cluster.Partitions.Version)},
					{"", "AssignmentsVersion", formatUint(info.Cluster.Partitions.AssignmentsVersion)},
				})

			table.SetAutoMergeCellsByColumnIndex([]int{0})
			table.SetColumnAlignment([]int{tablewriter.ALIGN_DEFAULT, tablewriter.ALIGN_DEFAULT, tablewriter.ALIGN_RIGHT})
			table.Render()
			return nil
		},
	}

	return c
}
