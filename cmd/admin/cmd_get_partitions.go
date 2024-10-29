package main

import (
	"fmt"

	"github.com/bcrusu/scout/internal/control"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/emptypb"
)

func newGetPartitionsCmd() *cobra.Command {
	c := &cobra.Command{
		Use:     "partitions",
		Aliases: []string{"p", "part", "partition"},
		Short:   "Get partitions.",
		RunE: func(c *cobra.Command, args []string) error {
			info, err := controlClient.GetClusterInfo(c.Context(), &emptypb.Empty{})
			if err != nil {
				return err
			}

			table := newTable(
				[]string{"Part", "Replicas (T/S/J/L)", "Leader", "Leader term", "Commited Index", "Version"},
				mapToTable(info.Cluster.Partitions.Items,
					func(a, b *control.Partition) int {
						return int(a.Id) - int(b.Id)
					},
					func(p *control.Partition) []string {
						return []string{
							formatUint(p.Id),
							fmt.Sprintf("%d/%d/%d/%d",
								len(p.Replicas),
								p.ServingReplicaCount(),
								p.ReplicaCountForState(control.ReplicaState_Joining),
								p.ReplicaCountForState(control.ReplicaState_Leaving)),
							p.Leader,
							formatUint(p.LeaderTerm),
							formatUint(p.CommitedIndex),
							formatUint(p.AssignmentsVersion),
						}
					}))

			table.SetCaption(true, fmt.Sprintf("Partition count: %d", info.Cluster.PartitionCount))
			table.Render()
			return nil
		},
	}

	return c
}
