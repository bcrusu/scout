package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/bcrusu/scout/internal/control"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/emptypb"
)

func newGetReplicasCmd() *cobra.Command {
	c := &cobra.Command{
		Use:     "replicas",
		Aliases: []string{"r", "replica"},
		Short:   "Get replicas.",
		RunE: func(c *cobra.Command, args []string) error {
			info, err := controlClient.GetClusterInfo(c.Context(), &emptypb.Empty{})
			if err != nil {
				return err
			}

			type pair struct {
				part *control.Partition
				repl *control.Partition_Replica
			}

			var pairs []pair
			for _, part := range info.Cluster.Partitions.Items {
				for _, replica := range part.Replicas {
					pairs = append(pairs, pair{part, replica})
				}
			}

			renderTable(
				[]string{"#", "Part", "Replica", "Server", "State", "Ready", "Leader", "Applied/Commited", "Created", "Transition", "Updated"},
				sliceToTable(pairs,
					func(a, b pair) int {
						if x := int(a.part.Id) - int(b.part.Id); x != 0 {
							return x
						} else if x := int(a.repl.CreatedTime.AsTime().Sub(b.repl.CreatedTime.AsTime())); x != 0 {
							return x
						} else {
							return strings.Compare(a.repl.Name, b.repl.Name)
						}
					},
					func(rowNo int, x pair) row {
						hl := time.Since(x.repl.LastUpdate.AsTime()) > 10*time.Second
						return row{
							formatInt(rowNo),
							formatUint(x.part.Id),
							highlight(x.repl.Name, hl),
							formatServer(info.Cluster, x.repl.ServerId),
							x.repl.State.String(),
							formatFlase(x.repl.Ready),
							formatTrue(x.repl.Name == x.part.Leader),
							fmt.Sprintf("%d/%d", x.repl.AppliedIndex, x.part.CommitedIndex),
							formatTime(x.repl.CreatedTime),
							formatTime(x.repl.StateTransitionTime),
							highlight(formatTime(x.repl.LastUpdate), hl),
						}
					}))

			fmt.Printf("Max imbalance: %d\n", info.Cluster.Partitions.MaxImbalance)

			return nil
		},
	}

	return c
}
