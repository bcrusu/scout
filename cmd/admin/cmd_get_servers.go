package main

import (
	"time"

	"github.com/bcrusu/scout/internal/control"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/emptypb"
)

func newGetServersCmd() *cobra.Command {
	c := &cobra.Command{
		Use:     "servers",
		Aliases: []string{"s", "srv", "server"},
		Short:   "Get servers.",
		RunE: func(c *cobra.Command, args []string) error {
			info, err := controlClient.GetClusterInfo(c.Context(), &emptypb.Empty{})
			if err != nil {
				return err
			}

			renderTable(
				[]string{"#", "ID", "Name", "Type", "Registered", "Last seen", "Address", "Tags"},
				mapToTable(info.Cluster.Servers.Items,
					func(a, b *control.Server) int {
						if x := int(a.Type) - int(b.Type); x != 0 {
							return x
						}
						return int(a.Id) - int(b.Id)
					},
					func(rowNo int, s *control.Server) row {
						hl := time.Since(s.LastSeen.AsTime()) > 10*time.Second
						return row{
							formatInt(rowNo),
							formatUint(s.Id),
							highlight(s.Name, hl),
							s.Type.String(),
							formatTime(s.RegisteredAt),
							highlight(formatTime(s.LastSeen), hl),
							s.LastAddress,
							highlight(formatTags(s.Tags...), hl),
						}
					}))

			return nil
		},
	}

	return c
}
