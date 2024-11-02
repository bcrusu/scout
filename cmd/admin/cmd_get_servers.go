package main

import (
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
				[]string{"ID", "Name", "Type", "Registered", "Last seen", "Address", "Tags"},
				mapToTable(info.Cluster.Servers.Items,
					func(a, b *control.Server) int {
						if x := int(a.Type) - int(b.Type); x != 0 {
							return x
						}
						return int(a.Id) - int(b.Id)
					},
					func(s *control.Server) []string {
						return []string{
							formatUint(s.Id),
							s.Name,
							s.Type.String(),
							formatTime(s.RegisteredAt),
							formatTime(s.LastSeen),
							s.LastAddress,
							formatTags(s.Tags...),
						}
					},
					false,
				))

			return nil
		},
	}

	return c
}
