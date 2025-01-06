package jepsen

import (
	"context"
	"time"

	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/testing/agent"
	"github.com/bcrusu/scout/internal/utils"
)

type timeNemesis struct {
	cluster  *cluster
	interval time.Duration
	history  *nemesisWriter
	log      logging.Logger
}

func (n *timeNemesis) Run(ctx context.Context) {
	duration := 2 * time.Second
	requests := n.makeRequests(duration)
	selectors := n.makeSelectors()

	for {
		selector := utils.RandElem(selectors)

		for _, node := range selector(n.cluster.All) {
			req := utils.RandElem(requests)

			if _, err := node.Agent.RunNemesis(context.Background(), req); err != nil {
				n.log.WithError(err).Error("RunNemesis call failed.", "name", req.Name, "node", node.Id)
			} else if err := n.history.Write(node.Id, req); err != nil {
				n.log.WithError(err).Error("History write failed.", "name", req.Name, "node", node.Id)
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(duration + utils.AddJitter(n.interval)):
		}
	}
}

func (n *timeNemesis) makeRequests(duration time.Duration) []*agent.NemesisRequest {
	return []*agent.NemesisRequest{
		agent.NewNemesisRequest(&agent.BumpTime{Delta: -250}, duration), // small
		agent.NewNemesisRequest(&agent.BumpTime{Delta: -450}, duration), // very large
		agent.NewNemesisRequest(&agent.BumpTime{Delta: -500}, duration), // max time offset
		agent.NewNemesisRequest(&agent.BumpTime{Delta: -800}, duration), // above max
		agent.NewNemesisRequest(&agent.BumpTime{Delta: 250}, duration),  // small
		agent.NewNemesisRequest(&agent.BumpTime{Delta: 450}, duration),  // very large
		agent.NewNemesisRequest(&agent.BumpTime{Delta: 500}, duration),  // max time offset
		agent.NewNemesisRequest(&agent.BumpTime{Delta: 800}, duration),  // above max
		agent.NewNemesisRequest(&agent.StrobeTime{Delta: -100, Period: 50}, duration),
		agent.NewNemesisRequest(&agent.StrobeTime{Delta: -300, Period: 50}, duration),
		agent.NewNemesisRequest(&agent.StrobeTime{Delta: 100, Period: 50}, duration),
		agent.NewNemesisRequest(&agent.StrobeTime{Delta: 300, Period: 50}, duration),
	}
}

func (n *timeNemesis) makeSelectors() []nodeSelector {
	return []nodeSelector{
		selectRand(1),
		selectRand(2),
	}
}
