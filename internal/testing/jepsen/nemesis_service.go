package jepsen

import (
	"context"
	"time"

	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/testing/agent"
	"github.com/bcrusu/scout/internal/utils"
)

type serviceNemesis struct {
	cluster  *cluster
	interval time.Duration
	history  *nemesisWriter
	log      logging.Logger
}

func (n *serviceNemesis) Run(ctx context.Context) {
	requests := n.makeRequests()
	selectors := n.makeSelectors()

	for {
		selector := utils.RandElem(selectors)
		var duration time.Duration

		for _, node := range selector(n.cluster.All) {
			req := utils.RandElem(requests)
			duration = max(duration, req.Duration.AsDuration())

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

func (n *serviceNemesis) makeRequests() []*agent.NemesisRequest {
	return []*agent.NemesisRequest{
		agent.NewNemesisRequest(&agent.Kill{}, time.Second),
		agent.NewNemesisRequest(&agent.Pause{}, time.Second/4),   // short GC pause
		agent.NewNemesisRequest(&agent.Pause{}, time.Second/2),   // large GC pause
		agent.NewNemesisRequest(&agent.Pause{}, time.Second),     // very large GC pause
		agent.NewNemesisRequest(&agent.Restart{}, time.Second/2), // short
		agent.NewNemesisRequest(&agent.Restart{}, time.Second),   // large
		agent.NewNemesisRequest(&agent.Restart{}, 2*time.Second), // very large
	}
}

func (n *serviceNemesis) makeSelectors() []nodeSelector {
	control := selectType(agent.ServiceType_Control)
	data := selectType(agent.ServiceType_Data)
	api := selectType(agent.ServiceType_Api)

	return []nodeSelector{
		compose(control, selectFraction(.25)), // minority of control plane nodes
		compose(control, selectFraction(.75)), // majority of control plane nodes
		control,                               // all control plane nodes
		compose(data, selectRand(1)),
		compose(data, selectRand(2)),
		compose(api, selectRand(1)),
	}
}
