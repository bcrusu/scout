package storage

import (
	"github.com/bcrusu/scout/internal/control"
	"github.com/bcrusu/scout/internal/metrics"
)

type fsmMeters struct {
	ApplySuccess        metrics.Counter
	ApplyError          metrics.Counter
	ServerTotal         metrics.Gauge
	ServerControl       metrics.Gauge
	ServerData          metrics.Gauge
	ServerApi           metrics.Gauge
	ReplicaCount        metrics.Gauge
	ReplicaJoining      metrics.Gauge
	ReplicaServing      metrics.Gauge
	ReplicaLeaving      metrics.Gauge
	ReplicaMaxImbalance metrics.Gauge
}

func newFsmMeters() fsmMeters {
	return fsmMeters{
		ApplySuccess:        metrics.NewCounter("fsm.apply.success"),
		ApplyError:          metrics.NewCounter("fsm.apply.error"),
		ServerTotal:         metrics.NewGauge("server.total.count"),
		ServerControl:       metrics.NewGauge("server.control.count"),
		ServerData:          metrics.NewGauge("server.data.count"),
		ServerApi:           metrics.NewGauge("server.api.count"),
		ReplicaCount:        metrics.NewGauge("replica.total.count"),
		ReplicaJoining:      metrics.NewGauge("replica.joining.count"),
		ReplicaServing:      metrics.NewGauge("replica.serving.count"),
		ReplicaLeaving:      metrics.NewGauge("replica.leaving.count"),
		ReplicaMaxImbalance: metrics.NewGauge("replica.max_imbalance"),
	}
}

func (m fsmMeters) Update(fsm *FSM) {
	m.ServerTotal.Update(len(fsm.servers.Items))
	m.ServerControl.Update(fsm.servers.CountForType(control.ServerType_Control))
	m.ServerData.Update(fsm.servers.CountForType(control.ServerType_Data))
	m.ServerApi.Update(fsm.servers.CountForType(control.ServerType_Api))
	m.ReplicaCount.Update(fsm.partitions.ReplicaCount())
	m.ReplicaJoining.Update(fsm.partitions.ReplicaCountForState(control.ReplicaState_Joining))
	m.ReplicaServing.Update(fsm.partitions.ServingReplicaCount())
	m.ReplicaLeaving.Update(fsm.partitions.ReplicaCountForState(control.ReplicaState_Leaving))
	m.ReplicaMaxImbalance.Update(int(fsm.partitions.MaxImbalance))
}
