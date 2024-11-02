package partitions

import (
	"fmt"

	"github.com/bcrusu/scout/internal/control"
	"github.com/bcrusu/scout/internal/control/server/storage"
)

func (a *Assigner) updateAssignments() {
	servers := a.store.Servers()
	partitions := a.store.Partitions()

	curr := a.makeState(servers, partitions)
	next := NextState(a.config, curr)

	cmd := a.makeUpdateAssignments(partitions.AssignmentsVersion, curr, next)

	if cmd.IsEmpty() {
		return
	}

	if _, err := a.store.UpdateAssignments(cmd); err != nil {
		log.WithError(err).Error("Update assignments failed.")
	}
}

func (a *Assigner) makeState(servers *control.Servers, partitions *control.Partitions) *State {
	result := NewState()

	for sid := range servers.DataServers() {
		result.AddServer(sid)
	}

	for pid, part := range partitions.Items {
		result.AddPartition(pid)

		for name, replica := range part.Replicas {
			sid := replica.ServerId

			switch {
			case replica.State.IsServing():
				result.AddServing(sid, pid)
			case replica.State == control.ReplicaState_Joining:
				result.AddJoining(sid, pid)
			case replica.State == control.ReplicaState_Leaving:
				result.AddLeaving(sid, pid)
			default:
				panic(fmt.Sprintf("unhandled replica state %s", replica.State))
			}

			result.AddReplica(sid, pid, ReplicaState{
				Pid:         pid,
				Name:        name,
				CreatedTime: replica.CreatedTime.AsTime(),
				Ready:       replica.Ready,
			})
		}
	}

	return result
}

func (a *Assigner) makeUpdateAssignments(version uint64, curr, next *State) *storage.UpdateAssignments {
	cmd := &storage.UpdateAssignments{
		IfMatch:      version,
		MaxImbalance: uint32(next.MaxImbalance()),
	}

	for pid, nextPart := range next.Part {
		currPart := curr.Part[pid]

		for sid := range nextPart.Joining {
			if !currPart.Joining[sid] {
				cmd.Add = append(cmd.Add, &storage.UpdateAssignments_Add{
					PartitionId: pid,
					ServerId:    sid,
				})
			}
		}

		for sid := range nextPart.Serving {
			if !currPart.Serving[sid] {
				replica := next.GetReplica(sid, pid)

				cmd.Update = append(cmd.Update, &storage.UpdateAssignments_Update{
					PartitionId: pid,
					Replica:     replica.Name,
					State:       control.ReplicaState_Voter,
				})
			}
		}

		for sid := range nextPart.Leaving {
			if !currPart.Leaving[sid] {
				replica := next.GetReplica(sid, pid)

				cmd.Update = append(cmd.Update, &storage.UpdateAssignments_Update{
					PartitionId: pid,
					Replica:     replica.Name,
					State:       control.ReplicaState_Leaving,
				})
			}
		}

		for sid := range currPart.Leaving {
			if !nextPart.Leaving[sid] {
				replica := next.GetReplica(sid, pid)

				cmd.Remove = append(cmd.Remove, &storage.UpdateAssignments_Remove{
					PartitionId: pid,
					Replica:     replica.Name,
				})
			}
		}
	}

	return cmd
}
