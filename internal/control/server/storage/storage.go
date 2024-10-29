package storage

func (u *UpdateAssignments) IsEmpty() bool {
	return len(u.Add) == 0 && len(u.Update) == 0 && len(u.Remove) == 0
}
