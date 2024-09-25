package storage

// TODO
func (p *txnProcessor) acquireLocks(locks []*Lock) error {
	// check for rerentrant locks
	// e.g. txn deletes key then inserts it
	// e.g. tx deletes key range than inserts (delete vertex and eddges, then create same vertex)

	return nil
}

func (p *txnProcessor) releaseLocks(locks []*Lock) {

}

func (p *txnProcessor) restoreLocks() {

}
