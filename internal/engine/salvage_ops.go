package engine

// Rebase reclassifies every retained edit against the current head and returns
// the session to active so the caller can keep correcting the same write set.
// It deliberately does NOT advance the read snapshot: transaction-scoped reads
// keep returning the data this session has actually seen (repeatable read plus
// its own private writes), so rebase can never silently substitute newer
// committed rows for stale ones. To observe newer committed state the caller
// issues an explicit current-read query; to accept overwriting it the caller
// uses ConfirmEdit with the head it just observed. Per-edit validation bases
// and stale flags are also left untouched until an explicit confirm or restage.
func (d *DB) Rebase(id string) (SalvageReport, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return SalvageReport{}, err
	}
	tx, err := d.tx(id)
	if err != nil {
		return SalvageReport{}, err
	}
	if tx.State != stateSalvaged {
		return SalvageReport{}, fault("conflict", "rebase requires a salvaged transaction")
	}
	// Read basis and the retention pin it holds are unchanged. The session
	// becomes active again so retained keys can be restaged and edits can be
	// explicitly confirmed; the returned report still names every edit that
	// needs correction against the current head.
	tx.State = stateActive
	report := d.buildReport(tx)
	return report, nil
}

// ConfirmEdit explicitly accepts one write-stale edit after the caller has
// inspected the newer committed state. observedSequence must equal the current
// head, proving the acceptance is based on data the caller has actually seen;
// it cannot be used to swallow a change that landed after the last inspection.
// Unique contention is never clearable here because accepting a duplicate does
// not make the resulting state valid — the staged row itself must be edited.
func (d *DB) ConfirmEdit(id, table, key string, observedSequence uint64) (SalvageReport, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return SalvageReport{}, err
	}
	tx, err := d.tx(id)
	if err != nil {
		return SalvageReport{}, err
	}
	if tx.State != stateActive {
		return SalvageReport{}, fault("conflict", "confirmation requires an active transaction; rebase the salvaged session first")
	}
	if observedSequence != d.sequence {
		return SalvageReport{}, fault("conflict", "observed sequence %d does not match current head %d; re-inspect before confirming", observedSequence, d.sequence)
	}
	mutations, ok := tx.writes[table]
	if !ok {
		return SalvageReport{}, fault("not_found", "staged edit not found")
	}
	st, ok := mutations[key]
	if !ok {
		return SalvageReport{}, fault("not_found", "staged edit not found")
	}
	// Recompute this edit's facts at the current head rather than trusting a
	// cached flag, so a commit that raced between inspection and confirmation
	// cannot be silently accepted.
	fresh := d.classifyEdits(tx)
	var current *EditView
	for i := range fresh {
		if fresh[i].Relation == table && fresh[i].Key == key {
			current = &fresh[i]
			break
		}
	}
	if current == nil {
		return SalvageReport{}, fault("not_found", "staged edit not found")
	}
	if current.UniqueStale {
		return SalvageReport{}, fault("unique_conflict", "edit still violates a unique index at head; restage the row instead of confirming")
	}
	if !current.WriteStale {
		// Nothing to confirm: leave state untouched and report the basis.
		return d.buildReport(tx), nil
	}
	// The caller has observed exactly this head and accepts overwriting it. The
	// per-edit basis advances so the write is no longer stale relative to the
	// data the caller has seen; Commit still revalidates against the head.
	st.Basis = d.sequence
	st.Confirmed = true
	mutations[key] = st
	return d.buildReport(tx), nil
}

// Inspect returns the current per-edit classification without changing any
// state. It works for both active and salvaged sessions.
func (d *DB) Inspect(id string) (SalvageReport, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if err := d.usable(); err != nil {
		return SalvageReport{}, err
	}
	tx, err := d.tx(id)
	if err != nil {
		return SalvageReport{}, err
	}
	return d.buildReport(tx), nil
}
