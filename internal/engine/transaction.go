package engine

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"lamina/internal/datum"
	"lamina/internal/journal"
	"lamina/internal/versions"
	"sort"
	"time"
)

type staged struct {
	Mutation   versions.Mutation
	InsertOnly bool
	// Basis is the database sequence this edit was last validated against. It
	// starts at the head when staged and moves only when the caller explicitly
	// confirms an overwrite or restages the key, letting a salvaged session tell
	// whether a committed change arrived before or after the caller acted.
	Basis uint64
	// Confirmed records that the caller explicitly accepted overwriting a
	// committed version observed at Basis. It never clears unique contention.
	Confirmed bool
}
type Transaction struct {
	ID       string `json:"id"`
	Snapshot uint64 `json:"snapshot"`
	ReadOnly bool   `json:"read_only"`
	Started  string `json:"started"`
	// State is active for normal sessions and salvaged after a failed commit
	// whose caller elected to retain the private write set. Only active
	// transactions accept new staging and commits; salvaged sessions must be
	// explicitly rebased or rolled back.
	State string `json:"state"`
	// OriginalSnapshot records the read basis the session began with. Rebase
	// deliberately keeps Snapshot unchanged so reads never silently advance;
	// this field makes the original invalidation boundary auditable regardless.
	OriginalSnapshot uint64 `json:"original_snapshot"`
	// salvageOffered records that this session has already been through the
	// salvage boundary once. After a rebase the session is a normal active
	// transaction again: further commit failures fail closed instead of
	// re-entering salvage, so retained context cannot become an infinite
	// auto-retry loop and every retry is an explicit caller decision.
	salvageOffered bool
	writes         map[string]map[string]staged
}
type Commit struct {
	Sequence uint64 `json:"sequence"`
	Writes   int    `json:"writes"`
}

func (d *DB) Begin(readOnly bool) (Transaction, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return Transaction{}, err
	}
	if len(d.transactions) >= 128 {
		return Transaction{}, fault("capacity", "128 active transactions reached")
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return Transaction{}, err
	}
	tx := &Transaction{ID: hex.EncodeToString(id[:]), Snapshot: d.sequence, ReadOnly: readOnly, Started: time.Now().UTC().Format(time.RFC3339Nano), State: stateActive, OriginalSnapshot: d.sequence, writes: map[string]map[string]staged{}}
	d.transactions[tx.ID] = tx
	out := *tx
	out.writes = nil
	return out, nil
}
func (d *DB) tx(id string) (*Transaction, error) {
	tx, ok := d.transactions[id]
	if !ok {
		return nil, fault("not_found", "transaction not active")
	}
	return tx, nil
}
func (d *DB) Put(id, table string, row datum.Row, insertOnly bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return err
	}
	tx, err := d.tx(id)
	if err != nil {
		return err
	}
	if tx.ReadOnly {
		return fault("conflict", "read-only transaction")
	}
	if tx.State == stateSalvaged {
		return fault("conflict", "salvaged transaction cannot stage edits; rebase before editing or rollback to discard")
	}
	r, err := d.catalog.Relation(table)
	if err != nil {
		return invalid(err)
	}
	if r.Created > tx.Snapshot {
		return fault("conflict", "relation outside transaction catalog snapshot")
	}
	normalized, err := r.Normalize(row)
	if err != nil {
		return invalid(err)
	}
	key := r.Key(normalized)
	rows := d.store.Scan(table, tx.Snapshot)
	overlay(rows, tx.writes, table)
	if insertOnly {
		if _, ok := rows[key]; ok {
			return fault("conflict", "primary already exists")
		}
	}
	if tx.writes[table] == nil {
		tx.writes[table] = map[string]staged{}
	}
	if err = tx.reserve(table, key); err != nil {
		return err
	}
	tx.writes[table][key] = staged{Mutation: versions.Mutation{Relation: table, Key: key, Row: normalized}, InsertOnly: insertOnly, Basis: d.sequence}
	return nil
}
func (tx *Transaction) reserve(table, key string) error {
	if _, exists := tx.writes[table][key]; exists {
		return nil
	}
	count := 0
	for _, m := range tx.writes {
		count += len(m)
	}
	if count >= 1000 {
		return fault("capacity", "transaction mutation limit 1000")
	}
	return nil
}
func (d *DB) Delete(id, table string, value datum.Value) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return err
	}
	tx, err := d.tx(id)
	if err != nil {
		return err
	}
	if tx.ReadOnly {
		return fault("conflict", "read-only transaction")
	}
	if tx.State == stateSalvaged {
		return fault("conflict", "salvaged transaction cannot stage edits; rebase before editing or rollback to discard")
	}
	r, err := d.catalog.Relation(table)
	if err != nil {
		return invalid(err)
	}
	if r.Created > tx.Snapshot {
		return fault("conflict", "relation outside transaction catalog snapshot")
	}
	col, _ := r.Column(r.Primary)
	if value.Null() || value.Kind != col.Type {
		return fault("invalid", "invalid primary value")
	}
	key := value.Key()
	if tx.writes[table] == nil {
		tx.writes[table] = map[string]staged{}
	}
	if err = tx.reserve(table, key); err != nil {
		return err
	}
	tx.writes[table][key] = staged{Mutation: versions.Mutation{Relation: table, Key: key, Deleted: true}, Basis: d.sequence}
	return nil
}
func (d *DB) Rollback(id string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return err
	}
	if _, err := d.tx(id); err != nil {
		return err
	}
	delete(d.transactions, id)
	return nil
}

// Commit validates the private write set, then durably appends and publishes
// it. On a validation failure the default behavior preserves the baseline
// fail-closed semantics and discards the whole write set. With
// salvageOnConflict the same failure instead retains the write set in a
// salvaged session whose per-edit basis the caller must explicitly resolve.
// Durable write failures are never salvageable: they poison the instance.
func (d *DB) Commit(id string, salvageOnConflict bool) (Commit, *SalvageReport, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return Commit{}, nil, err
	}
	tx, err := d.tx(id)
	if err != nil {
		return Commit{}, nil, err
	}
	if tx.State == stateSalvaged {
		return Commit{}, nil, fault("conflict", "salvaged transaction cannot commit; rebase and resolve flagged edits first, or rollback")
	}
	writes := make([]versions.Mutation, 0)
	if err = d.validateWrites(tx); err != nil {
		var conflict *conflictError
		// Salvage is offered exactly once, at the first failed validation of a
		// session that has never crossed the salvage boundary. Rebased sessions
		// and callers that elected abort keep the fail-closed behavior.
		if !salvageOnConflict || tx.salvageOffered || !errors.As(err, &conflict) {
			// Baseline fail-closed client behavior: a failed commit closes the
			// transaction and publishes none of its private rows.
			delete(d.transactions, id)
			return Commit{}, nil, d.asEngineError(err)
		}
		report := d.retain(tx)
		return Commit{}, &report, fault(conflict.kind, "%s; write set retained for explicit correction", conflict.message)
	}
	for _, mutations := range tx.writes {
		for _, m := range mutations {
			writes = append(writes, m.Mutation)
		}
	}
	if len(writes) == 0 {
		// Read-only/empty commit consumes no sequence, as in the baseline.
		delete(d.transactions, id)
		return Commit{Sequence: tx.Snapshot}, nil, nil
	}
	sort.Slice(writes, func(i, j int) bool {
		if writes[i].Relation == writes[j].Relation {
			return writes[i].Key < writes[j].Key
		}
		return writes[i].Relation < writes[j].Relation
	})
	if err = d.publish(journal.Record{Kind: "commit", Transaction: id, Writes: writes}); err != nil {
		// publish marks the journal poisoned; the instance fails closed and no
		// salvage is offered because the durable outcome is uncertain.
		delete(d.transactions, id)
		return Commit{}, nil, err
	}
	d.commits++
	delete(d.transactions, id)
	return Commit{Sequence: d.sequence, Writes: len(writes)}, nil, nil
}

// asEngineError normalizes an internal validation error into the engine error
// envelope transport already understands.
func (d *DB) asEngineError(err error) error {
	if conflict, ok := err.(*conflictError); ok {
		return fault(conflict.kind, "%s", conflict.message)
	}
	return err
}
