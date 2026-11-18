package engine

import (
	"sort"

	"lamina/internal/versions"
)

// State values for the transaction lifecycle. The baseline paths active ->
// staged -> committed/rolled_back and staged -> aborted remain; salvaged is an
// explicit, caller-elected continuation after a failed commit validation.
const (
	stateActive   = "active"
	stateSalvaged = "salvaged"
)

// EditView is one still-private staged edit together with the validation facts
// computed against the database head at the moment the owning transaction was
// last rebased. It is the "edit basis" the caller can reason about without the
// server silently reinterpreting data the caller never observed.
type EditView struct {
	Relation    string            `json:"relation"`
	Key         string            `json:"key"`
	Mutation    versions.Mutation `json:"mutation"`
	Basis       uint64            `json:"basis"`
	WriteStale  bool              `json:"write_stale"`
	UniqueStale bool              `json:"unique_stale"`
	Reconfirm   bool              `json:"requires_confirmation"`
	Reasons     []string          `json:"reasons,omitempty"`
}

// SalvageReport is returned (inside a 409 or from rebase/inspection) whenever
// a private write set is retained. Every edit is classified independently so
// the caller can see which edits remain committable and which must be
// reconfirmed against newer committed state.
type SalvageReport struct {
	TransactionID    string     `json:"transaction_id"`
	State            string     `json:"state"`
	OriginalSnapshot uint64     `json:"original_snapshot"`
	Snapshot         uint64     `json:"snapshot"`
	Head             uint64     `json:"head"`
	RetainedEdits    int        `json:"retained_edits"`
	WriteStaleEdits  int        `json:"write_stale_edits"`
	UniqueStaleEdits int        `json:"unique_stale_edits"`
	ReconfirmEdits   int        `json:"requires_confirmation_edits"`
	Blocked          bool       `json:"blocked"`
	Edits            []EditView `json:"edits"`
}

// conflictKind distinguishes the failure families that must never be presented
// to callers as one generic retryable state:
//   - write_conflict: another committed transaction moved the same key first
//   - unique_conflict: the final visible state violates a unique index
//   - unavailable (handled elsewhere): durable write outcome uncertain, the
//     instance fails closed and requires a restart before any further request.
const (
	kindWriteConflict  = "write_conflict"
	kindUniqueConflict = "unique_conflict"
)

// conflictError carries the first validation failure kind while the full
// per-edit classification lives in the SalvageReport produced alongside it.
type conflictError struct {
	kind    string
	message string
}

func (e *conflictError) Error() string { return e.message }

// classifyEdits recomputes every validation fact for the private write set
// against the current head without mutating any staged row. The write set is
// treated as a candidate final state: same-key first-committer-wins plus the
// unique constraints evaluated over the fully overlaid resulting rows.
func (d *DB) classifyEdits(tx *Transaction) []EditView {
	views := make([]EditView, 0)
	tables := make([]string, 0, len(tx.writes))
	for table := range tx.writes {
		tables = append(tables, table)
	}
	sort.Strings(tables)
	for _, table := range tables {
		// Unique-index facts computed over the candidate final visible state,
		// matching the semantics of constraints.unique used by Commit.
		uniqueRows := d.store.Scan(table, d.sequence)
		overlay(uniqueRows, tx.writes, table)
		violatingValues := map[string]map[string]bool{}
		for _, idx := range d.catalog.Indexes {
			if idx.Relation != table || !idx.Unique {
				continue
			}
			values := map[string]string{}
			for rowKey, row := range uniqueRows {
				v := row[idx.Column]
				if v.Null() {
					continue
				}
				token := v.Key()
				if prior, exists := values[token]; exists && prior != rowKey {
					if violatingValues[idx.Name] == nil {
						violatingValues[idx.Name] = map[string]bool{}
					}
					violatingValues[idx.Name][token] = true
					continue
				}
				values[token] = rowKey
			}
		}
		keys := make([]string, 0, len(tx.writes[table]))
		for key := range tx.writes[table] {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			st := tx.writes[table][key]
			view := EditView{
				Relation: table,
				Key:      key,
				Mutation: st.Mutation,
				Basis:    st.Basis,
				Reasons:  []string{},
			}
			if latest := d.store.Latest(table, key); latest > st.Basis {
				view.WriteStale = true
				view.Reasons = append(view.Reasons, "committed version newer than last validated basis")
			}
			if !st.Mutation.Deleted {
				for _, idx := range d.catalog.Indexes {
					if idx.Relation != table || !idx.Unique {
						continue
					}
					v := st.Mutation.Row[idx.Column]
					if v.Null() {
						continue
					}
					if violatingValues[idx.Name][v.Key()] {
						view.UniqueStale = true
						view.Reasons = append(view.Reasons, "unique index "+idx.Name+" contended at head")
					}
				}
			}
			view.Reconfirm = view.WriteStale || view.UniqueStale
			if len(view.Reasons) == 0 {
				view.Reasons = nil
			}
			views = append(views, view)
		}
	}
	return views
}

func (d *DB) buildReport(tx *Transaction) SalvageReport {
	views := d.classifyEdits(tx)
	report := SalvageReport{
		TransactionID:    tx.ID,
		State:            tx.State,
		OriginalSnapshot: tx.OriginalSnapshot,
		Snapshot:         tx.Snapshot,
		Head:             d.sequence,
		RetainedEdits:    len(views),
		Edits:            views,
	}
	for _, view := range views {
		if view.WriteStale {
			report.WriteStaleEdits++
		}
		if view.UniqueStale {
			report.UniqueStaleEdits++
		}
		if view.Reconfirm {
			report.ReconfirmEdits++
		}
	}
	report.Blocked = report.ReconfirmEdits > 0
	return report
}

// validateWrites runs the same final-state checks as the baseline Commit path
// and returns a typed conflictError on the first blocking validation failure.
// Same-key conflicts compare against each edit's validated basis rather than
// the transaction snapshot, so an explicit confirmation at an observed head is
// honored; unique constraints are always evaluated against the current head and
// can never be confirmed away.
func (d *DB) validateWrites(tx *Transaction) error {
	for table, mutations := range tx.writes {
		for key, st := range mutations {
			if d.store.Latest(table, key) > st.Basis {
				d.conflicts++
				return &conflictError{kind: kindWriteConflict, message: "write conflict on " + table}
			}
		}
		if err := d.unique(table, tx.writes); err != nil {
			d.conflicts++
			return &conflictError{kind: kindUniqueConflict, message: err.Error()}
		}
	}
	return nil
}

// retain moves a transaction that failed validation into the salvaged state.
// The private write set, read snapshot and original snapshot are untouched.
// Each edit's validation basis is pinned to the read snapshot so the first
// report accurately describes every committed change that invalidated it;
// subsequent rebases reclassify without moving that basis until the caller
// explicitly confirms or restages an edit.
func (d *DB) retain(tx *Transaction) SalvageReport {
	tx.State = stateSalvaged
	tx.salvageOffered = true
	for table, mutations := range tx.writes {
		for key, st := range mutations {
			st.Basis = tx.Snapshot
			st.Confirmed = false
			tx.writes[table][key] = st
		}
	}
	return d.buildReport(tx)
}
