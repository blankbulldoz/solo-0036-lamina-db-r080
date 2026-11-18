package transport

import (
	"lamina/internal/datum"
	"lamina/internal/engine"
	"net/http"
)

func (s *Server) begin(w http.ResponseWriter, r *http.Request) {
	var in struct {
		ReadOnly bool `json:"read_only"`
	}
	if !decode(w, r, &in) {
		return
	}
	out, err := s.db.Begin(in.ReadOnly)
	if err != nil {
		respondError(w, err)
		return
	}
	write(w, 201, out)
}
func (s *Server) put(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Relation   string    `json:"relation"`
		Row        datum.Row `json:"row"`
		InsertOnly bool      `json:"insert_only"`
	}
	if !decode(w, r, &in) {
		return
	}
	if err := s.db.Put(r.PathValue("tx"), in.Relation, in.Row, in.InsertOnly); err != nil {
		respondError(w, err)
		return
	}
	write(w, 200, map[string]string{"state": "staged"})
}
func (s *Server) deleteRow(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Relation string      `json:"relation"`
		Key      datum.Value `json:"key"`
	}
	if !decode(w, r, &in) {
		return
	}
	if err := s.db.Delete(r.PathValue("tx"), in.Relation, in.Key); err != nil {
		respondError(w, err)
		return
	}
	write(w, 200, map[string]string{"state": "staged"})
}
func (s *Server) commit(w http.ResponseWriter, r *http.Request) {
	// The body is optional: callers that do not send one keep the baseline
	// fail-closed behavior where a failed commit discards the write set.
	var in struct {
		OnConflict string `json:"on_conflict"`
	}
	if r.ContentLength != 0 {
		if !decode(w, r, &in) {
			return
		}
	} else {
		r.Body = http.NoBody
	}
	salvage := false
	switch in.OnConflict {
	case "", "abort":
		salvage = false
	case "salvage":
		salvage = true
	default:
		respondError(w, &engine.Error{Code: "invalid", Message: "on_conflict must be abort or salvage"})
		return
	}
	out, report, err := s.db.Commit(r.PathValue("tx"), salvage)
	if err != nil {
		if report != nil {
			respondErrorWith(w, err, map[string]any{"salvage": report})
			return
		}
		respondError(w, err)
		return
	}
	write(w, 200, out)
}
func (s *Server) rebase(w http.ResponseWriter, r *http.Request) {
	report, err := s.db.Rebase(r.PathValue("tx"))
	if err != nil {
		respondError(w, err)
		return
	}
	write(w, 200, report)
}
func (s *Server) confirmEdit(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Relation         string `json:"relation"`
		Key              string `json:"key"`
		ObservedSequence uint64 `json:"observed_sequence"`
	}
	if !decode(w, r, &in) {
		return
	}
	if in.Relation == "" || in.Key == "" {
		respondError(w, &engine.Error{Code: "invalid", Message: "relation and key are required"})
		return
	}
	report, err := s.db.ConfirmEdit(r.PathValue("tx"), in.Relation, in.Key, in.ObservedSequence)
	if err != nil {
		respondError(w, err)
		return
	}
	write(w, 200, report)
}
func (s *Server) inspectTx(w http.ResponseWriter, r *http.Request) {
	report, err := s.db.Inspect(r.PathValue("tx"))
	if err != nil {
		respondError(w, err)
		return
	}
	write(w, 200, report)
}
func (s *Server) rollback(w http.ResponseWriter, r *http.Request) {
	if err := s.db.Rollback(r.PathValue("tx")); err != nil {
		respondError(w, err)
		return
	}
	write(w, 200, map[string]string{"state": "rolled_back"})
}
func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Relation    string      `json:"relation"`
		Key         datum.Value `json:"key"`
		Transaction string      `json:"transaction"`
	}
	if !decode(w, r, &in) {
		return
	}
	out, err := s.db.Get(in.Transaction, in.Relation, in.Key)
	if err != nil {
		respondError(w, err)
		return
	}
	write(w, 200, map[string]any{"row": out})
}
