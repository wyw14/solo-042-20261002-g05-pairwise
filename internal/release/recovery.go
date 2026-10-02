package release

import (
	"encoding/json"

	"github.com/wyw14/cry-98/internal/model"
)

// Recover rebuilds every release from the journal. A release whose
// consumption_started was durable but whose outcome event never arrived is
// closed out as consumed (fail closed): its valve command may already have
// been issued, so it must never become reusable. The closure is journaled so
// later restarts observe the same result.
func (s *Service) Recover() error {
	events, err := s.journal.Events(Partition, 0)
	if err != nil {
		return err
	}

	latest := make(map[string]model.Release)
	pendingStart := make(map[string]model.ConsumptionStart)
	// Denials are their own event stream. State snapshots also carry them
	// inline, so the audit trail is rebuilt here from the events alone to
	// avoid double counting on replay.
	denials := make(map[string][]model.ReleaseDenial)

	for _, event := range events {
		switch event.Kind {
		case "release.proposed", "release.confirmed", "release.consumed",
			"release.failed", "release.invalidated", "release.recovered":
			var record model.Release
			if err := json.Unmarshal(event.Payload, &record); err != nil {
				return err
			}
			record.Audit = nil
			latest[event.AggregateID] = record
			if record.Spent() {
				delete(pendingStart, event.AggregateID)
			}
		case "release.consumption_started":
			var start model.ConsumptionStart
			if err := json.Unmarshal(event.Payload, &start); err != nil {
				return err
			}
			pendingStart[event.AggregateID] = start
		case "release.denied":
			var denial model.ReleaseDenial
			if err := json.Unmarshal(event.Payload, &denial); err != nil {
				return err
			}
			denials[event.AggregateID] = append(denials[event.AggregateID], denial)
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for id, record := range latest {
		if trail, ok := denials[id]; ok {
			record.Audit = trail
		}
		if start, interrupted := pendingStart[id]; interrupted {
			consumption := model.Consumption{Start: start, Vessel: record.Confirmation.Vessel}
			closed, err := record.RecoverInterrupted(consumption, s.now())
			if err != nil {
				return err
			}
			if err := s.appendState("release.recovered", closed); err != nil {
				return err
			}
			s.releases[record.ID] = closed
			continue
		}
		s.releases[record.ID] = record
	}
	return nil
}
