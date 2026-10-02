package release

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/model"
)

// Journal payloads. Each state transition is one event; Restore replays them
// in order to reconstruct every release, including the terminal consumed
// marker that prevents a used release being opened again after restart.

type submittedPayload struct {
	VesselID      string    `json:"vessel_id"`
	PlannedLiters float64   `json:"planned_liters"`
	ExpiresAt     time.Time `json:"expires_at"`
	OriginatorID  string    `json:"originator_id"`
	CreatedAt     time.Time `json:"created_at"`
}

type observedPayload struct {
	ReviewerID  string      `json:"reviewer_id"`
	Observation Observation `json:"observation"`
}

type endorsedPayload struct {
	ReviewerID  string      `json:"reviewer_id"`
	Observation Observation `json:"observation,omitempty"`
	Note        string      `json:"note,omitempty"`
	EndorsedAt  time.Time   `json:"endorsed_at"`
}

type consumedPayload struct {
	Consumption Consumption `json:"consumption"`
	Note        string      `json:"note,omitempty"`
}

type closedPayload struct {
	State State  `json:"state"`
	Note  string `json:"note,omitempty"`
}

// apply folds one journal event into the in-memory aggregate map. Called with
// s.mu held during Restore.
func (s *Service) apply(event model.Event) error {
	id, err := uuid.Parse(event.AggregateID)
	if err != nil {
		return err
	}
	switch event.Kind {
	case eventSubmitted:
		var p submittedPayload
		if err := json.Unmarshal(event.Payload, &p); err != nil {
			return err
		}
		r, err := newRelease(id, p.VesselID, p.OriginatorID, p.PlannedLiters, p.ExpiresAt, p.CreatedAt)
		if err != nil {
			return err
		}
		s.releases[id] = &r
		return nil

	case eventObserved:
		// Observation is persisted alongside endorsement; nothing to fold
		// beyond what the endorsed event carries.
		return nil

	case eventEndorsed:
		var p endorsedPayload
		if err := json.Unmarshal(event.Payload, &p); err != nil {
			return err
		}
		r, ok := s.releases[id]
		if !ok {
			return errors.New("endorsement refers to unknown release")
		}
		next, err := r.endorsed(Endorsement{
			ReviewerID:  p.ReviewerID,
			Observation: p.Observation,
			EndorsedAt:  p.EndorsedAt,
		}, p.EndorsedAt)
		if err != nil {
			// A release expired on the wall clock between writes must not
			// block replay of history; endorsement was valid when recorded.
			if errors.Is(err, ErrExpired) {
				r.State = StateEndorsed
				e := Endorsement{ReviewerID: p.ReviewerID, Observation: p.Observation, EndorsedAt: p.EndorsedAt}
				r.Endorsement = &e
				return nil
			}
			return err
		}
		*r = next
		return nil

	case eventConsumed:
		var p consumedPayload
		if err := json.Unmarshal(event.Payload, &p); err != nil {
			return err
		}
		r, ok := s.releases[id]
		if !ok {
			return errors.New("consumption refers to unknown release")
		}
		// Force the terminal state regardless of the replay wall clock: a
		// consumed release is consumed forever.
		r.State = StateConsumed
		c := p.Consumption
		r.Consumption = &c
		if p.Note != "" {
			r.OutcomeNote = p.Note
		}
		return nil

	case eventClosed:
		var p closedPayload
		if err := json.Unmarshal(event.Payload, &p); err != nil {
			return err
		}
		r, ok := s.releases[id]
		if !ok {
			return errors.New("closure refers to unknown release")
		}
		// A consumed marker always wins over a later failure note, but the
		// legitimate consumed -> completed teardown is applied.
		if r.State == StateConsumed && p.State != StateConsumed && p.State != StateCompleted {
			r.OutcomeNote = p.Note
			return nil
		}
		r.State = p.State
		if p.Note != "" {
			r.OutcomeNote = p.Note
		}
		return nil

	default:
		// Unknown event kinds are ignored so older logs stay forward
		// compatible.
		return nil
	}
}
