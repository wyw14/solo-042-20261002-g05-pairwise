package release

import (
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
)

// State is the lifecycle of a field-review release.
type State string

const (
	StatePending   State = "pending"   // submitted, awaiting the second operator's endorsement
	StateEndorsed  State = "endorsed"  // countersigned, awaiting the one-shot valve opening
	StateConsumed  State = "consumed"  // valve opened exactly once, fill in progress
	StateCompleted State = "completed" // valve closed and circuit released after the fill
	StateRejected  State = "rejected"  // reviewer refused countersignature after inspecting
	StateExpired   State = "expired"   // validity deadline passed before endorsement
	StateSuspended State = "suspended" // parameters changed (vessel/circuit drift) before endorsement
)

func (s State) terminal() bool {
	switch s {
	case StateCompleted, StateRejected, StateExpired, StateSuspended:
		return true
	default:
		return false
	}
}

// CircuitView is one circuit's status as seen during the field review.
type CircuitView struct {
	CircuitID string  `json:"circuit_id"`
	ValveID   string  `json:"valve_id"`
	Enabled   bool    `json:"enabled"`
	Reserved  bool    `json:"reserved"`
	Faulted   bool    `json:"faulted"`
	Pressure  float64 `json:"pressure_kpa"`
	Available bool    `json:"available"`
}

// Observation is the live vessel reading and circuit status captured by the
// reviewing duty operator at countersign time. It is the independent evidence
// the second pair of eyes relies on.
type Observation struct {
	VesselID     string        `json:"vessel_id"`
	VesselStatus string        `json:"vessel_status"`
	LevelMM      float64       `json:"level_mm"`
	PressureKPa  float64       `json:"pressure_kpa"`
	TemperatureC float64       `json:"temperature_c"`
	Circuits     []CircuitView `json:"circuits"`
	ObservedAt   time.Time     `json:"observed_at"`
}

// Endorsement records the countersigning duty operator and the exact
// observation they countersigned against.
type Endorsement struct {
	ReviewerID  string      `json:"reviewer_id"`
	Observation Observation `json:"observation"`
	EndorsedAt  time.Time   `json:"endorsed_at"`
}

// Consumption is the terminal audit record of the single valve opening.
type Consumption struct {
	SessionID         uuid.UUID `json:"session_id"`
	CircuitID         string    `json:"circuit_id"`
	CircuitGeneration uint64    `json:"circuit_generation"`
	ValveID           string    `json:"valve_id"`
	CommandID         uuid.UUID `json:"command_id"`
	ReceiptDuplicate  bool      `json:"receipt_duplicate"`
	ConsumedAt        time.Time `json:"consumed_at"`
}

// Release is the aggregate root. A release is bound to exactly one
// (vessel, planned liters, validity window) parameter set and may drive a
// valve open at most once.
type Release struct {
	ID            uuid.UUID    `json:"id"`
	VesselID      string       `json:"vessel_id"`
	PlannedLiters float64      `json:"planned_liters"`
	ExpiresAt     time.Time    `json:"expires_at"`
	OriginatorID  string       `json:"originator_id"`
	State         State        `json:"state"`
	Endorsement   *Endorsement `json:"endorsement,omitempty"`
	Consumption   *Consumption `json:"consumption,omitempty"`
	OutcomeNote   string       `json:"outcome_note,omitempty"`
	CreatedAt     time.Time    `json:"created_at"`
	UpdatedAt     time.Time    `json:"updated_at"`
}

// Usable reports whether the release may still drive a valve opening.
func (r Release) Usable(now time.Time) bool {
	return r.State == StateEndorsed && !r.Expired(now)
}

func newRelease(id uuid.UUID, vesselID, originatorID string, liters float64, expiresAt, now time.Time) (Release, error) {
	vesselID = strings.TrimSpace(vesselID)
	originatorID = strings.TrimSpace(originatorID)
	if id == uuid.Nil {
		return Release{}, errors.New("release id is required")
	}
	if vesselID == "" {
		return Release{}, errors.New("vessel id is required")
	}
	if originatorID == "" {
		return Release{}, errors.New("originator id is required")
	}
	if liters <= 0 {
		return Release{}, errors.New("planned liters must be positive")
	}
	if !expiresAt.After(now) {
		return Release{}, errors.New("validity deadline must be in the future")
	}
	return Release{
		ID:            id,
		VesselID:      vesselID,
		PlannedLiters: liters,
		ExpiresAt:     expiresAt.UTC(),
		OriginatorID:  originatorID,
		State:         StatePending,
		CreatedAt:     now.UTC(),
		UpdatedAt:     now.UTC(),
	}, nil
}

// Expired reports whether the release validity window has elapsed.
func (r Release) Expired(now time.Time) bool {
	return !now.UTC().Before(r.ExpiresAt)
}

func (r Release) endorsed(e Endorsement, now time.Time) (Release, error) {
	if r.State != StatePending {
		return Release{}, errors.New("release is not awaiting endorsement")
	}
	if r.Expired(now) {
		return Release{}, ErrExpired
	}
	if strings.TrimSpace(e.ReviewerID) == "" {
		return Release{}, errors.New("reviewer id is required")
	}
	if e.ReviewerID == r.OriginatorID {
		return Release{}, ErrSameOperator
	}
	if e.Observation.VesselID != r.VesselID {
		return Release{}, errors.New("observation vessel does not match release")
	}
	r.State = StateEndorsed
	r.Endorsement = &e
	r.UpdatedAt = now.UTC()
	return r, nil
}

func (r Release) consumed(c Consumption, note string, now time.Time) (Release, error) {
	if r.State != StateEndorsed {
		return Release{}, errors.New("release is not endorsed")
	}
	if r.Expired(now) {
		return Release{}, ErrExpired
	}
	r.State = StateConsumed
	r.Consumption = &c
	r.OutcomeNote = note
	r.UpdatedAt = now.UTC()
	return r, nil
}

func (r Release) closed(state State, note string, now time.Time) Release {
	r.State = state
	r.OutcomeNote = note
	r.UpdatedAt = now.UTC()
	return r
}

// completed moves a consumed release (valve open, fill in progress) to its
// audited end state after the valve is closed and the circuit released.
func (r Release) completed(note string, now time.Time) (Release, error) {
	if r.State != StateConsumed || r.Consumption == nil {
		return Release{}, errors.New("release has no open valve to complete")
	}
	r.State = StateCompleted
	if note != "" {
		r.OutcomeNote = note
	}
	r.UpdatedAt = now.UTC()
	return r, nil
}
