package model

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

// ReleaseState tracks the two-person field release lifecycle. Every terminal
// state is single-use: a release that has been consumed, has failed at the
// valve, or has been invalidated can never open a valve again.
type ReleaseState string

const (
	ReleaseProposed    ReleaseState = "proposed"
	ReleaseConfirmed   ReleaseState = "confirmed"
	ReleaseConsumed    ReleaseState = "consumed"
	ReleaseFailed      ReleaseState = "failed"
	ReleaseInvalidated ReleaseState = "invalidated"
)

// DenialStage identifies where a release was turned down.
type DenialStage string

const (
	DenialConfirm DenialStage = "confirm"
	DenialConsume DenialStage = "consume"
)

// InvalidationCause explains why a release was permanently voided.
type InvalidationCause string

const (
	InvalidationExpired InvalidationCause = "expired"
	InvalidationReading InvalidationCause = "vessel_reading_changed"
	InvalidationCircuit InvalidationCause = "circuit_state_changed"
)

// VesselReading is the immutable gauge snapshot a reviewer signs against.
type VesselReading struct {
	VesselID     string       `json:"vessel_id"`
	Generation   uint64       `json:"generation"`
	Status       VesselStatus `json:"status"`
	LevelMM      float64      `json:"level_mm"`
	PressureKPa  float64      `json:"pressure_kpa"`
	TemperatureC float64      `json:"temperature_c"`
	UpdatedAt    time.Time    `json:"updated_at"`
}

// CircuitStateView describes a circuit at a specific instant. A confirmed
// release binds to exactly one such view; the same view must still hold when
// the release is consumed.
type CircuitStateView struct {
	CircuitID   string    `json:"circuit_id"`
	ValveID     string    `json:"valve_id"`
	Enabled     bool      `json:"enabled"`
	PressureKPa float64   `json:"pressure_kpa"`
	Generation  uint64    `json:"generation"`
	Reserved    bool      `json:"reserved"`
	Faulted     bool      `json:"faulted"`
	CheckedAt   time.Time `json:"checked_at"`
}

// Confirmation records the second duty operator's sign-off and the plant
// state that was attested at that moment.
type Confirmation struct {
	ReviewerID  string           `json:"reviewer_id"`
	ConfirmedAt time.Time        `json:"confirmed_at"`
	Vessel      VesselReading    `json:"vessel"`
	Circuit     CircuitStateView `json:"circuit"`
}

// ConsumptionStart names every identity involved in opening the valve. It is
// journaled before the command is issued so a crash cannot leave a release
// reusable after its valve command may already have gone out.
type ConsumptionStart struct {
	FillSessionID     uuid.UUID `json:"fill_session_id"`
	CircuitID         string    `json:"circuit_id"`
	CircuitGeneration uint64    `json:"circuit_generation"`
	ValveID           string    `json:"valve_id"`
	OpenCommandID     uuid.UUID `json:"open_command_id"`
	CloseCommandID    uuid.UUID `json:"close_command_id"`
	OperatorID        string    `json:"operator_id"`
	StartedAt         time.Time `json:"started_at"`
}

// Consumption is the durable record of the final valve-open result.
type Consumption struct {
	Start          ConsumptionStart `json:"start"`
	Vessel         VesselReading    `json:"vessel"`
	Executed       bool             `json:"executed"`
	ReceiptMessage string           `json:"receipt_message,omitempty"`
	Failure        string           `json:"failure,omitempty"`
	Recovered      bool             `json:"recovered"`
	ConsumedAt     time.Time        `json:"consumed_at"`
}

// Invalidation records why a release was permanently voided.
type Invalidation struct {
	Cause      InvalidationCause `json:"cause"`
	Reason     string            `json:"reason"`
	OperatorID string            `json:"operator_id,omitempty"`
	At         time.Time         `json:"at"`
}

// ReleaseDenial is an audit entry for an attempt that was refused without
// changing the release lifecycle state (for example a concurrent duplicate
// execution while the first is still in flight).
type ReleaseDenial struct {
	Stage      DenialStage `json:"stage"`
	OperatorID string      `json:"operator_id,omitempty"`
	Reason     string      `json:"reason"`
	DeniedAt   time.Time   `json:"denied_at"`
}

// Release is a single-use, two-person authorization bound to one vessel, one
// planned volume and one validity window.
type Release struct {
	ID            uuid.UUID       `json:"id"`
	VesselID      string          `json:"vessel_id"`
	PlannedLiters float64         `json:"planned_liters"`
	ExpiresAt     time.Time       `json:"expires_at"`
	InitiatorID   string          `json:"initiator_id"`
	State         ReleaseState    `json:"state"`
	Confirmation  *Confirmation   `json:"confirmation,omitempty"`
	Consumption   *Consumption    `json:"consumption,omitempty"`
	Invalidation  *Invalidation   `json:"invalidation,omitempty"`
	Audit         []ReleaseDenial `json:"audit,omitempty"`
	ProposedAt    time.Time       `json:"proposed_at"`
	UpdatedAt     time.Time       `json:"updated_at"`
}

// NewRelease creates a proposed release for the attested parameter set.
func NewRelease(vesselID, initiatorID string, liters float64, expiresAt, now time.Time) (Release, error) {
	if vesselID == "" || initiatorID == "" {
		return Release{}, errors.New("release vessel and initiator are required")
	}
	if liters <= 0 {
		return Release{}, errors.New("release planned volume must be positive")
	}
	if !expiresAt.After(now) {
		return Release{}, errors.New("release expiry must be in the future")
	}
	return Release{
		ID:            uuid.New(),
		VesselID:      vesselID,
		PlannedLiters: liters,
		ExpiresAt:     expiresAt.UTC(),
		InitiatorID:   initiatorID,
		State:         ReleaseProposed,
		ProposedAt:    now.UTC(),
		UpdatedAt:     now.UTC(),
	}, nil
}

// Expired reports whether the validity window has elapsed.
func (r Release) Expired(now time.Time) bool {
	return now.UTC().After(r.ExpiresAt)
}

// Spent reports whether the release has already been used or voided.
func (r Release) Spent() bool {
	switch r.State {
	case ReleaseConsumed, ReleaseFailed, ReleaseInvalidated:
		return true
	default:
		return false
	}
}

// Confirm moves a proposed release to confirmed, binding the reviewer and the
// attested plant state. Lifecycle/expiry guards are the caller's
// responsibility; this enforces the two-different-operator rule.
func (r Release) Confirm(reviewerID string, vessel VesselReading, circuit CircuitStateView, now time.Time) (Release, error) {
	if r.State != ReleaseProposed {
		return Release{}, errors.New("release is not awaiting confirmation")
	}
	if reviewerID == "" {
		return Release{}, errors.New("reviewer identity is required")
	}
	if reviewerID == r.InitiatorID {
		return Release{}, errors.New("reviewer must be a different operator than the initiator")
	}
	r.Confirmation = &Confirmation{
		ReviewerID:  reviewerID,
		ConfirmedAt: now.UTC(),
		Vessel:      vessel,
		Circuit:     circuit,
	}
	r.State = ReleaseConfirmed
	r.UpdatedAt = now.UTC()
	return r, nil
}

// Invalidate permanently voids a release that has expired or whose bound
// parameters no longer hold. Only a live (proposed or confirmed) release can
// be invalidated.
func (r Release) Invalidate(cause InvalidationCause, reason, operatorID string, now time.Time) (Release, error) {
	if r.Spent() {
		return Release{}, errors.New("release is already spent")
	}
	r.Invalidation = &Invalidation{Cause: cause, Reason: reason, OperatorID: operatorID, At: now.UTC()}
	r.State = ReleaseInvalidated
	r.UpdatedAt = now.UTC()
	return r, nil
}

// MarkConsumption terminates the release with the valve-open result.
func (r Release) MarkConsumption(result Consumption, now time.Time) (Release, error) {
	if r.State != ReleaseConfirmed {
		return Release{}, errors.New("release is not confirmed")
	}
	result.ConsumedAt = now.UTC()
	r.Consumption = &result
	if result.Executed {
		r.State = ReleaseConsumed
	} else {
		r.State = ReleaseFailed
	}
	r.UpdatedAt = now.UTC()
	return r, nil
}

// RecoverInterrupted closes out a release whose consumption_started event was
// journaled but whose result never was. It is never made reusable: the valve
// command may already have been issued, so fail closed and force a fresh
// release.
func (r Release) RecoverInterrupted(result Consumption, now time.Time) (Release, error) {
	if result.Start.FillSessionID == uuid.Nil {
		return Release{}, errors.New("interrupted consumption is missing its fill session")
	}
	result.Executed = true
	result.Recovered = true
	if result.ReceiptMessage == "" {
		result.ReceiptMessage = "recovered after interrupted execution"
	}
	result.ConsumedAt = now.UTC()
	r.Consumption = &result
	r.State = ReleaseConsumed
	r.UpdatedAt = now.UTC()
	return r, nil
}

// ReleaseFillPlan carries the pre-authorised identities for opening a valve
// under a confirmed release. Every identity is generated before the intent is
// journaled, so the durable record and the physical command always agree.
type ReleaseFillPlan struct {
	ReleaseID          uuid.UUID `json:"release_id"`
	FillSessionID      uuid.UUID `json:"fill_session_id"`
	VesselID           string    `json:"vessel_id"`
	PlannedLiters      float64   `json:"planned_liters"`
	CircuitID          string    `json:"circuit_id"`
	ExpectedValveID    string    `json:"expected_valve_id"`
	ExpectedGeneration uint64    `json:"expected_generation"`
	OpenCommandID      uuid.UUID `json:"open_command_id"`
	CloseCommandID     uuid.UUID `json:"close_command_id"`
}

func (p ReleaseFillPlan) Validate() error {
	if p.ReleaseID == uuid.Nil || p.FillSessionID == uuid.Nil ||
		p.OpenCommandID == uuid.Nil || p.CloseCommandID == uuid.Nil {
		return errors.New("release fill plan identities are incomplete")
	}
	if p.VesselID == "" || p.CircuitID == "" || p.ExpectedValveID == "" || p.PlannedLiters <= 0 {
		return errors.New("release fill plan parameters are invalid")
	}
	return nil
}
