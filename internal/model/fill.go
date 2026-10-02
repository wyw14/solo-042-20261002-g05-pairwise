package model

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

type FillState string

const (
	FillCreated     FillState = "created"
	FillPrechecking FillState = "prechecking"
	FillFilling     FillState = "filling"
	FillSoaking     FillState = "soaking"
	FillVerified    FillState = "verified"
	FillCompleted   FillState = "completed"
	FillCancelled   FillState = "cancelled"
	FillFailed      FillState = "failed"
)

type FillOrigin string

const (
	FillAutomatic FillOrigin = "automatic"
	FillManual    FillOrigin = "manual"
)

type FillSession struct {
	ID            uuid.UUID  `json:"id"`
	VesselID      string     `json:"vessel_id"`
	Origin        FillOrigin `json:"origin"`
	State         FillState  `json:"state"`
	Generation    uint64     `json:"generation"`
	CircuitID     string     `json:"circuit_id"`
	PlannedLiters float64    `json:"planned_liters"`
	Failure       string     `json:"failure,omitempty"`
	CreatedAt     time.Time  `json:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at"`
}

func NewFillSession(vesselID string, origin FillOrigin, generation uint64, liters float64, now time.Time) (FillSession, error) {
	return NewFillSessionWithID(uuid.New(), vesselID, origin, generation, liters, now)
}

// NewFillSessionWithID creates a session with a caller-assigned id, used when
// the identity must be journaled before the session exists (field releases).
func NewFillSessionWithID(id uuid.UUID, vesselID string, origin FillOrigin, generation uint64, liters float64, now time.Time) (FillSession, error) {
	if id == uuid.Nil {
		return FillSession{}, errors.New("fill session id is required")
	}
	if vesselID == "" || generation == 0 || liters <= 0 {
		return FillSession{}, errors.New("invalid fill session parameters")
	}
	if origin != FillAutomatic && origin != FillManual {
		return FillSession{}, errors.New("invalid fill origin")
	}
	return FillSession{
		ID:            id,
		VesselID:      vesselID,
		Origin:        origin,
		State:         FillCreated,
		Generation:    generation,
		PlannedLiters: liters,
		CreatedAt:     now.UTC(),
		UpdatedAt:     now.UTC(),
	}, nil
}

func (s FillSession) Active() bool {
	switch s.State {
	case FillCreated, FillPrechecking, FillFilling, FillSoaking, FillVerified:
		return true
	default:
		return false
	}
}

func (s FillSession) Transition(next FillState, now time.Time) (FillSession, error) {
	allowed := map[FillState]map[FillState]bool{
		FillCreated:     {FillPrechecking: true, FillCancelled: true, FillFailed: true},
		FillPrechecking: {FillFilling: true, FillCancelled: true, FillFailed: true},
		FillFilling:     {FillSoaking: true, FillCancelled: true, FillFailed: true},
		FillSoaking:     {FillVerified: true, FillFailed: true},
		FillVerified:    {FillCompleted: true, FillFailed: true},
	}
	if !allowed[s.State][next] {
		return FillSession{}, errors.New("invalid fill state transition")
	}
	s.State = next
	s.UpdatedAt = now.UTC()
	return s, nil
}

func (s FillSession) WithCircuit(id string, now time.Time) FillSession {
	s.CircuitID = id
	s.UpdatedAt = now.UTC()
	return s
}

func (s FillSession) Failed(message string, now time.Time) FillSession {
	s.State = FillFailed
	s.Failure = message
	s.UpdatedAt = now.UTC()
	return s
}
