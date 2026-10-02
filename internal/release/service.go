package release

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/circuit"
	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/operator"
	"github.com/wyw14/cry-98/internal/vessel"
)

var (
	ErrNotFound          = errors.New("release not found")
	ErrInvalidCredential = errors.New("operator credential is invalid")
	ErrReleaseExpired    = errors.New("release has expired")
	ErrReleaseSpent      = errors.New("release has already been used")
	ErrNotConfirmed      = errors.New("release is not confirmed")
	ErrSameOperator      = errors.New("reviewer must be a different operator than the initiator")
	ErrConsumptionFlight = errors.New("release is already being executed")
	ErrReadingChanged    = errors.New("vessel reading changed since the release was confirmed")
	ErrCircuitChanged    = errors.New("circuit state changed since the release was confirmed")
	ErrNoCircuit         = errors.New("no circuit is available for the release")
)

// EventStore is the durable journal the release audit trail lives in.
type EventStore interface {
	Append(model.Event) (model.Event, error)
	Events(partition string, after uint64) ([]model.Event, error)
}

// FillStarter executes the pre-authorised valve opening.
type FillStarter interface {
	StartWithPlan(ctx context.Context, plan model.ReleaseFillPlan) (model.FillSession, error)
}

// Partition is the journal partition that holds every release event.
const Partition = "releases"

type Service struct {
	mu       sync.Mutex
	releases map[uuid.UUID]model.Release
	inFlight map[uuid.UUID]bool

	journal   EventStore
	operators *operator.Directory
	vessels   *vessel.Registry
	circuits  *circuit.Registry
	manager   *circuit.Manager
	fills     FillStarter
	now       func() time.Time
}

func NewService(
	journal EventStore,
	operators *operator.Directory,
	vessels *vessel.Registry,
	circuits *circuit.Registry,
	manager *circuit.Manager,
	fills FillStarter,
	now func() time.Time,
) (*Service, error) {
	if journal == nil || operators == nil || vessels == nil || circuits == nil || manager == nil || fills == nil {
		return nil, errors.New("release service dependencies are required")
	}
	if operators.Count() < 2 {
		return nil, errors.New("at least two duty operators are required")
	}
	if now == nil {
		now = time.Now
	}
	return &Service{
		releases:  make(map[uuid.UUID]model.Release),
		inFlight:  make(map[uuid.UUID]bool),
		journal:   journal,
		operators: operators,
		vessels:   vessels,
		circuits:  circuits,
		manager:   manager,
		fills:     fills,
		now:       now,
	}, nil
}

// Propose creates a release bound to one vessel, one volume and one expiry.
func (s *Service) Propose(vesselID, operatorID, token string, liters float64, expiresAt time.Time) (model.Release, error) {
	if !s.operators.Verify(operatorID, token) {
		return model.Release{}, ErrInvalidCredential
	}
	if _, ok := s.vessels.Get(vesselID); !ok {
		return model.Release{}, errors.New("vessel not found")
	}
	record, err := model.NewRelease(vesselID, operatorID, liters, expiresAt, s.now())
	if err != nil {
		return model.Release{}, err
	}
	if err := s.appendState("release.proposed", record); err != nil {
		return model.Release{}, err
	}
	s.mu.Lock()
	s.releases[record.ID] = record
	s.mu.Unlock()
	return record, nil
}

// ReviewBundle is what the second operator sees before signing: the bound
// parameters, the live vessel reading, and every circuit's current state.
type ReviewBundle struct {
	Release  model.Release            `json:"release"`
	Vessel   model.VesselReading      `json:"vessel_reading"`
	Circuits []model.CircuitStateView `json:"circuits"`
}

// Review authenticates the reviewer and returns the plant state to be checked.
// It is read-only; the authoritative snapshot is taken at Confirm.
func (s *Service) Review(id uuid.UUID, reviewerID, token string) (ReviewBundle, error) {
	if !s.operators.Verify(reviewerID, token) {
		return ReviewBundle{}, ErrInvalidCredential
	}
	s.mu.Lock()
	record, ok := s.releases[id]
	s.mu.Unlock()
	if !ok {
		return ReviewBundle{}, ErrNotFound
	}
	if reviewerID == record.InitiatorID {
		return ReviewBundle{}, ErrSameOperator
	}
	reading, err := s.vesselReading(record.VesselID)
	if err != nil {
		return ReviewBundle{}, err
	}
	return ReviewBundle{
		Release:  record,
		Vessel:   reading,
		Circuits: s.circuitViews(),
	}, nil
}

// Confirm binds the reviewer's sign-off to the vessel reading and the circuit
// state at that instant. The release can afterwards only open that circuit
// while that exact state still holds.
func (s *Service) Confirm(id uuid.UUID, reviewerID, token string) (model.Release, error) {
	if !s.operators.Verify(reviewerID, token) {
		return model.Release{}, ErrInvalidCredential
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	record, ok := s.releases[id]
	if !ok {
		return model.Release{}, ErrNotFound
	}
	if record.Spent() {
		return model.Release{}, ErrReleaseSpent
	}
	if record.State != model.ReleaseProposed {
		return model.Release{}, errors.New("release has already been confirmed")
	}
	if reviewerID == record.InitiatorID {
		return model.Release{}, ErrSameOperator
	}
	if record.Expired(s.now()) {
		if _, err := s.invalidateLocked(record, model.InvalidationExpired, ErrReleaseExpired.Error(), reviewerID); err != nil {
			return model.Release{}, err
		}
		return model.Release{}, ErrReleaseExpired
	}

	reading, err := s.vesselReading(record.VesselID)
	if err != nil {
		return model.Release{}, err
	}
	view, err := s.selectCircuitView()
	if err != nil {
		_ = s.denyLocked(record, model.DenialConfirm, reviewerID, err.Error())
		return model.Release{}, err
	}
	confirmed, err := record.Confirm(reviewerID, reading, view, s.now())
	if err != nil {
		return model.Release{}, err
	}
	if err := s.appendState("release.confirmed", confirmed); err != nil {
		return model.Release{}, err
	}
	s.releases[id] = confirmed
	return confirmed, nil
}

// Consume opens the valve. It refuses when the release expired, the attested
// readings or circuit state changed, or another execution is in flight. A
// release can reach the valve exactly once; the intent is journaled before
// the command is issued.
func (s *Service) Consume(ctx context.Context, id uuid.UUID, operatorID, token string) (model.Release, error) {
	if !s.operators.Verify(operatorID, token) {
		return model.Release{}, ErrInvalidCredential
	}

	s.mu.Lock()
	record, ok := s.releases[id]
	if !ok {
		s.mu.Unlock()
		return model.Release{}, ErrNotFound
	}
	if record.Spent() {
		s.mu.Unlock()
		return model.Release{}, ErrReleaseSpent
	}
	if record.State != model.ReleaseConfirmed {
		s.mu.Unlock()
		return model.Release{}, ErrNotConfirmed
	}
	if s.inFlight[id] {
		_ = s.denyLocked(record, model.DenialConsume, operatorID, ErrConsumptionFlight.Error())
		s.mu.Unlock()
		return model.Release{}, ErrConsumptionFlight
	}

	if record.Expired(s.now()) {
		if _, err := s.invalidateLocked(record, model.InvalidationExpired, ErrReleaseExpired.Error(), operatorID); err != nil {
			s.mu.Unlock()
			return model.Release{}, err
		}
		s.mu.Unlock()
		return model.Release{}, ErrReleaseExpired
	}
	reading, err := s.vesselReading(record.VesselID)
	if err != nil {
		_ = s.denyLocked(record, model.DenialConsume, operatorID, err.Error())
		s.mu.Unlock()
		return model.Release{}, err
	}
	if readingsDiffer(reading, record.Confirmation.Vessel) {
		if _, err := s.invalidateLocked(record, model.InvalidationReading, ErrReadingChanged.Error(), operatorID); err != nil {
			s.mu.Unlock()
			return model.Release{}, err
		}
		s.mu.Unlock()
		return model.Release{}, ErrReadingChanged
	}
	current, err := s.viewFor(record.Confirmation.Circuit.CircuitID)
	if err != nil || circuitChanged(record.Confirmation.Circuit, current) {
		reason := ErrCircuitChanged.Error()
		if err != nil {
			reason = err.Error()
		}
		if _, invalidErr := s.invalidateLocked(record, model.InvalidationCircuit, reason, operatorID); invalidErr != nil {
			s.mu.Unlock()
			return model.Release{}, invalidErr
		}
		s.mu.Unlock()
		return model.Release{}, ErrCircuitChanged
	}

	plan := model.ReleaseFillPlan{
		ReleaseID:          record.ID,
		FillSessionID:      uuid.New(),
		VesselID:           record.VesselID,
		PlannedLiters:      record.PlannedLiters,
		CircuitID:          current.CircuitID,
		ExpectedValveID:    current.ValveID,
		ExpectedGeneration: current.Generation,
		OpenCommandID:      uuid.New(),
		CloseCommandID:     uuid.New(),
	}
	start := model.ConsumptionStart{
		FillSessionID:     plan.FillSessionID,
		CircuitID:         plan.CircuitID,
		CircuitGeneration: plan.ExpectedGeneration,
		ValveID:           plan.ExpectedValveID,
		OpenCommandID:     plan.OpenCommandID,
		CloseCommandID:    plan.CloseCommandID,
		OperatorID:        operatorID,
		StartedAt:         s.now().UTC(),
	}
	if err := s.appendEvent("release.consumption_started", record.ID.String(), start); err != nil {
		s.mu.Unlock()
		return model.Release{}, err
	}
	s.inFlight[id] = true
	s.mu.Unlock()

	_, startErr := s.fills.StartWithPlan(ctx, plan)

	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.inFlight, id)
	record = s.releases[id]

	consumption := model.Consumption{Start: start, Vessel: reading}
	if startErr != nil {
		// The valve outcome is unknown (or a guard refused before issuing):
		// fail closed, mark the release failed and never make it reusable.
		consumption.Executed = false
		consumption.Failure = startErr.Error()
		failed, markErr := record.MarkConsumption(consumption, s.now())
		if markErr != nil {
			return model.Release{}, markErr
		}
		if err := s.appendState("release.failed", failed); err != nil {
			return model.Release{}, err
		}
		s.releases[id] = failed
		return model.Release{}, startErr
	}
	consumption.Executed = true
	consumption.ReceiptMessage = "accepted"
	consumed, err := record.MarkConsumption(consumption, s.now())
	if err != nil {
		return model.Release{}, err
	}
	if err := s.appendState("release.consumed", consumed); err != nil {
		return model.Release{}, err
	}
	s.releases[id] = consumed
	return consumed, nil
}

func (s *Service) Get(id uuid.UUID) (model.Release, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.releases[id]
	return record, ok
}

func (s *Service) List() []model.Release {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]model.Release, 0, len(s.releases))
	for _, record := range s.releases {
		result = append(result, record)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ProposedAt.Before(result[j].ProposedAt) })
	return result
}

func (s *Service) vesselReading(vesselID string) (model.VesselReading, error) {
	v, ok := s.vessels.Get(vesselID)
	if !ok {
		return model.VesselReading{}, errors.New("vessel not found")
	}
	return model.VesselReading{
		VesselID:     v.ID,
		Generation:   v.Generation,
		Status:       v.Status,
		LevelMM:      v.LevelMM,
		PressureKPa:  v.PressureKPa,
		TemperatureC: v.TemperatureC,
		UpdatedAt:    v.UpdatedAt,
	}, nil
}

func (s *Service) circuitViews() []model.CircuitStateView {
	circuits := s.circuits.List()
	views := make([]model.CircuitStateView, 0, len(circuits))
	for _, candidate := range circuits {
		views = append(views, s.viewOf(candidate))
	}
	return views
}

func (s *Service) selectCircuitView() (model.CircuitStateView, error) {
	for _, candidate := range s.circuits.List() {
		view := s.viewOf(candidate)
		if candidate.Enabled && !view.Reserved && !view.Faulted {
			return view, nil
		}
	}
	return model.CircuitStateView{}, ErrNoCircuit
}

func (s *Service) viewFor(circuitID string) (model.CircuitStateView, error) {
	candidate, ok := s.circuits.Get(circuitID)
	if !ok {
		return model.CircuitStateView{}, errors.New("circuit is not configured")
	}
	return s.viewOf(candidate), nil
}

func (s *Service) viewOf(candidate circuit.Circuit) model.CircuitStateView {
	view := model.CircuitStateView{
		CircuitID:   candidate.ID,
		ValveID:     candidate.ValveID,
		Enabled:     candidate.Enabled,
		PressureKPa: candidate.Pressure,
		CheckedAt:   s.now().UTC(),
	}
	if reservation, occupied := s.manager.Get(candidate.ID); occupied {
		view.Reserved = true
		view.Faulted = reservation.Faulted
		view.Generation = reservation.Generation
	} else if next, tracked := s.manager.NextGeneration(candidate.ID); tracked {
		view.Generation = next
	}
	return view
}

func readingsDiffer(a, b model.VesselReading) bool {
	return a.Generation != b.Generation ||
		a.Status != b.Status ||
		a.LevelMM != b.LevelMM ||
		a.PressureKPa != b.PressureKPa ||
		a.TemperatureC != b.TemperatureC
}

func circuitChanged(bound, current model.CircuitStateView) bool {
	return current.Reserved || current.Faulted || !current.Enabled ||
		current.ValveID != bound.ValveID ||
		current.Generation != bound.Generation
}

func (s *Service) appendState(kind string, record model.Release) error {
	return s.appendEvent(kind, record.ID.String(), record)
}

func (s *Service) appendEvent(kind, aggregateID string, payload any) error {
	event, err := model.NewEvent(Partition, kind, aggregateID, payload, s.now())
	if err != nil {
		return err
	}
	_, err = s.journal.Append(event)
	return err
}

// denyLocked records a refused attempt without changing the release state.
// Caller must hold s.mu.
func (s *Service) denyLocked(record model.Release, stage model.DenialStage, operatorID, reason string) error {
	denial := model.ReleaseDenial{
		Stage:      stage,
		OperatorID: operatorID,
		Reason:     reason,
		DeniedAt:   s.now().UTC(),
	}
	if err := s.appendEvent("release.denied", record.ID.String(), denial); err != nil {
		return err
	}
	record.Audit = append(record.Audit, denial)
	record.UpdatedAt = s.now().UTC()
	s.releases[record.ID] = record
	return nil
}

// invalidateLocked permanently voids a release and journals the reason.
// Caller must hold s.mu.
func (s *Service) invalidateLocked(record model.Release, cause model.InvalidationCause, reason, operatorID string) (model.Release, error) {
	invalidated, err := record.Invalidate(cause, reason, operatorID, s.now())
	if err != nil {
		return model.Release{}, err
	}
	if err := s.appendState("release.invalidated", invalidated); err != nil {
		return model.Release{}, err
	}
	s.releases[record.ID] = invalidated
	return invalidated, nil
}
