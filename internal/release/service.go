package release

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/circuit"
	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/valve"
	"github.com/wyw14/cry-98/internal/vessel"
)

// partition is the journal partition that durably records the full release
// lifecycle: submission, review snapshot, endorsement, and the terminal
// consumption/outcome.
const partition = "releases"

// Event kinds written to the journal.
const (
	eventSubmitted = "release.submitted"
	eventObserved  = "release.observed"
	eventEndorsed  = "release.endorsed"
	eventConsumed  = "release.consumed"
	eventClosed    = "release.closed"
)

// EventAppender is the durable log. *journal.Store satisfies it.
type EventAppender interface {
	Append(model.Event) (model.Event, error)
}

// Service implements the field-review release workflow.
//
// Invariants enforced here:
//   - originator and countersigning reviewer must authenticate as two
//     distinct operators (independent credentials);
//   - a release is bound to one vessel / planned-liters / validity window and
//     is rejected once expired or once live parameters drift;
//   - a release drives a valve open at most once; a durable "consumed" marker
//     is appended before the valve command so concurrent calls and process
//     restarts can never consume it twice.
type Service struct {
	mu           sync.Mutex
	releases     map[uuid.UUID]*Release
	credentials  *CredentialStore
	vessels      *vessel.Registry
	circuits     *circuit.Registry
	reservations *circuit.Manager
	routes       *valve.RouteService
	journal      EventAppender
	now          func() time.Time
}

// NewService wires the workflow. The valve route service is used only to open
// (never close) the valve: a released manual fill is subsequently driven by
// the normal fill session lifecycle.
func NewService(
	credentials *CredentialStore,
	vessels *vessel.Registry,
	circuits *circuit.Registry,
	reservations *circuit.Manager,
	routes *valve.RouteService,
	store EventAppender,
	now func() time.Time,
) (*Service, error) {
	if credentials == nil || vessels == nil || circuits == nil || reservations == nil || routes == nil || store == nil {
		return nil, errors.New("release service dependencies are incomplete")
	}
	if now == nil {
		now = time.Now
	}
	return &Service{
		releases:     make(map[uuid.UUID]*Release),
		credentials:  credentials,
		vessels:      vessels,
		circuits:     circuits,
		reservations: reservations,
		routes:       routes,
		journal:      store,
		now:          now,
	}, nil
}

// SubmitRequest carries the originator's parameters and independent
// credential. Both the originator and the reviewing duty operator must
// authenticate, and they must be two different operators.
type SubmitRequest struct {
	VesselID         string
	PlannedLiters    float64
	ValidFor         time.Duration
	OriginatorID     string
	OriginatorSecret string
}

// Submit creates a pending release bound to the submitted parameter set. The
// submission itself is durably recorded.
func (s *Service) Submit(req SubmitRequest) (Release, error) {
	if req.ValidFor <= 0 {
		return Release{}, errors.New("validity duration must be positive")
	}
	if !s.credentials.Verify(req.OriginatorID, req.OriginatorSecret) {
		return Release{}, ErrInvalidCredential
	}
	now := s.now()
	r, err := newRelease(uuid.New(), req.VesselID, req.OriginatorID, req.PlannedLiters, now.Add(req.ValidFor), now)
	if err != nil {
		return Release{}, err
	}
	payload := submittedPayload{
		VesselID:      r.VesselID,
		PlannedLiters: r.PlannedLiters,
		ExpiresAt:     r.ExpiresAt,
		OriginatorID:  r.OriginatorID,
		CreatedAt:     r.CreatedAt,
	}
	if err := s.append(eventSubmitted, r.ID, payload, now); err != nil {
		return Release{}, err
	}
	s.mu.Lock()
	s.releases[r.ID] = &r
	s.mu.Unlock()
	return r, nil
}

// Observe returns the current vessel reading and every circuit's status for a
// reviewer who has presented valid independent credentials. The viewing is
// durably recorded (who reviewed which readings and when); the returned
// snapshot is what the reviewer is asked to countersign.
func (s *Service) Observe(releaseID uuid.UUID, reviewerID, reviewerSecret string) (Release, Observation, error) {
	if !s.credentials.Verify(reviewerID, reviewerSecret) {
		return Release{}, Observation{}, ErrInvalidCredential
	}
	reviewerID = trimID(reviewerID)

	s.mu.Lock()
	defer s.mu.Unlock()
	r, err := s.get(releaseID)
	if err != nil {
		return Release{}, Observation{}, err
	}
	now := s.now()
	if r.State.terminal() {
		return Release{}, Observation{}, errors.New("release is already closed")
	}
	if r.Expired(now) {
		return Release{}, Observation{}, ErrExpired
	}
	if reviewerID == r.OriginatorID {
		return Release{}, Observation{}, ErrSameOperator
	}
	observation, err := s.observe(r.VesselID)
	if err != nil {
		return Release{}, Observation{}, err
	}
	if err := s.appendLocked(eventObserved, r.ID, observedPayload{
		ReviewerID:  reviewerID,
		Observation: observation,
	}, now); err != nil {
		return Release{}, Observation{}, err
	}
	return r.snapshot(), observation, nil
}

// EndorseRequest is the countersignature.
type EndorseRequest struct {
	ReviewerID     string
	ReviewerSecret string
	Note           string
}

// Endorse validates the reviewer's independent credential, re-reads live
// vessel/circuit state, rejects expiry or parameter drift, records the
// observation and countersignature durably, and marks the release endorsed.
func (s *Service) Endorse(releaseID uuid.UUID, req EndorseRequest) (Release, error) {
	if !s.credentials.Verify(req.ReviewerID, req.ReviewerSecret) {
		return Release{}, ErrInvalidCredential
	}
	reviewerID := trimID(req.ReviewerID)

	s.mu.Lock()
	defer s.mu.Unlock()
	r, err := s.get(releaseID)
	if err != nil {
		return Release{}, err
	}
	now := s.now()
	if r.State == StatePending && r.Expired(now) {
		return Release{}, s.markExpiredLocked(r, "validity deadline elapsed before endorsement", now)
	}
	observation, err := s.observe(r.VesselID)
	if err != nil {
		return Release{}, err
	}
	if !parametersMatch(r, observation) {
		note := "live vessel/circuit parameters drifted from the submitted release"
		_ = s.persistClosedLocked(r, StateSuspended, note, now)
		return Release{}, ErrParameterMismatch
	}
	endorsement := Endorsement{ReviewerID: reviewerID, Observation: observation, EndorsedAt: now.UTC()}
	next, err := r.endorsed(endorsement, now)
	if err != nil {
		return Release{}, err
	}
	if err := s.appendLocked(eventObserved, r.ID, observedPayload{
		ReviewerID:  reviewerID,
		Observation: observation,
	}, now); err != nil {
		return Release{}, err
	}
	if err := s.appendLocked(eventEndorsed, r.ID, endorsedPayload{
		ReviewerID:  reviewerID,
		Observation: observation,
		Note:        req.Note,
		EndorsedAt:  endorsement.EndorsedAt,
	}, now); err != nil {
		return Release{}, err
	}
	*r = next
	return r.snapshot(), nil
}

// Consume opens the valve exactly once for an endorsed, unexpired release.
//
// The durable consumed marker is appended before the valve is opened. Under a
// crash between the marker and the physical open, the at-least-once transport
// plus idempotent command id makes recovery safe without ever allowing a
// second, distinct open. A concurrent or restarted second call is rejected
// with ErrAlreadyConsumed.
func (s *Service) Consume(ctx context.Context, releaseID uuid.UUID) (Release, Consumption, error) {
	if err := ctx.Err(); err != nil {
		return Release{}, Consumption{}, err
	}

	s.mu.Lock()
	r, err := s.get(releaseID)
	if err != nil {
		s.mu.Unlock()
		return Release{}, Consumption{}, err
	}
	now := s.now()
	switch {
	case r.State == StateConsumed:
		s.mu.Unlock()
		return Release{}, Consumption{}, ErrAlreadyConsumed
	case r.State != StateEndorsed:
		s.mu.Unlock()
		return Release{}, Consumption{}, ErrNotEndorsed
	case r.Expired(now):
		err := s.markExpiredLocked(r, "validity deadline elapsed before valve opening", now)
		s.mu.Unlock()
		return Release{}, Consumption{}, err
	}

	// Re-validate the bound parameters against live state and reserve a
	// circuit. Selection here (rather than at submit) means the reviewer's
	// observed circuit set is what is actually authorized. Reservation is the
	// mutual-exclusion guard against concurrent fills on the same circuit.
	observation, err := s.observe(r.VesselID)
	if err != nil {
		s.mu.Unlock()
		return Release{}, Consumption{}, err
	}
	if !parametersMatch(r, observation) {
		s.mu.Unlock()
		return Release{}, Consumption{}, ErrParameterMismatch
	}
	selected, err := s.selectCircuit(observation)
	if err != nil {
		s.mu.Unlock()
		return Release{}, Consumption{}, err
	}
	reservation, err := s.reservations.Reserve(selected.ID, r.VesselID, r.ID, now)
	if err != nil {
		s.mu.Unlock()
		return Release{}, Consumption{}, err
	}

	consumption := Consumption{
		SessionID:         r.ID,
		CircuitID:         selected.ID,
		CircuitGeneration: reservation.Generation,
		ValveID:           selected.ValveID,
		CommandID:         uuid.New(),
		ConsumedAt:        now.UTC(),
	}
	consumed := r
	closed, err := consumed.consumed(consumption, "valve open authorized", now)
	if err != nil {
		_ = s.reservations.Release(selected.ID, r.ID)
		s.mu.Unlock()
		return Release{}, Consumption{}, err
	}
	// Durable one-shot marker first. Until this fsync completes the valve is
	// not touched, so a crash leaves an endorsed release that can be retried;
	// after it completes the release can never be consumed again.
	if err := s.appendLocked(eventConsumed, r.ID, consumedPayload{
		Consumption: consumption,
		Note:        closed.OutcomeNote,
	}, now); err != nil {
		_ = s.reservations.Release(selected.ID, r.ID)
		s.mu.Unlock()
		return Release{}, Consumption{}, err
	}
	*r = closed
	snapshot := r.snapshot()
	s.mu.Unlock()

	route := valve.Route{
		SessionID:         r.ID,
		CircuitID:         selected.ID,
		CircuitGeneration: reservation.Generation,
		ValveID:           selected.ValveID,
		OpenCommandID:     consumption.CommandID,
		CloseCommandID:    uuid.New(),
	}
	openErr := s.routes.Open(ctx, route)
	if openErr != nil {
		// The release stays consumed: a failed/uncertain open must be handled
		// by operational recovery, never by re-issuing or re-consuming the
		// release. Release the circuit so it is not leaked, then record the
		// failure in the durable audit trail.
		_ = s.reservations.Release(selected.ID, r.ID)
		s.mu.Lock()
		failureNote := "valve open reported an error: " + openErr.Error()
		r.OutcomeNote = failureNote
		_ = s.appendLocked(eventClosed, r.ID, closedPayload{State: StateConsumed, Note: failureNote}, s.now())
		s.mu.Unlock()
		return snapshot, consumption, openErr
	}
	return snapshot, consumption, nil
}

// Finish closes the valve opened by the single consumption and releases the
// reserved circuit, then records the audited completion. It is the paired end
// of Consume; it is idempotent for an already completed release.
func (s *Service) Finish(ctx context.Context, releaseID uuid.UUID) (Release, error) {
	if err := ctx.Err(); err != nil {
		return Release{}, err
	}

	s.mu.Lock()
	r, err := s.get(releaseID)
	if err != nil {
		s.mu.Unlock()
		return Release{}, err
	}
	switch r.State {
	case StateCompleted:
		s.mu.Unlock()
		return r.snapshot(), nil
	case StateConsumed:
	default:
		s.mu.Unlock()
		return Release{}, errors.New("release has no open valve")
	}
	if r.Consumption == nil {
		s.mu.Unlock()
		return Release{}, errors.New("consumed release is missing its consumption record")
	}
	circuitID := r.Consumption.CircuitID
	now := s.now()
	next, err := r.completed("valve closed and circuit released", now)
	if err != nil {
		s.mu.Unlock()
		return Release{}, err
	}
	s.mu.Unlock()

	// Physical teardown outside the aggregate lock, mirroring the fill
	// completion flow: close the valve first, then free the circuit.
	if err := s.routes.Close(ctx, releaseID); err != nil {
		return Release{}, err
	}
	if err := s.reservations.Release(circuitID, releaseID); err != nil {
		return Release{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	// Re-check under the lock: a concurrent Finish may have completed it.
	if r.State == StateCompleted {
		return r.snapshot(), nil
	}
	if err := s.appendLocked(eventClosed, r.ID, closedPayload{
		State: StateCompleted, Note: next.OutcomeNote,
	}, s.now()); err != nil {
		return Release{}, err
	}
	*r = next
	return r.snapshot(), nil
}

func (s *Service) Reject(releaseID uuid.UUID, req EndorseRequest, reason string) (Release, error) {
	if !s.credentials.Verify(req.ReviewerID, req.ReviewerSecret) {
		return Release{}, ErrInvalidCredential
	}
	reviewerID := trimID(req.ReviewerID)
	if strings.TrimSpace(reason) == "" {
		return Release{}, errors.New("rejection reason is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r, err := s.get(releaseID)
	if err != nil {
		return Release{}, err
	}
	if reviewerID == r.OriginatorID {
		return Release{}, ErrSameOperator
	}
	now := s.now()
	if r.State.terminal() {
		return r.snapshot(), nil
	}
	note := "rejected by reviewer " + reviewerID + ": " + reason
	if err := s.appendLocked(eventClosed, r.ID, closedPayload{State: StateRejected, Note: note}, now); err != nil {
		return Release{}, err
	}
	next := r.closed(StateRejected, note, now)
	*r = next
	return next, nil
}

// Get returns a release by id.
func (s *Service) Get(id uuid.UUID) (Release, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.releases[id]
	if !ok {
		return Release{}, false
	}
	return r.snapshot(), true
}

// List returns all releases ordered by creation time.
func (s *Service) List() []Release {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]Release, 0, len(s.releases))
	for _, r := range s.releases {
		result = append(result, r.snapshot())
	}
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt.Before(result[j].CreatedAt) })
	return result
}

// Restore replays the durable release history into a fresh service. Because
// every consumed release is represented by a release.consumed event, a
// restarted process never treats a used release as reusable.
func (s *Service) Restore() error {
	events, err := s.journalEvents()
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, event := range events {
		if err := s.apply(event); err != nil {
			return err
		}
	}
	return nil
}

// ---- internals -----------------------------------------------------------

func (s *Service) get(id uuid.UUID) (*Release, error) {
	r, ok := s.releases[id]
	if !ok {
		return nil, errors.New("release not found")
	}
	return r, nil
}

// observe reads live vessel and circuit state. Caller decides locking.
func (s *Service) observe(vesselID string) (Observation, error) {
	v, ok := s.vessels.Get(vesselID)
	if !ok {
		return Observation{}, errors.New("release vessel is not registered")
	}
	views := make([]CircuitView, 0)
	for _, c := range s.circuits.List() {
		view := CircuitView{
			CircuitID: c.ID,
			ValveID:   c.ValveID,
			Enabled:   c.Enabled,
			Pressure:  c.Pressure,
		}
		if reservation, occupied := s.reservations.Get(c.ID); occupied {
			view.Reserved = true
			view.Faulted = reservation.Faulted
		}
		view.Available = c.Enabled && !view.Reserved && !view.Faulted
		views = append(views, view)
	}
	sort.Slice(views, func(i, j int) bool { return views[i].CircuitID < views[j].CircuitID })
	return Observation{
		VesselID:     v.ID,
		VesselStatus: string(v.Status),
		LevelMM:      v.LevelMM,
		PressureKPa:  v.PressureKPa,
		TemperatureC: v.TemperatureC,
		Circuits:     views,
		ObservedAt:   s.now().UTC(),
	}, nil
}

// selectCircuit chooses the same circuit the reviewer would have seen as the
// first available one, binding execution to the observed circuit set.
func (s *Service) selectCircuit(observation Observation) (circuit.Circuit, error) {
	for _, view := range observation.Circuits {
		if !view.Available {
			continue
		}
		if c, ok := s.circuits.Get(view.CircuitID); ok {
			return c, nil
		}
	}
	return circuit.Circuit{}, circuit.ErrCircuitBusy
}

// parametersMatch enforces that a release only ever authorizes its submitted
// parameter set. The vessel must still be the bound vessel in a fillable
// state and at least one observed circuit must be available.
func parametersMatch(r *Release, observation Observation) bool {
	if observation.VesselID != r.VesselID {
		return false
	}
	if observation.VesselStatus != string(model.VesselReady) && observation.VesselStatus != string(model.VesselFilling) {
		return false
	}
	available := false
	for _, view := range observation.Circuits {
		if view.Available {
			available = true
			break
		}
	}
	return available
}

func (s *Service) markExpiredLocked(r *Release, note string, now time.Time) error {
	if err := s.persistClosedLocked(r, StateExpired, note, now); err != nil {
		return err
	}
	return ErrExpired
}

func (s *Service) persistClosedLocked(r *Release, state State, note string, now time.Time) error {
	if err := s.appendLocked(eventClosed, r.ID, closedPayload{State: state, Note: note}, now); err != nil {
		return err
	}
	next := r.closed(state, note, now)
	*r = next
	return nil
}

func (s *Service) append(kind string, id uuid.UUID, payload any, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.appendLocked(kind, id, payload, now)
}

// appendLocked appends an event. Caller holds s.mu. The journal has its own
// lock and fsyncs, so this is the serialization point for one-shot semantics.
func (s *Service) appendLocked(kind string, id uuid.UUID, payload any, now time.Time) error {
	event, err := model.NewEvent(partition, kind, id.String(), payload, now)
	if err != nil {
		return err
	}
	if _, err := s.journal.Append(event); err != nil {
		return err
	}
	return nil
}

// journalEvents reads the whole release partition. Used only by Restore.
func (s *Service) journalEvents() ([]model.Event, error) {
	// Events(partition, after) is on *journal.Store; accept the same shape via
	// a tiny interface so the dependency stays on EventAppender elsewhere.
	reader, ok := s.journal.(interface {
		Events(string, uint64) ([]model.Event, error)
	})
	if !ok {
		return nil, errors.New("journal does not support event replay")
	}
	return reader.Events(partition, 0)
}

func (r *Release) snapshot() Release {
	return *r
}

func trimID(id string) string { return strings.TrimSpace(id) }
