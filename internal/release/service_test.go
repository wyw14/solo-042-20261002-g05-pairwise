package release

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/circuit"
	"github.com/wyw14/cry-98/internal/journal"
	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/valve"
	"github.com/wyw14/cry-98/internal/vessel"
)

type harness struct {
	service     *Service
	store       *journal.Store
	vessels     *vessel.Registry
	circuits    *circuit.Registry
	manager     *circuit.Manager
	transport   *countingTransport
	credentials *CredentialStore
	clock       *time.Time
}

type countingTransport struct {
	mu      sync.Mutex
	opens   int
	receipt map[uuid.UUID]model.ValveReceipt
}

func (t *countingTransport) Execute(ctx context.Context, command model.ValveCommand) (model.ValveReceipt, error) {
	if err := ctx.Err(); err != nil {
		return model.ValveReceipt{}, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.opens++
	receipt := model.ValveReceipt{CommandID: command.ID, Executed: true, Message: "accepted", ReceivedAt: time.Now().UTC()}
	t.receipt[command.ID] = receipt
	return receipt, nil
}

func (t *countingTransport) openCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.opens
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	start := time.Unix(1_700_000_000, 0).UTC()
	clock := &start
	now := func() time.Time { return *clock }

	store, err := journal.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	vessels := vessel.NewRegistry()
	v, err := model.NewVessel("V-03", "Cryogenic vessel 03", "north", now())
	if err != nil {
		t.Fatal(err)
	}
	if err := vessels.Register(v); err != nil {
		t.Fatal(err)
	}
	circuits := circuit.NewRegistry(
		circuit.Circuit{ID: "C-A", Zone: "north", ValveID: "VALVE-A", Enabled: true, Pressure: 120},
		circuit.Circuit{ID: "C-B", Zone: "south", ValveID: "VALVE-B", Enabled: true, Pressure: 121},
	)
	manager := circuit.NewManager("C-A", "C-B")

	transport := &countingTransport{receipt: make(map[uuid.UUID]model.ValveReceipt)}
	controller, err := valve.NewController(transport, now)
	if err != nil {
		t.Fatal(err)
	}
	commands := valve.NewCommandService(controller, now)
	routes := valve.NewRouteService(commands)

	credentials := NewCredentialStore()
	if err := credentials.Register("duty-a", "secret-a"); err != nil {
		t.Fatal(err)
	}
	if err := credentials.Register("duty-b", "secret-b"); err != nil {
		t.Fatal(err)
	}
	service, err := NewService(credentials, vessels, circuits, manager, routes, store, now)
	if err != nil {
		t.Fatal(err)
	}
	return &harness{
		service:     service,
		store:       store,
		vessels:     vessels,
		circuits:    circuits,
		manager:     manager,
		transport:   transport,
		credentials: credentials,
		clock:       clock,
	}
}

func (h *harness) advance(d time.Duration) { *h.clock = h.clock.Add(d) }

func TestSubmitInspectEndorseConsumeOnce(t *testing.T) {
	h := newHarness(t)

	r, err := h.service.Submit(SubmitRequest{
		VesselID: "V-03", PlannedLiters: 150, ValidFor: 10 * time.Minute, OriginatorID: "duty-a", OriginatorSecret: "secret-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	if r.State != StatePending {
		t.Fatalf("state=%s", r.State)
	}

	viewed, observation, err := h.service.Observe(r.ID, "duty-b", "secret-b")
	if err != nil {
		t.Fatal(err)
	}
	if viewed.ID != r.ID || observation.VesselID != "V-03" {
		t.Fatalf("unexpected observation: %+v", observation)
	}
	if len(observation.Circuits) != 2 || !observation.Circuits[0].Available {
		t.Fatalf("circuits not available: %+v", observation.Circuits)
	}

	endorsed, err := h.service.Endorse(r.ID, EndorseRequest{ReviewerID: "duty-b", ReviewerSecret: "secret-b"})
	if err != nil {
		t.Fatal(err)
	}
	if endorsed.State != StateEndorsed || endorsed.Endorsement == nil || endorsed.Endorsement.ReviewerID != "duty-b" {
		t.Fatalf("bad endorsement: %+v", endorsed)
	}

	consumed, consumption, err := h.service.Consume(context.Background(), r.ID)
	if err != nil {
		t.Fatal(err)
	}
	if consumed.State != StateConsumed || consumption.ValveID != "VALVE-A" || consumption.CircuitID != "C-A" {
		t.Fatalf("bad consumption: %+v %+v", consumed, consumption)
	}
	if consumption.SessionID != r.ID {
		t.Fatalf("consumption session_id=%s want %s", consumption.SessionID, r.ID)
	}
	if h.transport.openCount() != 1 {
		t.Fatalf("valve opens=%d", h.transport.openCount())
	}
	if reservation, occupied := h.manager.Get("C-A"); !occupied || reservation.SessionID != r.ID {
		t.Fatal("circuit was not bound to the release")
	}

	// The same release must never open the valve again.
	if _, _, err := h.service.Consume(context.Background(), r.ID); !errors.Is(err, ErrAlreadyConsumed) {
		t.Fatalf("second consume err=%v", err)
	}
	if h.transport.openCount() != 1 {
		t.Fatalf("valve opens after retry=%d", h.transport.openCount())
	}
}

func TestOriginatorRequiresCredential(t *testing.T) {
	h := newHarness(t)
	if _, err := h.service.Submit(SubmitRequest{
		VesselID: "V-03", PlannedLiters: 10, ValidFor: time.Minute,
		OriginatorID: "duty-a", OriginatorSecret: "wrong",
	}); !errors.Is(err, ErrInvalidCredential) {
		t.Fatalf("bad originator credential err=%v", err)
	}
}

func TestReviewerMustBeDifferentOperator(t *testing.T) {
	h := newHarness(t)
	r, err := h.service.Submit(SubmitRequest{VesselID: "V-03", PlannedLiters: 10, ValidFor: time.Minute, OriginatorID: "duty-a", OriginatorSecret: "secret-a"})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := h.service.Observe(r.ID, "duty-a", "secret-a"); !errors.Is(err, ErrSameOperator) {
		t.Fatalf("self inspect err=%v", err)
	}
	if _, err := h.service.Endorse(r.ID, EndorseRequest{ReviewerID: "duty-a", ReviewerSecret: "secret-a"}); !errors.Is(err, ErrSameOperator) {
		t.Fatalf("self endorse err=%v", err)
	}
}

func TestReviewerRequiresIndependentCredential(t *testing.T) {
	h := newHarness(t)
	r, err := h.service.Submit(SubmitRequest{VesselID: "V-03", PlannedLiters: 10, ValidFor: time.Minute, OriginatorID: "duty-a", OriginatorSecret: "secret-a"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.service.Endorse(r.ID, EndorseRequest{ReviewerID: "duty-b", ReviewerSecret: "wrong"}); !errors.Is(err, ErrInvalidCredential) {
		t.Fatalf("bad credential err=%v", err)
	}
	if _, err := h.service.Endorse(r.ID, EndorseRequest{ReviewerID: "ghost", ReviewerSecret: "secret-b"}); !errors.Is(err, ErrInvalidCredential) {
		t.Fatalf("unknown reviewer err=%v", err)
	}
}

func TestExpiredReleaseIsRejected(t *testing.T) {
	h := newHarness(t)
	r, err := h.service.Submit(SubmitRequest{VesselID: "V-03", PlannedLiters: 10, ValidFor: time.Minute, OriginatorID: "duty-a", OriginatorSecret: "secret-a"})
	if err != nil {
		t.Fatal(err)
	}
	h.advance(2 * time.Minute)
	if _, err := h.service.Endorse(r.ID, EndorseRequest{ReviewerID: "duty-b", ReviewerSecret: "secret-b"}); !errors.Is(err, ErrExpired) {
		t.Fatalf("expired endorse err=%v", err)
	}
	stored, _ := h.service.Get(r.ID)
	if stored.State != StateExpired {
		t.Fatalf("state=%s", stored.State)
	}
}

func TestParameterDriftSuspendsRelease(t *testing.T) {
	h := newHarness(t)
	r, err := h.service.Submit(SubmitRequest{VesselID: "V-03", PlannedLiters: 10, ValidFor: 10 * time.Minute, OriginatorID: "duty-a", OriginatorSecret: "secret-a"})
	if err != nil {
		t.Fatal(err)
	}
	// Every circuit becomes unavailable (reserved elsewhere): parameters no
	// longer match what the reviewer is expected to authorize.
	if _, err := h.manager.Reserve("C-A", "V-07", uuid.New(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := h.manager.Reserve("C-B", "V-09", uuid.New(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := h.service.Endorse(r.ID, EndorseRequest{ReviewerID: "duty-b", ReviewerSecret: "secret-b"}); !errors.Is(err, ErrParameterMismatch) {
		t.Fatalf("drift endorse err=%v", err)
	}
	stored, _ := h.service.Get(r.ID)
	if stored.State != StateSuspended {
		t.Fatalf("state=%s", stored.State)
	}
	if _, _, err := h.service.Consume(context.Background(), r.ID); !errors.Is(err, ErrNotEndorsed) {
		t.Fatalf("suspended consume err=%v", err)
	}
	if h.transport.openCount() != 0 {
		t.Fatalf("valve opened despite drift: %d", h.transport.openCount())
	}
}

func TestConsumeBeforeEndorsementRejected(t *testing.T) {
	h := newHarness(t)
	r, err := h.service.Submit(SubmitRequest{VesselID: "V-03", PlannedLiters: 10, ValidFor: time.Minute, OriginatorID: "duty-a", OriginatorSecret: "secret-a"})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := h.service.Consume(context.Background(), r.ID); !errors.Is(err, ErrNotEndorsed) {
		t.Fatalf("consume err=%v", err)
	}
	if h.transport.openCount() != 0 {
		t.Fatal("valve opened without endorsement")
	}
}

func TestConcurrentConsumeOpensValveOnce(t *testing.T) {
	h := newHarness(t)
	r, err := h.service.Submit(SubmitRequest{VesselID: "V-03", PlannedLiters: 10, ValidFor: time.Minute, OriginatorID: "duty-a", OriginatorSecret: "secret-a"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.service.Endorse(r.ID, EndorseRequest{ReviewerID: "duty-b", ReviewerSecret: "secret-b"}); err != nil {
		t.Fatal(err)
	}

	const runners = 16
	var wg sync.WaitGroup
	results := make(chan error, runners)
	start := make(chan struct{})
	for range runners {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, _, err := h.service.Consume(context.Background(), r.ID)
			results <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	successes := 0
	for err := range results {
		if err == nil {
			successes++
		} else if !errors.Is(err, ErrAlreadyConsumed) {
			t.Fatalf("unexpected consume error: %v", err)
		}
	}
	if successes != 1 {
		t.Fatalf("successful consumes=%d", successes)
	}
	if h.transport.openCount() != 1 {
		t.Fatalf("valve opens=%d", h.transport.openCount())
	}
}

func TestRestoredConsumedReleaseCannotBeReconsumed(t *testing.T) {
	h := newHarness(t)
	r, err := h.service.Submit(SubmitRequest{VesselID: "V-03", PlannedLiters: 10, ValidFor: time.Minute, OriginatorID: "duty-a", OriginatorSecret: "secret-a"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.service.Endorse(r.ID, EndorseRequest{ReviewerID: "duty-b", ReviewerSecret: "secret-b"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := h.service.Consume(context.Background(), r.ID); err != nil {
		t.Fatal(err)
	}

	// Simulate a process restart: brand new registries/service over the same
	// durable journal, then replay.
	reopened, err := journal.NewStore(h.store.Root())
	if err != nil {
		t.Fatal(err)
	}
	events, err := reopened.Events("releases", 0)
	if err != nil {
		t.Fatal(err)
	}
	sawConsumed := false
	for _, event := range events {
		if event.Kind == "release.consumed" && event.AggregateID == r.ID.String() {
			sawConsumed = true
		}
	}
	if !sawConsumed {
		t.Fatal("consumed marker was not persisted")
	}

	freshVessels := vessel.NewRegistry()
	freshCircuits := circuit.NewRegistry(circuit.Circuit{ID: "C-A", ValveID: "VALVE-A", Enabled: true})
	freshManager := circuit.NewManager("C-A")
	freshTransport := &countingTransport{receipt: make(map[uuid.UUID]model.ValveReceipt)}
	controller, _ := valve.NewController(freshTransport, time.Now)
	routes := valve.NewRouteService(valve.NewCommandService(controller, time.Now))
	restored, err := NewService(h.credentials, freshVessels, freshCircuits, freshManager, routes, reopened, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.Restore(); err != nil {
		t.Fatal(err)
	}
	got, ok := restored.Get(r.ID)
	if !ok {
		t.Fatal("release missing after restore")
	}
	if got.State != StateConsumed || got.Consumption == nil || got.Endorsement == nil {
		t.Fatalf("restored release lost its audit trail: %+v", got)
	}
	if _, _, err := restored.Consume(context.Background(), r.ID); !errors.Is(err, ErrAlreadyConsumed) {
		t.Fatalf("reconsume after restart err=%v", err)
	}
	if freshTransport.openCount() != 0 {
		t.Fatal("restart reopened a consumed release")
	}
}

func TestFinishClosesValveAndReleasesCircuit(t *testing.T) {
	h := newHarness(t)
	r, err := h.service.Submit(SubmitRequest{VesselID: "V-03", PlannedLiters: 10, ValidFor: time.Minute, OriginatorID: "duty-a", OriginatorSecret: "secret-a"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.service.Endorse(r.ID, EndorseRequest{ReviewerID: "duty-b", ReviewerSecret: "secret-b"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := h.service.Consume(context.Background(), r.ID); err != nil {
		t.Fatal(err)
	}

	// Finish before consume-pair is invalid for a pending release elsewhere;
	// here the release is consumed so Finish must tear down.
	finished, err := h.service.Finish(context.Background(), r.ID)
	if err != nil {
		t.Fatal(err)
	}
	if finished.State != StateCompleted {
		t.Fatalf("state=%s", finished.State)
	}
	if _, occupied := h.manager.Get("C-A"); occupied {
		t.Fatal("circuit was not released on finish")
	}

	// Finish is idempotent and must not touch the valve again.
	again, err := h.service.Finish(context.Background(), r.ID)
	if err != nil || again.State != StateCompleted {
		t.Fatalf("idempotent finish: state=%s err=%v", again.State, err)
	}

	// Restart replay reconstructs the completed terminal state.
	reopened, err := journal.NewStore(h.store.Root())
	if err != nil {
		t.Fatal(err)
	}
	freshCircuits := circuit.NewRegistry(circuit.Circuit{ID: "C-A", ValveID: "VALVE-A", Enabled: true})
	freshManager := circuit.NewManager("C-A")
	freshTransport := &countingTransport{receipt: make(map[uuid.UUID]model.ValveReceipt)}
	controller, _ := valve.NewController(freshTransport, time.Now)
	routes := valve.NewRouteService(valve.NewCommandService(controller, time.Now))
	restored, err := NewService(h.credentials, vessel.NewRegistry(), freshCircuits, freshManager, routes, reopened, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.Restore(); err != nil {
		t.Fatal(err)
	}
	got, ok := restored.Get(r.ID)
	if !ok || got.State != StateCompleted {
		t.Fatalf("restored state=%s found=%v", got.State, ok)
	}
}

func TestReviewerCanReject(t *testing.T) {
	h := newHarness(t)
	r, err := h.service.Submit(SubmitRequest{VesselID: "V-03", PlannedLiters: 10, ValidFor: time.Minute, OriginatorID: "duty-a", OriginatorSecret: "secret-a"})
	if err != nil {
		t.Fatal(err)
	}
	rejected, err := h.service.Reject(r.ID, EndorseRequest{ReviewerID: "duty-b", ReviewerSecret: "secret-b"}, "gauge looks suspect")
	if err != nil {
		t.Fatal(err)
	}
	if rejected.State != StateRejected {
		t.Fatalf("state=%s", rejected.State)
	}
	if _, _, err := h.service.Consume(context.Background(), r.ID); !errors.Is(err, ErrNotEndorsed) {
		t.Fatalf("rejected consume err=%v", err)
	}
}
