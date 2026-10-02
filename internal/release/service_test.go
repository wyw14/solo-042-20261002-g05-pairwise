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
	"github.com/wyw14/cry-98/internal/operator"
	"github.com/wyw14/cry-98/internal/vessel"
)

const (
	opA    = "duty-a"
	tokenA = "token-a"
	opB    = "duty-b"
	tokenB = "token-b"
)

type mutableClock struct{ t time.Time }

func (c *mutableClock) now() time.Time          { return c.t }
func (c *mutableClock) advance(d time.Duration) { c.t = c.t.Add(d) }

type fakeFillStarter struct {
	mu       sync.Mutex
	plans    []model.ReleaseFillPlan
	failWith error
}

func (f *fakeFillStarter) StartWithPlan(ctx context.Context, plan model.ReleaseFillPlan) (model.FillSession, error) {
	if err := ctx.Err(); err != nil {
		return model.FillSession{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.plans = append(f.plans, plan)
	return model.FillSession{ID: plan.FillSessionID, VesselID: plan.VesselID, CircuitID: plan.CircuitID, PlannedLiters: plan.PlannedLiters, State: model.FillFilling}, f.failWith
}

func (f *fakeFillStarter) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.plans)
}

// reentrantStarter issues a second Consume from inside the valve call, while
// the in-flight marker is guaranteed to be held.
type reentrantStarter struct {
	svc       *Service
	innerErr  error
	innerOnce sync.Once
}

func (r *reentrantStarter) StartWithPlan(ctx context.Context, plan model.ReleaseFillPlan) (model.FillSession, error) {
	r.innerOnce.Do(func() {
		_, r.innerErr = r.svc.Consume(ctx, plan.ReleaseID, opB, tokenB)
	})
	return model.FillSession{ID: plan.FillSessionID, VesselID: plan.VesselID, State: model.FillFilling}, nil
}

type fixture struct {
	svc      *Service
	store    *journal.Store
	ops      *operator.Directory
	vessels  *vessel.Registry
	circuits *circuit.Registry
	manager  *circuit.Manager
	starter  *fakeFillStarter
	clock    *mutableClock
}

func newFixture(t *testing.T, starter *fakeFillStarter) *fixture {
	t.Helper()
	f := newFixtureWithStarter(t, starter)
	f.starter = starter
	return f
}

func newFixtureWithStarter(t *testing.T, starter FillStarter) *fixture {
	t.Helper()
	clock := &mutableClock{t: time.Unix(1700000000, 0).UTC()}
	store, err := journal.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	vessels := vessel.NewRegistry()
	seed, err := model.NewVessel("V-01", "Vessel one", "north", clock.t)
	if err != nil {
		t.Fatal(err)
	}
	seed = seed.WithTelemetry(100, 500, 77, clock.t)
	if err := vessels.Register(seed); err != nil {
		t.Fatal(err)
	}
	circuits := circuit.NewRegistry(circuit.Circuit{ID: "C-A", Zone: "north", ValveID: "VALVE-A", Enabled: true, Pressure: 120})
	manager := circuit.NewManager("C-A")
	ops, err := operator.NewDirectory(
		operator.Credential{ID: opA, DisplayName: "A", Token: tokenA},
		operator.Credential{ID: opB, DisplayName: "B", Token: tokenB},
	)
	if err != nil {
		t.Fatal(err)
	}
	svc, err := NewService(store, ops, vessels, circuits, manager, starter, clock.now)
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{svc: svc, store: store, ops: ops, vessels: vessels, circuits: circuits, manager: manager, clock: clock}
}

// rebuildService creates a fresh service over the same durable journal,
// simulating a process restart.
func rebuildService(t *testing.T, f *fixture, starter *fakeFillStarter) *Service {
	t.Helper()
	svc, err := NewService(f.store, f.ops, f.vessels, f.circuits, f.manager, starter, f.clock.now)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Recover(); err != nil {
		t.Fatal(err)
	}
	return svc
}

func mustPropose(t *testing.T, svc *Service, clock *mutableClock) model.Release {
	t.Helper()
	record, err := svc.Propose("V-01", opA, tokenA, 12.5, clock.t.Add(10*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func mustConfirm(t *testing.T, svc *Service, id uuid.UUID) model.Release {
	t.Helper()
	record, err := svc.Confirm(id, opB, tokenB)
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func TestProposeRejectsBadCredential(t *testing.T) {
	f := newFixture(t, &fakeFillStarter{})
	if _, err := f.svc.Propose("V-01", opA, "wrong", 12.5, f.clock.t.Add(time.Minute)); !errors.Is(err, ErrInvalidCredential) {
		t.Fatalf("err=%v", err)
	}
}

func TestReviewShowsReadingAndCircuitState(t *testing.T) {
	f := newFixture(t, &fakeFillStarter{})
	record := mustPropose(t, f.svc, f.clock)

	// The initiator cannot be the reviewer.
	if _, err := f.svc.Review(record.ID, opA, tokenA); !errors.Is(err, ErrSameOperator) {
		t.Fatalf("err=%v", err)
	}

	bundle, err := f.svc.Review(record.ID, opB, tokenB)
	if err != nil {
		t.Fatal(err)
	}
	if bundle.Vessel.LevelMM != 100 || bundle.Vessel.Generation != 1 {
		t.Fatalf("reading=%+v", bundle.Vessel)
	}
	if len(bundle.Circuits) != 1 || bundle.Circuits[0].CircuitID != "C-A" || bundle.Circuits[0].Generation != 1 {
		t.Fatalf("circuits=%+v", bundle.Circuits)
	}
}

func TestConfirmRequiresDifferentOperator(t *testing.T) {
	f := newFixture(t, &fakeFillStarter{})
	record := mustPropose(t, f.svc, f.clock)
	if _, err := f.svc.Confirm(record.ID, opA, tokenA); !errors.Is(err, ErrSameOperator) {
		t.Fatalf("err=%v", err)
	}
	confirmed := mustConfirm(t, f.svc, record.ID)
	if confirmed.State != model.ReleaseConfirmed || confirmed.Confirmation.ReviewerID != opB {
		t.Fatalf("confirmed=%+v", confirmed)
	}
	// Confirmation is one-way and single.
	if _, err := f.svc.Confirm(record.ID, opB, tokenB); err == nil {
		t.Fatal("second confirmation accepted")
	}
}

func TestHappyPathConsumesExactlyOnce(t *testing.T) {
	starter := &fakeFillStarter{}
	f := newFixture(t, starter)
	record := mustConfirm(t, f.svc, mustPropose(t, f.svc, f.clock).ID)

	consumed, err := f.svc.Consume(context.Background(), record.ID, opB, tokenB)
	if err != nil {
		t.Fatal(err)
	}
	if consumed.State != model.ReleaseConsumed || !consumed.Consumption.Executed ||
		consumed.Consumption.Start.FillSessionID == uuid.Nil {
		t.Fatalf("consumed=%+v", consumed)
	}
	if starter.calls() != 1 {
		t.Fatalf("starter calls=%d", starter.calls())
	}
	if _, err := f.svc.Consume(context.Background(), record.ID, opB, tokenB); !errors.Is(err, ErrReleaseSpent) {
		t.Fatalf("second consume err=%v", err)
	}
	if starter.calls() != 1 {
		t.Fatalf("starter calls after repeat=%d", starter.calls())
	}
}

func TestConsumeRequiresConfirmation(t *testing.T) {
	f := newFixture(t, &fakeFillStarter{})
	record := mustPropose(t, f.svc, f.clock)
	if _, err := f.svc.Consume(context.Background(), record.ID, opB, tokenB); !errors.Is(err, ErrNotConfirmed) {
		t.Fatalf("err=%v", err)
	}
}

func TestExpiryAtConfirmInvalidates(t *testing.T) {
	f := newFixture(t, &fakeFillStarter{})
	record, err := f.svc.Propose("V-01", opA, tokenA, 12.5, f.clock.t.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	f.clock.advance(2 * time.Minute)
	if _, err := f.svc.Confirm(record.ID, opB, tokenB); !errors.Is(err, ErrReleaseExpired) {
		t.Fatalf("err=%v", err)
	}
	got, _ := f.svc.Get(record.ID)
	if got.State != model.ReleaseInvalidated || got.Invalidation.Cause != model.InvalidationExpired {
		t.Fatalf("state=%s cause=%s", got.State, got.Invalidation.Cause)
	}
}

func TestExpiryAtConsumeInvalidates(t *testing.T) {
	f := newFixture(t, &fakeFillStarter{})
	record, err := f.svc.Propose("V-01", opA, tokenA, 12.5, f.clock.t.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	mustConfirm(t, f.svc, record.ID)
	f.clock.advance(2 * time.Minute)
	if _, err := f.svc.Consume(context.Background(), record.ID, opB, tokenB); !errors.Is(err, ErrReleaseExpired) {
		t.Fatalf("err=%v", err)
	}
	got, _ := f.svc.Get(record.ID)
	if got.State != model.ReleaseInvalidated {
		t.Fatalf("state=%s", got.State)
	}
	if f.starter.calls() != 0 {
		t.Fatal("valve was opened after expiry")
	}
}

func TestVesselReadingChangedInvalidates(t *testing.T) {
	f := newFixture(t, &fakeFillStarter{})
	record := mustConfirm(t, f.svc, mustPropose(t, f.svc, f.clock).ID)

	if _, err := f.vessels.Update("V-01", func(v model.Vessel) (model.Vessel, error) {
		return v.WithTelemetry(101, 500, 77, f.clock.t), nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.svc.Consume(context.Background(), record.ID, opB, tokenB); !errors.Is(err, ErrReadingChanged) {
		t.Fatalf("err=%v", err)
	}
	got, _ := f.svc.Get(record.ID)
	if got.State != model.ReleaseInvalidated || got.Invalidation.Cause != model.InvalidationReading {
		t.Fatalf("state=%s", got.State)
	}
	if _, err := f.svc.Consume(context.Background(), record.ID, opB, tokenB); !errors.Is(err, ErrReleaseSpent) {
		t.Fatalf("repeat err=%v", err)
	}
	if f.starter.calls() != 0 {
		t.Fatal("valve opened after reading changed")
	}
}

func TestCircuitStateChangedInvalidates(t *testing.T) {
	f := newFixture(t, &fakeFillStarter{})
	record := mustConfirm(t, f.svc, mustPropose(t, f.svc, f.clock).ID)

	// Another reservation cycle bumps the circuit generation past the value
	// captured at sign-off.
	sid := uuid.New()
	if _, err := f.manager.Reserve("C-A", "V-09", sid, f.clock.t); err != nil {
		t.Fatal(err)
	}
	if err := f.manager.Release("C-A", sid); err != nil {
		t.Fatal(err)
	}
	if _, err := f.svc.Consume(context.Background(), record.ID, opB, tokenB); !errors.Is(err, ErrCircuitChanged) {
		t.Fatalf("err=%v", err)
	}
	got, _ := f.svc.Get(record.ID)
	if got.State != model.ReleaseInvalidated || got.Invalidation.Cause != model.InvalidationCircuit {
		t.Fatalf("state=%s", got.State)
	}
	if f.starter.calls() != 0 {
		t.Fatal("valve opened after circuit changed")
	}
}

func TestDuplicateDuringExecutionIsRejected(t *testing.T) {
	starter := &reentrantStarter{}
	f := newFixtureWithStarter(t, starter)
	starter.svc = f.svc
	record := mustConfirm(t, f.svc, mustPropose(t, f.svc, f.clock).ID)

	consumed, err := f.svc.Consume(context.Background(), record.ID, opB, tokenB)
	if err != nil {
		t.Fatal(err)
	}
	if consumed.State != model.ReleaseConsumed {
		t.Fatalf("state=%s", consumed.State)
	}
	// The nested consume, issued while the valve call was in flight, had to
	// be refused rather than queued or executed.
	if !errors.Is(starter.innerErr, ErrConsumptionFlight) {
		t.Fatalf("nested consume err=%v", starter.innerErr)
	}
	if got, _ := f.svc.Get(record.ID); got.State != model.ReleaseConsumed {
		t.Fatalf("final state=%s", got.State)
	}
}

func TestConcurrentBurstOpensValveOnce(t *testing.T) {
	starter := &fakeFillStarter{}
	f := newFixture(t, starter)
	record := mustConfirm(t, f.svc, mustPropose(t, f.svc, f.clock).ID)

	const contenders = 16
	var wg sync.WaitGroup
	wg.Add(contenders)
	var successes, spent, inFlight int
	var resultMu sync.Mutex
	for range contenders {
		go func() {
			defer wg.Done()
			_, err := f.svc.Consume(context.Background(), record.ID, opB, tokenB)
			resultMu.Lock()
			defer resultMu.Unlock()
			switch {
			case err == nil:
				successes++
			case errors.Is(err, ErrReleaseSpent):
				spent++
			case errors.Is(err, ErrConsumptionFlight):
				inFlight++
			default:
				t.Errorf("unexpected consume error: %v", err)
			}
		}()
	}
	wg.Wait()

	if successes != 1 {
		t.Fatalf("successes=%d (spent=%d inFlight=%d), want exactly 1", successes, spent, inFlight)
	}
	if starter.calls() != 1 {
		t.Fatalf("valve opened %d times, want 1", starter.calls())
	}
	if got, _ := f.svc.Get(record.ID); got.State != model.ReleaseConsumed {
		t.Fatalf("final state=%s", got.State)
	}
}

func TestValveFailureFailsClosedAndIsNotRetried(t *testing.T) {
	starter := &fakeFillStarter{failWith: errors.New("valve bus offline")}
	f := newFixture(t, starter)
	record := mustConfirm(t, f.svc, mustPropose(t, f.svc, f.clock).ID)

	if _, err := f.svc.Consume(context.Background(), record.ID, opB, tokenB); err == nil {
		t.Fatal("expected valve failure")
	}
	got, _ := f.svc.Get(record.ID)
	if got.State != model.ReleaseFailed || got.Consumption.Executed || got.Consumption.Failure == "" {
		t.Fatalf("failed=%+v", got)
	}
	if _, err := f.svc.Consume(context.Background(), record.ID, opB, tokenB); !errors.Is(err, ErrReleaseSpent) {
		t.Fatalf("retry err=%v", err)
	}
	if starter.calls() != 1 {
		t.Fatalf("valve retried after failure: calls=%d", starter.calls())
	}
}

func TestRestartDoesNotReconsumeUsedRelease(t *testing.T) {
	starter1 := &fakeFillStarter{}
	f := newFixture(t, starter1)
	record := mustConfirm(t, f.svc, mustPropose(t, f.svc, f.clock).ID)
	if _, err := f.svc.Consume(context.Background(), record.ID, opB, tokenB); err != nil {
		t.Fatal(err)
	}

	starter2 := &fakeFillStarter{}
	restarted := rebuildService(t, f, starter2)
	got, ok := restarted.Get(record.ID)
	if !ok || got.State != model.ReleaseConsumed {
		t.Fatalf("recovered state=%s ok=%v", got.State, ok)
	}
	if _, err := restarted.Consume(context.Background(), record.ID, opB, tokenB); !errors.Is(err, ErrReleaseSpent) {
		t.Fatalf("post-restart consume err=%v", err)
	}
	if starter2.calls() != 0 {
		t.Fatal("used release opened a valve again after restart")
	}
}

func TestRecoveryClosesInterruptedConsumption(t *testing.T) {
	f := newFixture(t, &fakeFillStarter{})
	record := mustConfirm(t, f.svc, mustPropose(t, f.svc, f.clock).ID)

	// Simulate a crash after the intent was journaled but before the result.
	start := model.ConsumptionStart{
		FillSessionID:     uuid.New(),
		CircuitID:         "C-A",
		CircuitGeneration: 1,
		ValveID:           "VALVE-A",
		OpenCommandID:     uuid.New(),
		CloseCommandID:    uuid.New(),
		OperatorID:        opB,
		StartedAt:         f.clock.t,
	}
	intent, err := model.NewEvent(Partition, "release.consumption_started", record.ID.String(), start, f.clock.t)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.Append(intent); err != nil {
		t.Fatal(err)
	}

	starter2 := &fakeFillStarter{}
	restarted := rebuildService(t, f, starter2)
	got, ok := restarted.Get(record.ID)
	if !ok {
		t.Fatal("release lost during recovery")
	}
	if got.State != model.ReleaseConsumed ||
		!got.Consumption.Recovered ||
		!got.Consumption.Executed ||
		got.Consumption.Start.FillSessionID != start.FillSessionID {
		t.Fatalf("recovered=%+v", got)
	}
	if _, err := restarted.Consume(context.Background(), record.ID, opB, tokenB); !errors.Is(err, ErrReleaseSpent) {
		t.Fatalf("interrupted release was reusable: err=%v", err)
	}
	if starter2.calls() != 0 {
		t.Fatal("interrupted release issued another valve command")
	}
}

func TestRecoveryIsIdempotentAcrossRestarts(t *testing.T) {
	f := newFixture(t, &fakeFillStarter{})
	record := mustConfirm(t, f.svc, mustPropose(t, f.svc, f.clock).ID)
	start := model.ConsumptionStart{
		FillSessionID: uuid.New(), CircuitID: "C-A", CircuitGeneration: 1,
		ValveID: "VALVE-A", OpenCommandID: uuid.New(), CloseCommandID: uuid.New(),
		OperatorID: opB, StartedAt: f.clock.t,
	}
	intent, _ := model.NewEvent(Partition, "release.consumption_started", record.ID.String(), start, f.clock.t)
	if _, err := f.store.Append(intent); err != nil {
		t.Fatal(err)
	}

	_ = rebuildService(t, f, &fakeFillStarter{})
	second := rebuildService(t, f, &fakeFillStarter{})
	got, _ := second.Get(record.ID)
	if got.State != model.ReleaseConsumed || !got.Consumption.Recovered {
		t.Fatalf("state after second restart=%s", got.State)
	}
}

func TestRecoveryDoesNotDuplicateDenialAudit(t *testing.T) {
	starter := &reentrantStarter{}
	f := newFixtureWithStarter(t, starter)
	starter.svc = f.svc
	record := mustConfirm(t, f.svc, mustPropose(t, f.svc, f.clock).ID)

	// The in-flight duplicate emits a release.denied event; the final
	// consumed snapshot carries that same denial inline.
	if _, err := f.svc.Consume(context.Background(), record.ID, opB, tokenB); err != nil {
		t.Fatal(err)
	}
	before, _ := f.svc.Get(record.ID)
	if len(before.Audit) != 1 {
		t.Fatalf("pre-restart audit entries=%d", len(before.Audit))
	}

	restarted := rebuildService(t, f, &fakeFillStarter{})
	after, _ := restarted.Get(record.ID)
	if len(after.Audit) != 1 {
		t.Fatalf("post-restart audit entries=%d, denial was duplicated on replay", len(after.Audit))
	}
	if after.Audit[0].Reason != ErrConsumptionFlight.Error() {
		t.Fatalf("audit reason=%s", after.Audit[0].Reason)
	}
}
