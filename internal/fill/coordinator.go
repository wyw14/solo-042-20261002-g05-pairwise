package fill

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/circuit"
	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/valve"
	"github.com/wyw14/cry-98/internal/vessel"
)

var ErrActiveSession = errors.New("vessel already has an active fill session")

type Coordinator struct {
	mu          sync.RWMutex
	sessions    map[uuid.UUID]model.FillSession
	active      map[string]uuid.UUID
	circuits    *circuit.Manager
	circuitList *circuit.Registry
	routes      *valve.RouteService
	retry       *RetryService
	vessels     *vessel.StateService
	now         func() time.Time
}

func (c *Coordinator) SetRetryService(retry *RetryService) {
	c.mu.Lock()
	c.retry = retry
	c.mu.Unlock()
}

func NewCoordinator(circuits *circuit.Manager, circuitList *circuit.Registry, routes *valve.RouteService, vessels *vessel.StateService, now func() time.Time) *Coordinator {
	if now == nil {
		now = time.Now
	}
	return &Coordinator{
		sessions:    make(map[uuid.UUID]model.FillSession),
		active:      make(map[string]uuid.UUID),
		circuits:    circuits,
		circuitList: circuitList,
		routes:      routes,
		vessels:     vessels,
		now:         now,
	}
}

func (c *Coordinator) Start(ctx context.Context, vesselID string, origin model.FillOrigin, liters float64) (model.FillSession, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.active[vesselID]; exists {
		return model.FillSession{}, ErrActiveSession
	}
	selected, err := c.selectCircuit()
	if err != nil {
		return model.FillSession{}, err
	}
	session, err := model.NewFillSession(vesselID, origin, 1, liters, c.now())
	if err != nil {
		return model.FillSession{}, err
	}
	reservation, err := c.circuits.Reserve(selected.ID, vesselID, session.ID, c.now())
	if err != nil {
		return model.FillSession{}, err
	}
	session = session.WithCircuit(selected.ID, c.now())
	session, err = session.Transition(model.FillPrechecking, c.now())
	if err != nil {
		_ = c.circuits.Release(selected.ID, session.ID)
		return model.FillSession{}, err
	}
	if _, err := c.vessels.BeginFill(vesselID); err != nil {
		_ = c.circuits.Release(selected.ID, session.ID)
		return model.FillSession{}, err
	}
	route := valve.Route{
		SessionID:         session.ID,
		CircuitID:         selected.ID,
		CircuitGeneration: reservation.Generation,
		ValveID:           selected.ValveID,
		OpenCommandID:     uuid.New(),
		CloseCommandID:    uuid.New(),
	}
	var openErr error
	if c.retry != nil {
		_, openErr = c.retry.Open(ctx, session.ID, reservation.Generation, selected.ValveID)
		if openErr == nil {
			openErr = c.routes.TrackOpen(route)
		}
	} else {
		openErr = c.routes.Open(ctx, route)
	}
	if openErr != nil {
		_, _ = c.vessels.FinishFill(vesselID)
		_ = c.circuits.Release(selected.ID, session.ID)
		return model.FillSession{}, openErr
	}
	session, err = session.Transition(model.FillFilling, c.now())
	if err != nil {
		return model.FillSession{}, err
	}
	c.sessions[session.ID] = session
	c.active[vesselID] = session.ID
	return session, nil
}

func (c *Coordinator) StartAutomatic(ctx context.Context, vesselID string, liters float64) (model.FillSession, error) {
	return c.Start(ctx, vesselID, model.FillAutomatic, liters)
}

func (c *Coordinator) StartManual(ctx context.Context, vesselID string, liters float64) (model.FillSession, error) {
	return c.Start(ctx, vesselID, model.FillManual, liters)
}

// StartWithPlan starts a manual fill using identities a two-person release
// pre-authorised. It refuses if the selected circuit is not the one bound to
// the release or its reservation generation differs from the sign-off view.
func (c *Coordinator) StartWithPlan(ctx context.Context, plan model.ReleaseFillPlan) (model.FillSession, error) {
	if err := plan.Validate(); err != nil {
		return model.FillSession{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.active[plan.VesselID]; exists {
		return model.FillSession{}, ErrActiveSession
	}
	selected, ok := c.circuitList.Get(plan.CircuitID)
	if !ok {
		return model.FillSession{}, errors.New("release circuit is not configured")
	}
	if !selected.Enabled {
		return model.FillSession{}, errors.New("release circuit is disabled")
	}
	if selected.ValveID != plan.ExpectedValveID {
		return model.FillSession{}, errors.New("circuit valve changed since the release was confirmed")
	}
	session, err := model.NewFillSessionWithID(plan.FillSessionID, plan.VesselID, model.FillManual, 1, plan.PlannedLiters, c.now())
	if err != nil {
		return model.FillSession{}, err
	}
	reservation, err := c.circuits.Reserve(selected.ID, plan.VesselID, session.ID, c.now())
	if err != nil {
		return model.FillSession{}, err
	}
	if reservation.Generation != plan.ExpectedGeneration {
		_ = c.circuits.Release(selected.ID, session.ID)
		return model.FillSession{}, errors.New("circuit state changed since the release was confirmed")
	}
	session = session.WithCircuit(selected.ID, c.now())
	session, err = session.Transition(model.FillPrechecking, c.now())
	if err != nil {
		_ = c.circuits.Release(selected.ID, session.ID)
		return model.FillSession{}, err
	}
	if _, err := c.vessels.BeginFill(plan.VesselID); err != nil {
		_ = c.circuits.Release(selected.ID, session.ID)
		return model.FillSession{}, err
	}
	route := valve.Route{
		SessionID:         session.ID,
		CircuitID:         selected.ID,
		CircuitGeneration: reservation.Generation,
		ValveID:           selected.ValveID,
		OpenCommandID:     plan.OpenCommandID,
		CloseCommandID:    plan.CloseCommandID,
	}
	var openErr error
	if c.retry != nil {
		_, openErr = c.retry.OpenCommand(ctx, plan.OpenCommandID, session.ID, reservation.Generation, selected.ValveID)
		if openErr == nil {
			openErr = c.routes.TrackOpen(route)
		}
	} else {
		openErr = c.routes.Open(ctx, route)
	}
	if openErr != nil {
		_, _ = c.vessels.FinishFill(plan.VesselID)
		_ = c.circuits.Release(selected.ID, session.ID)
		return model.FillSession{}, openErr
	}
	session, err = session.Transition(model.FillFilling, c.now())
	if err != nil {
		return model.FillSession{}, err
	}
	c.sessions[session.ID] = session
	c.active[plan.VesselID] = session.ID
	return session, nil
}

func (c *Coordinator) BeginSoak(sessionID uuid.UUID) (model.FillSession, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	session, ok := c.sessions[sessionID]
	if !ok {
		return model.FillSession{}, errors.New("fill session not found")
	}
	next, err := session.Transition(model.FillSoaking, c.now())
	if err != nil {
		return model.FillSession{}, err
	}
	c.sessions[sessionID] = next
	return next, nil
}

func (c *Coordinator) Get(id uuid.UUID) (model.FillSession, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	session, ok := c.sessions[id]
	return session, ok
}

func (c *Coordinator) List() []model.FillSession {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make([]model.FillSession, 0, len(c.sessions))
	for _, session := range c.sessions {
		result = append(result, session)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt.Before(result[j].CreatedAt) })
	return result
}

func (c *Coordinator) selectCircuit() (circuit.Circuit, error) {
	for _, candidate := range c.circuitList.List() {
		if !candidate.Enabled {
			continue
		}
		if _, occupied := c.circuits.Get(candidate.ID); !occupied {
			return candidate, nil
		}
	}
	return circuit.Circuit{}, circuit.ErrCircuitBusy
}

func (c *Coordinator) finishState(session model.FillSession) {
	c.sessions[session.ID] = session
	delete(c.active, session.VesselID)
}
