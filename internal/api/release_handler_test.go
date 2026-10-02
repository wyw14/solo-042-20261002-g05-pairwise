package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/circuit"
	"github.com/wyw14/cry-98/internal/journal"
	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/operator"
	"github.com/wyw14/cry-98/internal/release"
	"github.com/wyw14/cry-98/internal/vessel"
)

type recordingStarter struct {
	calls int
}

func (r *recordingStarter) StartWithPlan(ctx context.Context, plan model.ReleaseFillPlan) (model.FillSession, error) {
	r.calls++
	return model.FillSession{ID: plan.FillSessionID, State: model.FillFilling}, nil
}

func newReleaseServer(t *testing.T) (*Server, *recordingStarter, func() time.Time) {
	t.Helper()
	now := time.Unix(1700000000, 0).UTC()
	clock := func() time.Time { return now }
	store, err := journal.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	vessels := vessel.NewRegistry()
	seed, err := model.NewVessel("V-01", "Vessel one", "north", now)
	if err != nil {
		t.Fatal(err)
	}
	seed = seed.WithTelemetry(100, 500, 77, now)
	if err := vessels.Register(seed); err != nil {
		t.Fatal(err)
	}
	circuits := circuit.NewRegistry(circuit.Circuit{ID: "C-A", Zone: "north", ValveID: "VALVE-A", Enabled: true})
	manager := circuit.NewManager("C-A")
	ops, err := operator.NewDirectory(
		operator.Credential{ID: "duty-a", Token: "token-a"},
		operator.Credential{ID: "duty-b", Token: "token-b"},
	)
	if err != nil {
		t.Fatal(err)
	}
	starter := &recordingStarter{}
	service, err := release.NewService(store, ops, vessels, circuits, manager, starter, clock)
	if err != nil {
		t.Fatal(err)
	}
	server := NewServer(Dependencies{Releases: service, Now: clock})
	return server, starter, clock
}

func doRequest(t *testing.T, server *Server, method, path string, headers map[string]string, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	return recorder
}

func parseReleaseID(t *testing.T, recorder *httptest.ResponseRecorder) uuid.UUID {
	t.Helper()
	var record struct {
		ID uuid.UUID `json:"id"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	return record.ID
}

func TestReleaseFlowRejectsSameOperatorAndDuplicateExecution(t *testing.T) {
	server, starter, _ := newReleaseServer(t)
	authA := map[string]string{"X-Operator-ID": "duty-a", "X-Operator-Token": "token-a"}
	authB := map[string]string{"X-Operator-ID": "duty-b", "X-Operator-Token": "token-b"}

	// Proposing without credentials is unauthorized.
	if rec := doRequest(t, server, "POST", "/api/releases", nil, `{"vessel_id":"V-01","liters":12.5,"valid_seconds":600}`); rec.Code != http.StatusUnauthorized {
		t.Fatalf("propose without creds: status=%d", rec.Code)
	}

	// Initiator proposes.
	rec := doRequest(t, server, "POST", "/api/releases", authA, `{"vessel_id":"V-01","liters":12.5,"valid_seconds":600}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("propose: status=%d body=%s", rec.Code, rec.Body.String())
	}
	id := parseReleaseID(t, rec)

	// The initiator cannot review or confirm their own release.
	if rec := doRequest(t, server, "GET", "/api/releases/"+id.String()+"/review", authA, ""); rec.Code != http.StatusForbidden {
		t.Fatalf("self review: status=%d body=%s", rec.Code, rec.Body.String())
	}
	if rec := doRequest(t, server, "POST", "/api/releases/"+id.String()+"/confirm", authA, ""); rec.Code != http.StatusForbidden {
		t.Fatalf("self confirm: status=%d", rec.Code)
	}

	// The second operator reviews live state, then confirms.
	if rec := doRequest(t, server, "GET", "/api/releases/"+id.String()+"/review", authB, ""); rec.Code != http.StatusOK {
		t.Fatalf("review: status=%d body=%s", rec.Code, rec.Body.String())
	}
	if rec := doRequest(t, server, "POST", "/api/releases/"+id.String()+"/confirm", authB, ""); rec.Code != http.StatusOK {
		t.Fatalf("confirm: status=%d body=%s", rec.Code, rec.Body.String())
	}

	// Consume once: valve opens.
	if rec := doRequest(t, server, "POST", "/api/releases/"+id.String()+"/consume", authB, ""); rec.Code != http.StatusOK {
		t.Fatalf("consume: status=%d body=%s", rec.Code, rec.Body.String())
	}
	if starter.calls != 1 {
		t.Fatalf("valve calls=%d", starter.calls)
	}
	// Duplicate execution is refused.
	if rec := doRequest(t, server, "POST", "/api/releases/"+id.String()+"/consume", authB, ""); rec.Code != http.StatusConflict {
		t.Fatalf("duplicate consume: status=%d body=%s", rec.Code, rec.Body.String())
	}
	if starter.calls != 1 {
		t.Fatalf("valve calls after duplicate=%d", starter.calls)
	}
}

func TestReleaseProposeValidation(t *testing.T) {
	server, _, _ := newReleaseServer(t)
	authA := map[string]string{"X-Operator-ID": "duty-a", "X-Operator-Token": "token-a"}

	for _, body := range []string{
		`{"vessel_id":"V-01","liters":0,"valid_seconds":600}`,
		`{"vessel_id":"V-01","liters":12.5}`,
		`{"vessel_id":"missing","liters":12.5,"valid_seconds":600}`,
	} {
		if rec := doRequest(t, server, "POST", "/api/releases", authA, body); rec.Code != http.StatusBadRequest {
			t.Fatalf("body=%s status=%d want 400 (%s)", body, rec.Code, rec.Body.String())
		}
	}
}

func TestReleaseUnknownID(t *testing.T) {
	server, _, _ := newReleaseServer(t)
	authB := map[string]string{"X-Operator-ID": "duty-b", "X-Operator-Token": "token-b"}
	if rec := doRequest(t, server, "GET", "/api/releases/not-a-uuid/review", authB, ""); rec.Code != http.StatusBadRequest {
		t.Fatalf("malformed id status=%d", rec.Code)
	}
	if rec := doRequest(t, server, "GET", "/api/releases/"+uuid.NewString()+"/review", authB, ""); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown id status=%d", rec.Code)
	}
}
