package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/wyw14/cry-98/internal/circuit"
	"github.com/wyw14/cry-98/internal/journal"
	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/release"
	"github.com/wyw14/cry-98/internal/valve"
	"github.com/wyw14/cry-98/internal/vessel"
)

func newReleaseTestServer(t *testing.T) http.Handler {
	t.Helper()
	start := time.Unix(1_700_000_000, 0).UTC()
	now := func() time.Time { return start }

	store, err := journal.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	vessels := vessel.NewRegistry()
	v, err := model.NewVessel("V-03", "Vessel 03", "north", now())
	if err != nil {
		t.Fatal(err)
	}
	if err := vessels.Register(v); err != nil {
		t.Fatal(err)
	}
	circuits := circuit.NewRegistry(
		circuit.Circuit{ID: "C-A", Zone: "north", ValveID: "VALVE-A", Enabled: true},
	)
	manager := circuit.NewManager("C-A")
	controller, err := valve.NewController(valve.NewMemoryTransport(now), now)
	if err != nil {
		t.Fatal(err)
	}
	routes := valve.NewRouteService(valve.NewCommandService(controller, now))
	credentials := release.NewCredentialStore()
	if err := credentials.Register("duty-a", "secret-a"); err != nil {
		t.Fatal(err)
	}
	if err := credentials.Register("duty-b", "secret-b"); err != nil {
		t.Fatal(err)
	}
	service, err := release.NewService(credentials, vessels, circuits, manager, routes, store, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Restore(); err != nil {
		t.Fatal(err)
	}
	return NewServer(Dependencies{Releases: service, Now: now}).Handler()
}

func doRelease(t *testing.T, handler http.Handler, method, path, body string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	var decoded map[string]any
	if rec.Body.Len() > 0 {
		if err := json.Unmarshal(rec.Body.Bytes(), &decoded); err != nil {
			t.Fatalf("invalid json: %v (%s)", err, rec.Body.String())
		}
	}
	return rec.Code, decoded
}

func TestReleaseWorkflowOverHTTP(t *testing.T) {
	handler := newReleaseTestServer(t)

	submit := `{"vessel_id":"V-03","planned_liters":120,"valid_seconds":600,"originator_id":"duty-a","originator_secret":"secret-a"}`
	status, body := doRelease(t, handler, http.MethodPost, "/api/releases", submit)
	if status != http.StatusCreated {
		t.Fatalf("submit status=%d body=%v", status, body)
	}
	id := body["id"].(string)

	// Independent reviewer inspects the live readings.
	inspect := `{"reviewer_id":"duty-b","reviewer_secret":"secret-b"}`
	status, body = doRelease(t, handler, http.MethodPost, "/api/releases/"+id+"/inspect", inspect)
	if status != http.StatusOK {
		t.Fatalf("inspect status=%d body=%v", status, body)
	}
	if _, ok := body["observation"]; !ok {
		t.Fatal("inspect response missing observation")
	}

	// Wrong credential is rejected with 401.
	status, _ = doRelease(t, handler, http.MethodPost, "/api/releases/"+id+"/inspect",
		`{"reviewer_id":"duty-b","reviewer_secret":"nope"}`)
	if status != http.StatusUnauthorized {
		t.Fatalf("bad credential status=%d", status)
	}

	// Originator cannot countersign their own release.
	status, _ = doRelease(t, handler, http.MethodPost, "/api/releases/"+id+"/endorse",
		`{"reviewer_id":"duty-a","reviewer_secret":"secret-a"}`)
	if status != http.StatusConflict {
		t.Fatalf("self endorse status=%d", status)
	}

	// Distinct reviewer endorses.
	status, body = doRelease(t, handler, http.MethodPost, "/api/releases/"+id+"/endorse", inspect)
	if status != http.StatusOK || body["state"] != "endorsed" {
		t.Fatalf("endorse status=%d body=%v", status, body)
	}

	// First consume opens the valve.
	status, body = doRelease(t, handler, http.MethodPost, "/api/releases/"+id+"/consume", "")
	if status != http.StatusOK {
		t.Fatalf("consume status=%d body=%v", status, body)
	}
	if body["release"].(map[string]any)["state"] != "consumed" {
		t.Fatalf("consume body=%v", body)
	}

	// Duplicate consume is refused and must not reopen.
	status, body = doRelease(t, handler, http.MethodPost, "/api/releases/"+id+"/consume", "")
	if status != http.StatusConflict || !strings.Contains(body["error"].(string), "already been consumed") {
		t.Fatalf("duplicate consume status=%d body=%v", status, body)
	}

	// Finish tears the release down and is idempotent.
	status, body = doRelease(t, handler, http.MethodPost, "/api/releases/"+id+"/finish", "")
	if status != http.StatusOK || body["state"] != "completed" {
		t.Fatalf("finish status=%d body=%v", status, body)
	}
	status, _ = doRelease(t, handler, http.MethodPost, "/api/releases/"+id+"/finish", "")
	if status != http.StatusOK {
		t.Fatalf("idempotent finish status=%d", status)
	}
}
