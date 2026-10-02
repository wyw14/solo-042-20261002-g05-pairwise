package api

import (
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/release"
)

const (
	operatorIDHeader    = "X-Operator-ID"
	operatorTokenHeader = "X-Operator-Token"
)

type proposeReleaseRequest struct {
	VesselID     string     `json:"vessel_id"`
	Liters       float64    `json:"liters"`
	ExpiresAt    *time.Time `json:"expires_at"`
	ValidSeconds int        `json:"valid_seconds"`
}

func operatorCredentials(request *http.Request) (string, string) {
	return request.Header.Get(operatorIDHeader), request.Header.Get(operatorTokenHeader)
}

func (s *Server) listReleases(writer http.ResponseWriter, request *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]any{"items": s.deps.Releases.List()})
}

func (s *Server) getRelease(writer http.ResponseWriter, request *http.Request) {
	id, err := uuid.Parse(chi.URLParam(request, "id"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid release id")
		return
	}
	record, ok := s.deps.Releases.Get(id)
	if !ok {
		writeError(writer, http.StatusNotFound, release.ErrNotFound.Error())
		return
	}
	writeJSON(writer, http.StatusOK, record)
}

func (s *Server) proposeRelease(writer http.ResponseWriter, request *http.Request) {
	operatorID, token := operatorCredentials(request)
	var input proposeReleaseRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	expiresAt, err := resolveExpiry(input, s.deps.Now())
	if err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	record, err := s.deps.Releases.Propose(input.VesselID, operatorID, token, input.Liters, expiresAt)
	if err != nil {
		writeReleaseError(writer, err)
		return
	}
	writeJSON(writer, http.StatusCreated, record)
}

func (s *Server) reviewRelease(writer http.ResponseWriter, request *http.Request) {
	operatorID, token := operatorCredentials(request)
	id, err := uuid.Parse(chi.URLParam(request, "id"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid release id")
		return
	}
	bundle, err := s.deps.Releases.Review(id, operatorID, token)
	if err != nil {
		writeReleaseError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, bundle)
}

func (s *Server) confirmRelease(writer http.ResponseWriter, request *http.Request) {
	operatorID, token := operatorCredentials(request)
	id, err := uuid.Parse(chi.URLParam(request, "id"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid release id")
		return
	}
	record, err := s.deps.Releases.Confirm(id, operatorID, token)
	if err != nil {
		writeReleaseError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, record)
}

func (s *Server) consumeRelease(writer http.ResponseWriter, request *http.Request) {
	operatorID, token := operatorCredentials(request)
	id, err := uuid.Parse(chi.URLParam(request, "id"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid release id")
		return
	}
	record, err := s.deps.Releases.Consume(request.Context(), id, operatorID, token)
	if err != nil {
		writeReleaseError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, record)
}

func resolveExpiry(input proposeReleaseRequest, now time.Time) (time.Time, error) {
	if input.ExpiresAt != nil {
		if !input.ExpiresAt.After(now) {
			return time.Time{}, errors.New("expires_at must be in the future")
		}
		return input.ExpiresAt.UTC(), nil
	}
	if input.ValidSeconds <= 0 {
		return time.Time{}, errors.New("valid_seconds must be positive or expires_at must be set")
	}
	return now.Add(time.Duration(input.ValidSeconds) * time.Second), nil
}

func writeReleaseError(writer http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, release.ErrInvalidCredential):
		writeError(writer, http.StatusUnauthorized, err.Error())
	case errors.Is(err, release.ErrNotFound):
		writeError(writer, http.StatusNotFound, err.Error())
	case errors.Is(err, release.ErrSameOperator):
		writeError(writer, http.StatusForbidden, err.Error())
	case errors.Is(err, release.ErrReleaseSpent),
		errors.Is(err, release.ErrNotConfirmed),
		errors.Is(err, release.ErrReleaseExpired),
		errors.Is(err, release.ErrReadingChanged),
		errors.Is(err, release.ErrCircuitChanged),
		errors.Is(err, release.ErrConsumptionFlight),
		errors.Is(err, release.ErrNoCircuit):
		writeError(writer, http.StatusConflict, err.Error())
	default:
		writeError(writer, http.StatusBadRequest, err.Error())
	}
}
