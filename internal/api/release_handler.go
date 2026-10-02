package api

import (
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/circuit"
	"github.com/wyw14/cry-98/internal/release"
)

type submitReleaseRequest struct {
	VesselID         string  `json:"vessel_id"`
	PlannedLiters    float64 `json:"planned_liters"`
	ValidSeconds     int64   `json:"valid_seconds"`
	OriginatorID     string  `json:"originator_id"`
	OriginatorSecret string  `json:"originator_secret"`
}

type reviewerRequest struct {
	ReviewerID     string `json:"reviewer_id"`
	ReviewerSecret string `json:"reviewer_secret"`
	Note           string `json:"note,omitempty"`
	Reason         string `json:"reason,omitempty"`
}

func (s *Server) listReleases(writer http.ResponseWriter, request *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]any{"items": s.deps.Releases.List()})
}

func (s *Server) getRelease(writer http.ResponseWriter, request *http.Request) {
	id, ok := parseReleaseID(writer, request)
	if !ok {
		return
	}
	r, found := s.deps.Releases.Get(id)
	if !found {
		writeError(writer, http.StatusNotFound, "release not found")
		return
	}
	writeJSON(writer, http.StatusOK, r)
}

func (s *Server) submitRelease(writer http.ResponseWriter, request *http.Request) {
	var input submitReleaseRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	if input.ValidSeconds <= 0 {
		writeError(writer, http.StatusBadRequest, "valid_seconds must be positive")
		return
	}
	r, err := s.deps.Releases.Submit(release.SubmitRequest{
		VesselID:         input.VesselID,
		PlannedLiters:    input.PlannedLiters,
		ValidFor:         time.Duration(input.ValidSeconds) * time.Second,
		OriginatorID:     input.OriginatorID,
		OriginatorSecret: input.OriginatorSecret,
	})
	if err != nil {
		writeReleaseError(writer, err)
		return
	}
	writeJSON(writer, http.StatusCreated, r)
}

func (s *Server) inspectRelease(writer http.ResponseWriter, request *http.Request) {
	id, ok := parseReleaseID(writer, request)
	if !ok {
		return
	}
	var input reviewerRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	r, observation, err := s.deps.Releases.Observe(id, input.ReviewerID, input.ReviewerSecret)
	if err != nil {
		writeReleaseError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"release": r, "observation": observation})
}

func (s *Server) endorseRelease(writer http.ResponseWriter, request *http.Request) {
	id, ok := parseReleaseID(writer, request)
	if !ok {
		return
	}
	var input reviewerRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	r, err := s.deps.Releases.Endorse(id, release.EndorseRequest{
		ReviewerID:     input.ReviewerID,
		ReviewerSecret: input.ReviewerSecret,
		Note:           input.Note,
	})
	if err != nil {
		writeReleaseError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, r)
}

func (s *Server) rejectRelease(writer http.ResponseWriter, request *http.Request) {
	id, ok := parseReleaseID(writer, request)
	if !ok {
		return
	}
	var input reviewerRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	r, err := s.deps.Releases.Reject(id, release.EndorseRequest{
		ReviewerID:     input.ReviewerID,
		ReviewerSecret: input.ReviewerSecret,
	}, input.Reason)
	if err != nil {
		writeReleaseError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, r)
}

func (s *Server) consumeRelease(writer http.ResponseWriter, request *http.Request) {
	id, ok := parseReleaseID(writer, request)
	if !ok {
		return
	}
	r, consumption, err := s.deps.Releases.Consume(request.Context(), id)
	if err != nil {
		writeReleaseError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"release": r, "consumption": consumption})
}

func (s *Server) finishRelease(writer http.ResponseWriter, request *http.Request) {
	id, ok := parseReleaseID(writer, request)
	if !ok {
		return
	}
	r, err := s.deps.Releases.Finish(request.Context(), id)
	if err != nil {
		writeReleaseError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, r)
}

func parseReleaseID(writer http.ResponseWriter, request *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(chi.URLParam(request, "id"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid release id")
		return uuid.Nil, false
	}
	return id, true
}

func writeReleaseError(writer http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, release.ErrInvalidCredential):
		writeError(writer, http.StatusUnauthorized, err.Error())
	case errors.Is(err, release.ErrAlreadyConsumed),
		errors.Is(err, release.ErrNotEndorsed),
		errors.Is(err, release.ErrSameOperator),
		errors.Is(err, release.ErrParameterMismatch),
		errors.Is(err, circuit.ErrCircuitBusy):
		writeError(writer, http.StatusConflict, err.Error())
	case errors.Is(err, release.ErrExpired):
		writeError(writer, http.StatusGone, err.Error())
	default:
		if err.Error() == "release not found" {
			writeError(writer, http.StatusNotFound, err.Error())
			return
		}
		writeError(writer, http.StatusBadRequest, err.Error())
	}
}
