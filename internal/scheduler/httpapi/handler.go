package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/muthuku37/distributed-scheduler/internal/scheduler/coordinator"
	"github.com/muthuku37/distributed-scheduler/internal/scheduler/domain"
)

const maximumRequestBodySize = 1 << 20

type jobSubmitter interface {
	SubmitJob(ctx context.Context, task string, payload map[string]any, priority int32) (domain.Job, error)
}

type scheduleRequest struct {
	Task     string         `json:"task"`
	Payload  map[string]any `json:"payload"`
	Priority int32          `json:"priority"`
}

type scheduleResponse struct {
	JobID     domain.JobID     `json:"job_id"`
	Task      string           `json:"task"`
	Payload   map[string]any   `json:"payload"`
	Priority  int32            `json:"priority"`
	Status    domain.JobStatus `json:"status"`
	CreatedAt time.Time        `json:"created_at"`
}

type Handler struct {
	coordinator jobSubmitter
}

func NewHandler(coordinator jobSubmitter) http.Handler {
	handler := &Handler{coordinator: coordinator}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /schedule", handler.schedule)

	return mux
}

func (h *Handler) schedule(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maximumRequestBodySize)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	var request scheduleRequest
	if err := decoder.Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "request body must be valid JSON")
		return
	}

	if err := ensureSingleJSONValue(decoder); err != nil {
		writeError(w, http.StatusBadRequest, "request body must contain one JSON object")
		return
	}

	job, err := h.coordinator.SubmitJob(r.Context(), request.Task, request.Payload, request.Priority)
	if err != nil {
		if errors.Is(err, coordinator.ErrTaskRequired) || errors.Is(err, coordinator.ErrUnsupportedTask) {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		writeError(w, http.StatusInternalServerError, "could not schedule job")
		return
	}

	writeJSON(w, http.StatusAccepted, scheduleResponse{
		JobID:     job.ID,
		Task:      string(job.Task),
		Payload:   job.Payload,
		Priority:  job.Priority,
		Status:    job.Status,
		CreatedAt: job.CreatedAt,
	})
}

func ensureSingleJSONValue(decoder *json.Decoder) error {
	var extra any
	err := decoder.Decode(&extra)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err == nil {
		return errors.New("multiple JSON values")
	}

	return err
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
