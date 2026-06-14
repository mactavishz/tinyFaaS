package stats

import (
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"
)

const DefaultNamespace = "tinyfaas"

type InvocationRecord struct {
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
	DurationNS int64     `json:"duration_ns"`
	Method     string    `json:"method"`
	Path       string    `json:"path"`
	StatusCode int       `json:"status_code"`
	Success    bool      `json:"success"`
}

type InvocationSummary struct {
	SuccessfulInvocations int            `json:"successful_invocations"`
	FailedInvocations     int            `json:"failed_invocations"`
	StatusCodes           map[string]int `json:"status_codes"`
}

type FunctionStatsResponse struct {
	Function struct {
		Name      string `json:"name"`
		Namespace string `json:"namespace"`
	} `json:"function"`
	Summary     InvocationSummary  `json:"summary"`
	Invocations []InvocationRecord `json:"invocations"`
}

type functionStats struct {
	invocations []InvocationRecord
	summary     InvocationSummary
}

type Store struct {
	mu    sync.RWMutex
	stats map[string]functionStats
}

func NewStore() *Store {
	return &Store{stats: map[string]functionStats{}}
}

func (s *Store) Record(name string, record InvocationRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stats := s.stats[name]
	stats.invocations = append(stats.invocations, record)
	if stats.summary.StatusCodes == nil {
		stats.summary.StatusCodes = map[string]int{}
	}
	if record.Success {
		stats.summary.SuccessfulInvocations++
	} else {
		stats.summary.FailedInvocations++
	}
	stats.summary.StatusCodes[strconv.Itoa(record.StatusCode)]++
	s.stats[name] = stats
}

func (s *Store) Reset(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.stats, name)
}

func (s *Store) Response(name string, exists bool) (FunctionStatsResponse, bool) {
	s.mu.RLock()
	stats, ok := s.stats[name]
	s.mu.RUnlock()
	if !ok && !exists {
		return FunctionStatsResponse{}, false
	}

	if !ok {
		stats = functionStats{summary: InvocationSummary{StatusCodes: map[string]int{}}, invocations: []InvocationRecord{}}
	}

	invocations := make([]InvocationRecord, len(stats.invocations))
	copy(invocations, stats.invocations)
	sort.Slice(invocations, func(i, j int) bool {
		return invocations[i].StartedAt.Before(invocations[j].StartedAt)
	})

	resp := FunctionStatsResponse{Summary: stats.summary, Invocations: invocations}
	resp.Function.Name = name
	resp.Function.Namespace = DefaultNamespace
	if resp.Summary.StatusCodes == nil {
		resp.Summary.StatusCodes = map[string]int{}
	}
	if resp.Invocations == nil {
		resp.Invocations = []InvocationRecord{}
	}
	return resp, true
}

func WriteJSON(w http.ResponseWriter, response FunctionStatsResponse) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
}
