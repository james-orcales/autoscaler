package main

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/james-orcales/golang_snacks/invariant"
	"github.com/james-orcales/golang_snacks/itlog"
)

var (
	next_id atomic.Int64

	pending_tasks = NewInt64Queue()

	all_tasks    = make(map[int64]*Task)
	all_tasks_mu = sync.RWMutex{}
)

type Task struct {
	ID     int64
	Status string
	Input  string
	Output []string
}

const (
	STATUS_PENDING    = "PENDING"
	STATUS_PROCESSING = "PROCESSING"
	STATUS_FINISHED   = "FINISHED"
)

func main() {
	lgr := itlog.New(os.Stdout, itlog.LevelInfo)
	if os.Getenv("SILENCE_LOGS") == "true" {
		lgr.Level = itlog.LevelDisabled
	}
	lgr.Info().Msg("backend initialized")

	t := time.NewTicker(time.Second * 3)
	go func() {
		for range t.C {
			lgr.Info().Int("pending_count", pending_tasks.Len()).Msg("checking pending tasks count")
		}
	}()

	// === ONLY FOR AUTOSCALER ===
	http.HandleFunc("GET /__SUPER_DUPER_SECRET_PENDING_COUNT__", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(strconv.Itoa(len(pending_tasks.data))))
	})

	http.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Health check passed!"))
	})

	// === Client ===
	http.HandleFunc("POST /submit", func(w http.ResponseWriter, r *http.Request) {
		type Response struct {
			ID    int64  `json:"id"`
			Error string `json:"error"`
		}
		write := func(resp *Response, code int) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(code)
			json.NewEncoder(w).Encode(resp)
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			return
		}
		type Payload struct {
			Data string `json:"data"`
		}
		payload := &Payload{}
		if err := json.Unmarshal(body, &payload); err != nil {
			write(&Response{ID: -1, Error: "Malformed_JSON"}, http.StatusBadRequest)
			return
		}

		task := Task{
			ID:     next_id.Load(),
			Input:  payload.Data,
			Status: STATUS_PENDING,
		}
		next_id.Add(1)
		all_tasks_mu.Lock()
		all_tasks[task.ID] = &task
		all_tasks_mu.Unlock()
		pending_tasks.Enqueue(task.ID)
		write(&Response{ID: task.ID, Error: ""}, http.StatusOK)
	})

	// === Client ===
	http.HandleFunc("GET /status/{id}", func(w http.ResponseWriter, r *http.Request) {
		type Response struct {
			ID     int64    `json:"id"`
			Status string   `json:"status"`
			Input  string   `json:"input"`
			Output []string `json:"output"`
			Error  string   `json:"error"`
		}
		write := func(resp *Response, code int) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(code)
			json.NewEncoder(w).Encode(resp)
		}

		id, err := strconv.Atoi(r.PathValue("id"))
		if err != nil || id < 0 {
			write(&Response{ID: -1, Status: "", Error: "Malformed_ID"}, http.StatusBadRequest)
			return
		}

		all_tasks_mu.RLock()
		defer all_tasks_mu.RUnlock()
		task, ok := all_tasks[int64(id)]
		if !ok {
			write(&Response{ID: -1, Status: "", Error: "Unknown_Task"}, http.StatusBadRequest)
			return
		}
		if task.Status == STATUS_FINISHED {
			write(&Response{ID: task.ID, Output: task.Output, Status: task.Status}, http.StatusOK)
		} else {
			write(&Response{ID: task.ID, Status: task.Status}, http.StatusOK)
		}
	})

	// === Worker ===
	http.HandleFunc("GET /pending", func(w http.ResponseWriter, r *http.Request) {
		type Response struct {
			ID    int64  `json:"id"`
			Input string `json:"input"`
		}
		write := func(resp *Response, code int) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(code)
			json.NewEncoder(w).Encode(resp)
		}

		id := pending_tasks.Dequeue()
		all_tasks_mu.Lock()
		{
			all_tasks[id].Status = STATUS_PROCESSING
			task := all_tasks[id]
			write(&Response{ID: task.ID, Input: task.Input}, http.StatusOK)
		}
		all_tasks_mu.Unlock()
	})

	// === Worker ===
	http.HandleFunc("POST /processed", func(w http.ResponseWriter, r *http.Request) {
		type Response struct {
			ID    int64  `json:"id"`
			Error string `json:"error"`
		}
		write := func(resp *Response, code int) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(code)
			json.NewEncoder(w).Encode(resp)
		}
		type Payload struct {
			ID     int64    `json:"id"`
			Input  string   `json:"input"`
			Output []string `json:"output"`
		}

		// === Implementation ===

		body, err := io.ReadAll(r.Body)
		if err != nil {
			return
		}
		payload := &Payload{}
		if err := json.Unmarshal(body, &payload); err != nil {
			invariant.Unreachable("Server generates valid JSON response")
		}

		all_tasks_mu.Lock()
		{
			task, exists := all_tasks[payload.ID] // this line
			invariant.Always(exists, "Worker processes an existing task")
			invariant.Always(task.Input == payload.Input, "Worker's submitted output has expected input")
			task.Output = payload.Output
			task.Status = STATUS_FINISHED
			write(&Response{ID: task.ID}, http.StatusOK)
		}
		all_tasks_mu.Unlock()
	})
	http.ListenAndServe(":8080", nil)
}

type Int64Queue struct {
	data        []int64
	mu          sync.Mutex
	has_pending *sync.Cond
}

func NewInt64Queue() *Int64Queue {
	queue := &Int64Queue{}
	queue.has_pending = sync.NewCond(&queue.mu)
	return queue
}

func (queue *Int64Queue) Len() int {
	queue.mu.Lock()
	n := len(queue.data)
	queue.mu.Unlock()
	return n
}

func (queue *Int64Queue) Enqueue(v int64) {
	queue.mu.Lock()
	queue.data = append(queue.data, v)
	queue.has_pending.Signal()
	queue.mu.Unlock()
}

func (queue *Int64Queue) Dequeue() int64 {
	queue.mu.Lock()
	for len(queue.data) == 0 {
		queue.has_pending.Wait()
	}
	v := queue.data[0]
	queue.data = queue.data[1:]
	queue.mu.Unlock()
	return v
}
