package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/james-orcales/golang_snacks/invariant"
	"github.com/james-orcales/golang_snacks/itlog"
)

func get_env_int64(key string) int64 {
	if invariant.IsRunningUnderGoTest {
		return 0
	}
	v := os.Getenv(key)
	if v == "" {
		panic(key + " is unset")
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		panic(err)
	}
	return int64(i)
}

var (
	MAX_COMPUTE_DELAY_MILLISECOND = get_env_int64("MAX_COMPUTE_DELAY_MILLISECOND")
	MIN_COMPUTE_DELAY_MILLISECOND = get_env_int64("MIN_COMPUTE_DELAY_MILLISECOND")
)

func main() {
	runtime.GOMAXPROCS(1)

	lgr := itlog.New(os.Stdout, itlog.LevelInfo)
	lgr.Level = itlog.LevelWarn

	var last_id int64 = -1
	for {
		type Task struct {
			ID     int64    `json:"id"`
			Input  string   `json:"input"`
			Output []string `json:"output"`
		}
		task := Task{}

		// === Fetch task ===
		{
			type Payload struct {
				ID    int64  `json:"id"`
				Input string `json:"input"`
			}
			payload := &Payload{}
			// Backend blocks when there are no available tasks
			resp, err := http.Get("http://backend:8080/pending")
			if err != nil {
				continue
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				lgr.Warn().Err(err).Msg("GET /pending: reading response body")
				continue
			}
			if err := json.Unmarshal(body, payload); err != nil {
				invariant.Unreachable("Backend sends correct GET /pending JSON response")
			}
			task = Task{
				ID:     payload.ID,
				Input:  payload.Input,
				Output: AllSubstrings(payload.Input),
			}
		}

		invariant.Always(task.ID > last_id, "Task IDs increase monotically")
		last_id = task.ID

		// === Marshal output ===
		lgr := lgr.Clone().WithInt64("id", task.ID).WithStr("input", task.Input)

		output_payload, err := json.Marshal(task)
		if err != nil {
			lgr.Error(err).Msg("json marshal task output")
			continue
		}
		lgr.WithStr("output", string(output_payload))

		// === Commit ===
		{
			type Payload struct {
				ID    int64  `json:"id"`
				Error string `json:"error"`
			}
			payload := &Payload{}
			resp, err := http.Post("http://backend:8080/processed", "application/json", bytes.NewReader(output_payload))
			if err != nil {
				lgr.Error(err).Msg("json marshal task output")
				continue
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				lgr.Error(err).Msg("read POST /processed response body")
				continue
			}
			if err := json.Unmarshal(body, payload); err != nil {
				invariant.Unreachable("Backend sends correct POST /processed JSON response")
				continue
			}
			if payload.ID < 0 || payload.Error != "" {
				lgr.Error(fmt.Errorf("%s", payload.Error)).Msg("POST /processed")
				continue
			}
		}
	}
}

func AllSubstrings(s string) []string {
	if !invariant.IsRunningUnderGoTest {
		time.Sleep(time.Millisecond * time.Duration(max(MIN_COMPUTE_DELAY_MILLISECOND, rand.Int64()%(MAX_COMPUTE_DELAY_MILLISECOND))))
	}
	if s == "" {
		invariant.Sometimes(true, "String to compute is empty")
		return []string{""}
	}
	n := len(s)
	out := make([]string, 0, n*(n+1)/2)
	for i := 0; i < n; i++ {
		for j := i + 1; j <= n; j++ {
			b := make([]byte, j-i)
			copy(b, s[i:j])
			out = append(out, string(b))
		}
	}
	return out
}
