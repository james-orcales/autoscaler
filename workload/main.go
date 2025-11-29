package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/james-orcales/golang_snacks/invariant"
)

func get_env_int(key string) int {
	v := os.Getenv(key)
	if v == "" {
		panic(key + " is unset")
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		panic(err)
	}
	return i
}

var (
	REQUESTS_PER_SECOND           = get_env_int("REQUESTS_PER_SECOND")
	MAX_TASKS_CREATED             = get_env_int("MAX_TASKS_CREATED")
	MAX_STRING_LENGTH             = get_env_int("MAX_STRING_LENGTH")
	MAX_FETCH_STATUS_RETRY_SECOND = get_env_int("MAX_FETCH_STATUS_RETRY_SECOND")
)

func main() {
	runtime.GOMAXPROCS(1)
	t := time.NewTicker(time.Second / time.Duration(REQUESTS_PER_SECOND))

	println("begin pushing tasks")
	type Task struct {
		ID    int64  `json:"id"`
		Input string `json:"input"`
	}
	tasks := make([]*Task, 0, MAX_TASKS_CREATED)
	tasks_mu := sync.Mutex{}
	{
		var wg sync.WaitGroup
		for range t.C {
			tasks_mu.Lock()
			if len(tasks) >= MAX_TASKS_CREATED {
				tasks_mu.Unlock()
				break
			}
			tasks_mu.Unlock()

			wg.Add(1)
			go func() {
				defer wg.Done()
				type Payload struct {
					Data string `json:"data"`
				}
				type Response struct {
					ID    int64  `json:"id"`
					Error string `json:"error"`
				}
				// === Implementation ===
				response_payload := &Response{}
				{
					request_payload := Payload{Data: rand_string()}
					b, _ := json.Marshal(request_payload)
					resp, err := http.Post("http://backend:8080/submit", "application/json", bytes.NewBuffer(b))
					if err != nil {
						return
					}
					body, err := io.ReadAll(resp.Body)
					if err != nil {
						panic(err)
					}
					if err := json.Unmarshal(body, response_payload); err != nil {
						panic(err)
					}
					resp.Body.Close()
				}
				if response_payload.ID < 0 || response_payload.Error != "" {
					panic(fmt.Errorf(response_payload.Error))
				}
				tasks_mu.Lock()
				tasks = append(tasks, &Task{ID: response_payload.ID})
				tasks_mu.Unlock()
			}()
		}
		wg.Wait()
	}

	invariant.Always(len(tasks) == MAX_TASKS_CREATED, "Created maximum number of tasks")

	println("done  pushing tasks")
	println("begin fetching results")

	type Payload struct {
		ID     int64    `json:"id"`
		Status string   `json:"status"`
		Input  string   `json:"input"`
		Output []string `json:"output"`
		Error  string   `json:"error"`
	}
	var wg sync.WaitGroup
	fetched_count := atomic.Int64{}
	defer func() {
		invariant.Always(fetched_count.Load() <= int64(len(tasks)), "Cannot have more tasks than you started with")
		invariant.Always(fetched_count.Load() == int64(len(tasks)), "All tasks were processed successfully")
	}()
	for _, task := range tasks {
		task := task
		invariant.Always(task.ID >= 0, "Tasks started have a non-positive ID")
		invariant.Always(task.ID >= 0, "Tasks started have a non-empty input string")
		wg.Add(1)
		go func() {
			defer wg.Done()
			for backoff := time.Duration(0); backoff <= time.Second*time.Duration(MAX_FETCH_STATUS_RETRY_SECOND); backoff *= 2 {
				if backoff == 0 {
					backoff++
				} else {
					time.Sleep(backoff)
				}
				resp, err := http.Get(fmt.Sprintf("http://backend:8080/status/%d", task.ID))
				if err != nil {
					continue
				}
				body, err := io.ReadAll(resp.Body)
				if err != nil {
					continue
				}
				payload := &Payload{}
				if err := json.Unmarshal(body, payload); err != nil {
					invariant.Unreachable("Backend sent a valid payload")
				}
				invariant.Always(payload.ID == task.ID, "Backend replied with corresponding task")
				invariant.Always(payload.Input == task.Input, "Corresponding task has matching input")
				fmt.Printf("task #%d: %s\n", task.ID, payload.Status)
				if payload.Status != "FINISHED" {
					continue
				}
				fetched_count.Add(1)
				resp.Body.Close()
				break
			}
		}()
	}
	wg.Wait()
}

var pool = sync.Pool{New: func() interface{} { return make([]rune, MAX_STRING_LENGTH) }}

func rand_string() string {
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789\n\t <[({>])}@'=-+!\":<^;$~?/,`")
	n := rand.Intn(MAX_STRING_LENGTH) + 1
	b := pool.Get().([]rune)
	for i := 0; i < n; i++ {
		b[i] = letters[rand.Intn(len(letters))]
	}
	s := string(b[:n])
	pool.Put(b)
	return s
}
