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
	REQUESTS_PER_SECOND = get_env_int("REQUESTS_PER_SECOND")
	MAX_TASKS_CREATED   = get_env_int("MAX_TASKS_CREATED")
	MAX_STRING_LENGTH   = get_env_int("MAX_STRING_LENGTH")
)

func main() {
	runtime.GOMAXPROCS(1)

	type Task struct {
		ID    int64  `json:"id"`
		Input string `json:"input"`
	}
	var (
		tasks    = make([]*Task, 0, MAX_TASKS_CREATED)
		tasks_mu = sync.Mutex{}

		pending_count    int
		processing_count int
		finished_count   int
		count_mu         sync.RWMutex
	)

	go func() {
		time.Sleep(time.Second * 3)
		ticker := time.NewTicker(time.Second * 6)
		for range ticker.C {
			if len(tasks) < MAX_TASKS_CREATED {
			}
			count_mu.RLock()
			fmt.Printf(
				"PENDING=%d PROCESSING=%d FINISHED=%d\n",
				pending_count,
				processing_count,
				finished_count,
			)
			count_mu.RUnlock()
		}
	}()

	println("begin pushing tasks")
	t := time.NewTicker(time.Second / time.Duration(REQUESTS_PER_SECOND))
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
			var task Task
			// === Submit ===
			{
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
				task = Task{ID: response_payload.ID}
				tasks = append(tasks, &task)
				tasks_mu.Unlock()
			}
			// === Fetch status ===
			{
				type Payload struct {
					ID     int64    `json:"id"`
					Status string   `json:"status"`
					Input  string   `json:"input"`
					Output []string `json:"output"`
					Error  string   `json:"error"`
				}
				task := task
				previous_status := ""
				for backoff := time.Duration(0); true; backoff *= 2 {
					if backoff == 0 {
						backoff++
					} else {
						time.Sleep(time.Second * backoff)
					}
					resp, err := http.Get(fmt.Sprintf("http://backend:8080/status/%d", task.ID))
					if err != nil {
						continue
					}
					body, err := io.ReadAll(resp.Body)
					if err != nil {
						continue
					}
					resp.Body.Close()
					payload := &Payload{}
					if err := json.Unmarshal(body, payload); err != nil {
						invariant.Unreachable("Backend sent a valid payload")
					}
					invariant.Always(payload.ID == task.ID, "Backend replied with corresponding task")
					invariant.Always(payload.Input == task.Input, "Corresponding task has matching input")
					invariant.Always(
						payload.Status == "PENDING" || payload.Status == "PROCESSING" || payload.Status == "FINISHED",
						"Backend replied with a valid status",
					)

					if previous_status == "" {
						count_mu.Lock()
						switch payload.Status {
						default:
							panic("unreachable")
						case "PENDING":
							pending_count++
							count_mu.Unlock()
						case "PROCESSING":
							processing_count++
							count_mu.Unlock()
						case "FINISHED":
							finished_count++
							count_mu.Unlock()
							return
						}
					} else {
						switch payload.Status {
						default:
							panic("unreachable")
						case "PENDING":
							invariant.Always(previous_status == "PENDING", "Task is still pending")
							invariant.Always(pending_count > 0, "Some tasks are pending")
						case "PROCESSING":
							invariant.Always(
								previous_status == "PENDING" || previous_status == "PROCESSING",
								"Task transitioned from pending, or is still processing",
							)
							if previous_status == "PENDING" {
								invariant.Always(pending_count > 0, "Some tasks are pending")
								count_mu.Lock()
								pending_count--
								processing_count++
								count_mu.Unlock()
							} else {
								invariant.Always(processing_count > 0, "Some tasks are processing")
							}
						case "FINISHED":
							invariant.Always(
								previous_status == "PENDING" || previous_status == "PROCESSING",
								"Task could be finished from any previous state",
							)
							count_mu.Lock()
							finished_count++
							if previous_status == "PENDING" {
								invariant.Always(pending_count > 0, "Some tasks are pending")
								pending_count--
							} else {
								invariant.Always(processing_count > 0, "Some tasks are processing")
								processing_count--
							}
							count_mu.Unlock()
							return
						}
					}
					previous_status = payload.Status
				}
			}

		}()
	}
	wg.Wait()

	invariant.Always(len(tasks) == MAX_TASKS_CREATED, "Created maximum number of tasks")
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
