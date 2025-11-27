package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"
)

const (
	WORKER_SERVICE_NAME = "worker"
	// Number of consecutive iterations where pending tasks decrease compared to the previous check
	// before reducing workers. A value of one means workers are halved immediately once pending tasks
	// drop faster than they accumulate and remain below the threshold.
	// Must be a positive integer.
	CONSECUTIVE_REDUCTION_THRESHOLD = 3
	// Number of pending tasks before doubling workers
	PENDING_COUNT_THRESHOLD = 100
	CHECK_FREQUENCY_SECOND  = time.Second * 5
)

func main() {
	runtime.GOMAXPROCS(1)

	println("autoscaler initialized")

	if CONSECUTIVE_REDUCTION_THRESHOLD <= 0 {
		panic("CONSECUTIVE_REDUCTION_THRESHOLD must be positive")
	}

	var previous []int
	for {
		time.Sleep(CHECK_FREQUENCY_SECOND)
		resp, err := http.Get("http://localhost:8080/__SUPER_DUPER_SECRET_PENDING_COUNT__")
		if err != nil {
			continue
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		pending, _ := strconv.Atoi(strings.TrimSpace(string(b)))
		n_workers := strings.Count(pipe("docker-compose", "ps", "-q", WORKER_SERVICE_NAME), "\n") + 1

		fmt.Printf("pending tasks=%d n_workers=%d\n", pending, n_workers)

		previous = append(previous, pending)
		if len(previous) > CONSECUTIVE_REDUCTION_THRESHOLD {
			previous = previous[1:]
		}

		decreasing := true
		if CONSECUTIVE_REDUCTION_THRESHOLD == 1 {
			decreasing = pending < previous[0]
		} else {
			for i := 1; i < len(previous); i++ {
				if previous[i] == 0 && previous[i-1] == 0 {
					continue
				}
				if previous[i] >= previous[i-1] {
					decreasing = false
					break
				}
			}
		}

		n := -1
		if pending > PENDING_COUNT_THRESHOLD {
			if decreasing {
				n = max(1, n_workers/2)
				if n != n_workers {
					fmt.Printf("halved workers to %d\n", n)
				}
			} else {
				n = n_workers * 2
				fmt.Printf("doubled workers to %d\n", n)
			}
		} else {
			if n_workers > 1 {
				n = 1
				fmt.Printf("traffic is slowing down. resetting to %d worker\n", n)
			}
		}
		spawn("", nil, "docker", "compose", "up", "--no-recreate", "--detach", fmt.Sprintf("--scale=%s=%d", WORKER_SERVICE_NAME, n))
	}
}

// === Scripting ===

func pipe(cmd string, args ...string) string {
	c := exec.Command(cmd, args...)
	var buf bytes.Buffer
	c.Stdout = &buf
	c.Run()
	output, _ := strings.CutSuffix(buf.String(), "\n")
	return output
}

func spawn(working_directory string, environment []string, binary string, arguments ...string) error {
	cmd := exec.Command(binary, arguments...)
	if len(environment) > 0 {
		cmd.Env = os.Environ()
		cmd.Env = append(cmd.Env, environment...)
	}
	if working_directory != "" {
		os.MkdirAll(working_directory, 0o755)
		cmd.Dir = working_directory
	}
	buf := &bytes.Buffer{}
	// Since this only spawns docker compose up, the logs would be cleaner without the container
	// initialization logs.
	cmd.Stdout = nil
	cmd.Stderr = nil
	cmd.Stdin = os.Stdin
	if err := cmd.Run(); err != nil {
		return fmt.Errorf(buf.String())
	}
	return nil
}
