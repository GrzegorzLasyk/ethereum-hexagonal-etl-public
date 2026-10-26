// Command probe is the small HTTP readiness client used by Compose healthchecks.
package main

import (
	"io"
	"net/http"
	"os"
	"time"
)

func main() {
	args := os.Args[1:]
	body := false
	if len(args) == 2 && args[0] == "--body" {
		body = true
		args = args[1:]
	}
	if len(args) != 1 {
		os.Exit(1)
	}
	c := &http.Client{Timeout: 2 * time.Second}
	r, err := c.Get(args[0])
	if err != nil {
		os.Exit(1)
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		os.Exit(1)
	}
	if body {
		if _, err := io.Copy(os.Stdout, io.LimitReader(r.Body, 256<<10)); err != nil {
			os.Exit(1)
		}
	}
}
