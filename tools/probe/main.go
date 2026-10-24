// Command probe is the small HTTP readiness client used by Compose healthchecks.
package main

import (
	"net/http"
	"os"
	"time"
)

func main() {
	if len(os.Args) != 2 {
		os.Exit(1)
	}
	c := &http.Client{Timeout: 2 * time.Second}
	r, err := c.Get(os.Args[1])
	if err != nil {
		os.Exit(1)
	}
	r.Body.Close()
	if r.StatusCode != http.StatusOK {
		os.Exit(1)
	}
}
