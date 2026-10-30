package main

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

type recoveryRequest struct {
	service, queue, event, operation string
	allowance                        int
}

func parseRecovery(args []string) (recoveryRequest, error) {
	var r recoveryRequest
	switch {
	case len(args) == 5 && args[0] == "replay":
		r = recoveryRequest{service: "extractor", queue: "outbox", event: args[2], operation: args[3]}
		r.allowance, _ = strconv.Atoi(args[4])
	case len(args) == 7 && args[0] == "repair":
		r = recoveryRequest{service: args[2], queue: args[3], event: args[4], operation: args[5]}
		r.allowance, _ = strconv.Atoi(args[6])
	case len(args) == 3 && args[0] == "reindex":
		r.operation = args[2]
		if !operationID(r.operation) {
			return r, errors.New("invalid operation ID")
		}
		return r, nil
	default:
		return r, errors.New("usage: mainnet replay CONFIG EVENT OPERATION ALLOWANCE | repair CONFIG SERVICE QUEUE EVENT OPERATION ALLOWANCE | reindex CONFIG OPERATION")
	}
	if r.service != "extractor" && r.service != "transformer" && r.service != "loader" || r.queue != "inbox" && r.queue != "outbox" || r.service == "extractor" && r.queue != "outbox" || r.service == "loader" && r.queue != "inbox" || args[0] == "repair" && r.service == "extractor" || !operationID(r.operation) || r.allowance < 1 || r.allowance > 1000 {
		return r, errors.New("invalid recovery request")
	}
	prefix := "transformed-block"
	if r.service == "extractor" || r.service == "transformer" && r.queue == "inbox" {
		prefix = "raw-block"
	}
	parts := strings.Split(r.event, "/")
	if len(parts) != 5 || parts[0] != prefix || parts[1] != "v1" || parts[2] != "1" || !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(parts[4]) || parts[4] == strings.Repeat("0", 64) {
		return r, errors.New("invalid recovery event identity")
	}
	n, err := strconv.ParseUint(parts[3], 10, 64)
	if err != nil || strconv.FormatUint(n, 10) != parts[3] {
		return r, errors.New("invalid block height")
	}
	return r, nil
}
func operationID(id string) bool {
	return regexp.MustCompile(`^[A-Za-z0-9_.-]{1,128}$`).MatchString(id)
}
func recoveryArgs(action, project string, r recoveryRequest) []string {
	name := project + "-acquisition"
	if action == "repair" {
		name = project + "-repair-" + r.service
	}
	a := []string{"run", "--rm", "--no-deps", "--name", name}
	env := func(k, v string) { a = append(a, "--env", k+"="+v) }
	if action == "repair" {
		p := strings.ToUpper(r.service)
		env(p+"_MODE", "repair")
		env(p+"_REPAIR_QUEUE", r.queue)
		env(p+"_REPAIR_EVENT_ID", r.event)
		env(p+"_REPAIR_OPERATION_ID", r.operation)
		env(p+"_REPAIR_ALLOWANCE", fmt.Sprint(r.allowance))
		return append(a, r.service)
	}
	env("EXTRACTOR_MODE", action)
	if action != "reindex" {
		env("EXTRACTOR_RPC_URL", "")
		env("EXTRACTOR_MAINNET_OPT_IN", "false")
	}
	if action == "replay" {
		env("EXTRACTOR_REPLAY_EVENT_ID", r.event)
		env("EXTRACTOR_REPLAY_ATTEMPTS", fmt.Sprint(r.allowance))
	}
	if action == "replay" || action == "reindex" {
		env("EXTRACTOR_OPERATION_ID", r.operation)
	}
	return append(a, "extractor")
}
