// repair_test.go exercises audited finite retry allowance and recovery guards in the application
// orchestration layer.

package usecases

import (
	"context"
	"strings"
	"testing"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/application/ports/outbound"
)

// repairFake counts repair-port calls to distinguish request validation from storage effects.
type repairFake struct{ calls int }

// Repair counts invocation of the fake repair port so invalid requests can be proven to fail
// before storage.
func (f *repairFake) Repair(context.Context, outbound.RepairRequest) error { f.calls++; return nil }

// TestRepairValidation checks an audited finite allowance and stable operation/event identity are
// required before invoking recovery storage.
func TestRepairValidation(t *testing.T) {
	valid := outbound.RepairRequest{Queue: "inbox", EventID: "raw-block/v1/1/1/" + strings.Repeat("a", 64), OperationID: "repair-1", Allowance: 3}
	f := new(repairFake)
	if err := Repair(t.Context(), "1", f, valid); err != nil || f.calls != 1 {
		t.Fatal(err)
	}
	for _, mutate := range []func(*outbound.RepairRequest){func(r *outbound.RepairRequest) { r.Queue = "bulk" }, func(r *outbound.RepairRequest) { r.Allowance = 0 }, func(r *outbound.RepairRequest) { r.Allowance = 1001 }, func(r *outbound.RepairRequest) { r.EventID = "secret" }, func(r *outbound.RepairRequest) { r.OperationID = "" }, func(r *outbound.RepairRequest) { r.EventID = strings.Replace(r.EventID, "/1/1/", "/2/1/", 1) }} {
		r := valid
		mutate(&r)
		if Repair(t.Context(), "1", f, r) == nil || f.calls != 1 {
			t.Fatal("invalid request reached store")
		}
	}
}
