// repair_test.go exercises audited finite retry allowance and recovery guards in the startup
// configuration boundary.

package config

import (
	"strings"
	"testing"
)

// TestRepairConfiguration checks an audited finite allowance and stable operation/event identity
// are required before invoking recovery storage.
func TestRepairConfiguration(t *testing.T) {
	for _, key := range []string{"MODE", "REPAIR_QUEUE", "REPAIR_EVENT_ID", "REPAIR_OPERATION_ID", "REPAIR_ALLOWANCE"} {
		t.Setenv("TRANSFORMER_"+key, "")
	}
	if r, err := loadRepair("1"); err != nil || r != nil {
		t.Fatal("normal service mode changed", err)
	}
	t.Setenv("TRANSFORMER_REPAIR_ALLOWANCE", "3")
	if _, err := loadRepair("1"); err == nil {
		t.Fatal("implicit repair accepted")
	}
	t.Setenv("TRANSFORMER_MODE", "repair")
	t.Setenv("TRANSFORMER_REPAIR_QUEUE", "inbox")
	t.Setenv("TRANSFORMER_REPAIR_EVENT_ID", "raw-block/v1/1/1000000/"+strings.Repeat("a", 64))
	t.Setenv("TRANSFORMER_REPAIR_OPERATION_ID", "repair-1")
	if r, err := loadRepair("1"); err != nil || r.Allowance != 3 {
		t.Fatal("valid repair rejected", err)
	}
	for _, invalid := range []string{"0", "1001", "-1", "", "credential"} {
		t.Setenv("TRANSFORMER_REPAIR_ALLOWANCE", invalid)
		if _, err := loadRepair("1"); err == nil || strings.Contains(err.Error(), "credential") {
			t.Fatal("invalid allowance or secret disclosure", err)
		}
	}
}
