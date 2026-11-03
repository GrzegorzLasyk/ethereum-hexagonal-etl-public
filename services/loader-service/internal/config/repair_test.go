package config

import (
	"strings"
	"testing"
)

func TestRepairConfiguration(t *testing.T) {
	for _, key := range []string{"MODE", "REPAIR_QUEUE", "REPAIR_EVENT_ID", "REPAIR_OPERATION_ID", "REPAIR_ALLOWANCE"} {
		t.Setenv("LOADER_"+key, "")
	}
	if r, err := loadRepair("1"); err != nil || r != nil {
		t.Fatal("normal service mode changed", err)
	}
	t.Setenv("LOADER_REPAIR_ALLOWANCE", "3")
	if _, err := loadRepair("1"); err == nil {
		t.Fatal("implicit repair accepted")
	}
	t.Setenv("LOADER_MODE", "repair")
	t.Setenv("LOADER_REPAIR_QUEUE", "inbox")
	t.Setenv("LOADER_REPAIR_EVENT_ID", "transformed-block/v1/1/1000000/"+strings.Repeat("a", 64))
	t.Setenv("LOADER_REPAIR_OPERATION_ID", "repair-1")
	if r, err := loadRepair("1"); err != nil || r.Allowance != 3 {
		t.Fatal("valid repair rejected", err)
	}
	for _, invalid := range []string{"0", "1001", "-1", "", "credential"} {
		t.Setenv("LOADER_REPAIR_ALLOWANCE", invalid)
		if _, err := loadRepair("1"); err == nil || strings.Contains(err.Error(), "credential") {
			t.Fatal("invalid allowance or secret disclosure", err)
		}
	}
}
