package metrics

import (
	"bytes"
	"fmt"
)

type admissionMetrics struct {
	enabled                                  bool
	active, slots, messageBytes, budget      int
	transport, durable, connections, retries uint64
}

func (r *Registry) ConfigureAdmission(slots, messageBytes, budget int) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.admission.enabled = true
	r.admission.slots = slots
	r.admission.messageBytes = messageBytes
	r.admission.budget = budget
}
func (r *Registry) AdmissionActive(delta int) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.admission.active += delta
}
func (r *Registry) AdmissionRejected(reason string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	switch reason {
	case "transport":
		r.admission.transport++
	case "durable":
		r.admission.durable++
	case "connections":
		r.admission.connections++
	}
}
func (r *Registry) OverloadRetry() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.admission.retries++
}
func (r *Registry) writeAdmission(b *bytes.Buffer) {
	a := r.admission
	family(b, "etl_overload_retries_total", "counter", "Durably scheduled retries caused by receiver overload since restart.")
	fmt.Fprintf(b, "etl_overload_retries_total %d\n", a.retries)
	if !a.enabled {
		return
	}
	gauge(b, "etl_admission_inflight_requests", "Currently admitted RPCs including decode and persistence.", a.active)
	gauge(b, "etl_admission_request_capacity", "Effective global RPC slots.", a.slots)
	gauge(b, "etl_admission_reserved_bytes", "Maximum serialized bytes reserved by active RPCs; not heap size.", a.active*a.messageBytes)
	gauge(b, "etl_admission_byte_capacity", "Configured serialized message reservation budget.", a.budget)
	gauge(b, "etl_admission_transport_backpressure", "1 when every global RPC slot is occupied.", flag(a.active >= a.slots))
	family(b, "etl_admission_rejections_total", "counter", "Admission rejections since process restart; connection refusals are transport errors.")
	fmt.Fprintf(b, "etl_admission_rejections_total{reason=\"transport\"} %d\netl_admission_rejections_total{reason=\"durable\"} %d\netl_admission_rejections_total{reason=\"connections\"} %d\n", a.transport, a.durable, a.connections)
}
