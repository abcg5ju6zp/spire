package datastore

import (
	"github.com/spiffe/spire/pkg/common/telemetry"
)

// Call counters (timing and success metrics) for recoverable Entry batch
// operation records.

// StartCreateEntryBatchOperationCall returns the metric for persisting a new
// batch operation.
func StartCreateEntryBatchOperationCall(m telemetry.Metrics) *telemetry.CallCounter {
	return telemetry.StartCall(m, telemetry.Datastore, telemetry.EntryBatchOperation, telemetry.Create)
}

// StartFetchEntryBatchOperationCall returns the metric for fetching a batch
// operation by ID.
func StartFetchEntryBatchOperationCall(m telemetry.Metrics) *telemetry.CallCounter {
	return telemetry.StartCall(m, telemetry.Datastore, telemetry.EntryBatchOperation, telemetry.Fetch)
}

// StartPruneEntryBatchOperationsCall returns the metric for pruning expired
// batch operations.
func StartPruneEntryBatchOperationsCall(m telemetry.Metrics) *telemetry.CallCounter {
	return telemetry.StartCall(m, telemetry.Datastore, telemetry.EntryBatchOperation, telemetry.Prune)
}
