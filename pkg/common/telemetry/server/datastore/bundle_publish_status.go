package datastore

import (
	"github.com/spiffe/spire/pkg/common/telemetry"
)

// StartSetBundlePublishStatus returns metric for server's datastore, on
// setting a bundle publishing status.
func StartSetBundlePublishStatus(m telemetry.Metrics) *telemetry.CallCounter {
	return telemetry.StartCall(m, telemetry.Datastore, telemetry.BundlePublishStatus, telemetry.Set)
}

// StartFetchBundlePublishStatuses returns metric for server's datastore, on
// fetching bundle publishing statuses.
func StartFetchBundlePublishStatuses(m telemetry.Metrics) *telemetry.CallCounter {
	return telemetry.StartCall(m, telemetry.Datastore, telemetry.BundlePublishStatus, telemetry.Fetch)
}
