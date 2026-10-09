package sqlstore

import (
	"testing"
	"time"

	"github.com/spiffe/spire/pkg/server/datastore"
	"github.com/spiffe/spire/proto/spire/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func testEntry(spiffePath string) *common.RegistrationEntry {
	return &common.RegistrationEntry{
		ParentId:  "spiffe://example.org/agent",
		SpiffeId:  "spiffe://example.org" + spiffePath,
		Selectors: []*common.Selector{{Type: "unix", Value: "uid:1000"}},
	}
}

func TestEntryBatchOperationLifecycle(t *testing.T) {
	ds := newTestPlugin(t)

	op := &datastore.EntryBatchOperation{
		ID:          "op-1",
		Type:        datastore.BatchCreateEntryOperation,
		RequestHash: []byte("hash"),
		ItemCount:   1,
		ExpiresAt:   time.Now().Add(time.Hour),
	}

	// Creating an operation persists it.
	require.NoError(t, ds.CreateEntryBatchOperation(ctx, op))
	assert.False(t, op.CreatedAt.IsZero())

	fetched, err := ds.FetchEntryBatchOperation(ctx, "op-1")
	require.NoError(t, err)
	assert.Equal(t, "op-1", fetched.ID)
	assert.Equal(t, datastore.BatchCreateEntryOperation, fetched.Type)
	assert.Equal(t, []byte("hash"), fetched.RequestHash)
	assert.Equal(t, 1, fetched.ItemCount)

	// Creating the same operation ID again fails with AlreadyExists.
	err = ds.CreateEntryBatchOperation(ctx, op)
	assert.Equal(t, codes.AlreadyExists, status.Code(err))

	// Unknown operations are NotFound.
	_, err = ds.FetchEntryBatchOperation(ctx, "missing")
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestJournaledCreateReplay(t *testing.T) {
	ds := newTestPlugin(t)
	require.NoError(t, ds.CreateEntryBatchOperation(ctx, &datastore.EntryBatchOperation{
		ID:        "op-create",
		Type:      datastore.BatchCreateEntryOperation,
		ItemCount: 1,
		ExpiresAt: time.Now().Add(time.Hour),
	}))

	entry := testEntry("/workload-1")

	first, existing, err := ds.CreateOrReturnRegistrationEntryForBatchOperation(ctx, entry, "op-create", 0)
	require.NoError(t, err)
	assert.False(t, existing)
	require.NotEmpty(t, first.EntryId)

	// Replaying returns the same entry and does not mutate again.
	replay, replayExisting, err := ds.CreateOrReturnRegistrationEntryForBatchOperation(ctx, entry, "op-create", 0)
	require.NoError(t, err)
	assert.False(t, replayExisting)
	assert.Equal(t, first.EntryId, replay.EntryId)
	assert.Equal(t, first.RevisionNumber, replay.RevisionNumber)
	assert.Equal(t, first.CreatedAt, replay.CreatedAt)

	// The "already exists" outcome is recorded too.
	require.NoError(t, ds.CreateEntryBatchOperation(ctx, &datastore.EntryBatchOperation{
		ID:        "op-create-2",
		Type:      datastore.BatchCreateEntryOperation,
		ItemCount: 1,
		ExpiresAt: time.Now().Add(time.Hour),
	}))
	existedEntry, existed, err := ds.CreateOrReturnRegistrationEntryForBatchOperation(ctx, testEntry("/workload-1"), "op-create-2", 0)
	require.NoError(t, err)
	assert.True(t, existed)
	assert.Equal(t, first.EntryId, existedEntry.EntryId)

	existedReplay, existedReplayFlag, err := ds.CreateOrReturnRegistrationEntryForBatchOperation(ctx, testEntry("/workload-1"), "op-create-2", 0)
	require.NoError(t, err)
	assert.True(t, existedReplayFlag)
	assert.Equal(t, first.EntryId, existedReplay.EntryId)
}

func TestJournaledUpdateReplayBumpsRevisionOnce(t *testing.T) {
	ds := newTestPlugin(t)
	require.NoError(t, ds.CreateEntryBatchOperation(ctx, &datastore.EntryBatchOperation{
		ID:        "op-update",
		Type:      datastore.BatchUpdateEntryOperation,
		ItemCount: 1,
		ExpiresAt: time.Now().Add(time.Hour),
	}))

	created, err := ds.CreateRegistrationEntry(ctx, testEntry("/workload-2"))
	require.NoError(t, err)

	updated := proto.Clone(created).(*common.RegistrationEntry)
	updated.X509SvidTtl = 42

	first, err := ds.UpdateRegistrationEntryForBatchOperation(ctx, &updated, nil, "op-update", 0)
	require.NoError(t, err)
	assert.Equal(t, int64(1), first.RevisionNumber)

	replay, err := ds.UpdateRegistrationEntryForBatchOperation(ctx, &updated, nil, "op-update", 0)
	require.NoError(t, err)
	assert.Equal(t, int64(1), replay.RevisionNumber)

	fetched, err := ds.FetchRegistrationEntries(ctx, []string{created.EntryId})
	require.NoError(t, err)
	assert.Equal(t, int64(1), fetched[created.EntryId].RevisionNumber)

	// Updating a missing entry records and replays NotFound.
	missing := &common.RegistrationEntry{
		EntryId:   "missing",
		ParentId:  "spiffe://example.org/agent",
		SpiffeId:  "spiffe://example.org/missing",
		Selectors: []*common.Selector{{Type: "unix", Value: "uid:1"}},
	}
	_, err = ds.UpdateRegistrationEntryForBatchOperation(ctx, missing, nil, "op-update", 1)
	assert.Equal(t, codes.NotFound, status.Code(err))
	_, err = ds.UpdateRegistrationEntryForBatchOperation(ctx, missing, nil, "op-update", 1)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestJournaledDeleteReplay(t *testing.T) {
	ds := newTestPlugin(t)
	require.NoError(t, ds.CreateEntryBatchOperation(ctx, &datastore.EntryBatchOperation{
		ID:        "op-delete",
		Type:      datastore.BatchDeleteEntryOperation,
		ItemCount: 1,
		ExpiresAt: time.Now().Add(time.Hour),
	}))

	created, err := ds.CreateRegistrationEntry(ctx, testEntry("/workload-3"))
	require.NoError(t, err)

	_, err = ds.DeleteRegistrationEntryForBatchOperation(ctx, created.EntryId, "op-delete", 0)
	require.NoError(t, err)

	// Replaying stays OK even though the entry is gone.
	_, err = ds.DeleteRegistrationEntryForBatchOperation(ctx, created.EntryId, "op-delete", 0)
	require.NoError(t, err)

	// Missing deletes record and replay NotFound.
	_, err = ds.DeleteRegistrationEntryForBatchOperation(ctx, "never-existed", "op-delete", 1)
	assert.Equal(t, codes.NotFound, status.Code(err))
	_, err = ds.DeleteRegistrationEntryForBatchOperation(ctx, "never-existed", "op-delete", 1)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestPruneEntryBatchOperations(t *testing.T) {
	ds := newTestPlugin(t)
	expiry := time.Now().Add(time.Hour)
	require.NoError(t, ds.CreateEntryBatchOperation(ctx, &datastore.EntryBatchOperation{
		ID:        "op-prune",
		Type:      datastore.BatchDeleteEntryOperation,
		ItemCount: 1,
		ExpiresAt: expiry,
	}))
	_, err := ds.DeleteRegistrationEntryForBatchOperation(ctx, "missing", "op-prune", 0)
	assert.Equal(t, codes.NotFound, status.Code(err))

	require.NoError(t, ds.PruneEntryBatchOperations(ctx, expiry))
	_, err = ds.FetchEntryBatchOperation(ctx, "op-prune")
	require.NoError(t, err)

	require.NoError(t, ds.PruneEntryBatchOperations(ctx, expiry.Add(time.Second)))
	_, err = ds.FetchEntryBatchOperation(ctx, "op-prune")
	assert.Equal(t, codes.NotFound, status.Code(err))

	// The item results were pruned as well: the delete executes afresh.
	require.NoError(t, ds.CreateEntryBatchOperation(ctx, &datastore.EntryBatchOperation{
		ID:        "op-prune",
		Type:      datastore.BatchDeleteEntryOperation,
		ItemCount: 1,
		ExpiresAt: time.Now().Add(time.Hour),
	}))
	_, err = ds.DeleteRegistrationEntryForBatchOperation(ctx, "missing", "op-prune", 0)
	assert.Equal(t, codes.NotFound, status.Code(err))
}
