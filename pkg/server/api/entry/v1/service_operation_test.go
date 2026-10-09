package entry_test

import (
	"context"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	entryv1 "github.com/spiffe/spire-api-sdk/proto/spire/api/server/entry/v1"
	"github.com/spiffe/spire-api-sdk/proto/spire/api/types"
	"github.com/spiffe/spire/pkg/common/telemetry"
	"github.com/spiffe/spire/pkg/server/api"
	"github.com/spiffe/spire/pkg/server/datastore"
	sqlstore "github.com/spiffe/spire/pkg/server/datastore/sqlstore"
	"github.com/spiffe/spire/proto/spire/common"
	"github.com/spiffe/spire/test/fakes/fakedatastore"
	"github.com/spiffe/spire/test/spiretest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const operationIDHeader = "x-spire-operation-id"

func withOperationID(ctx context.Context, operationID string) context.Context {
	if operationID == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, operationIDHeader, operationID)
}

func createEntryProto(path string, uid string) *types.Entry {
	return &types.Entry{
		ParentId: api.ProtoFromID(agentID),
		SpiffeId: &types.SPIFFEID{
			TrustDomain: td.Name(),
			Path:        path,
		},
		Selectors: []*types.Selector{
			{Type: "unix", Value: uid},
		},
	}
}

func commonRegistrationEntry(path string, uid string) *common.RegistrationEntry {
	return &common.RegistrationEntry{
		ParentId: agentID.String(),
		SpiffeId: spiffeid.RequireFromSegments(td, path).String(),
		Selectors: []*common.Selector{
			{Type: "unix", Value: uid},
		},
	}
}

func requireAllOK(t *testing.T, results []*entryv1.BatchCreateEntryResponse_Result) {
	t.Helper()
	for i, r := range results {
		require.Equal(t, codes.OK, codes.Code(r.Status.Code), "item %d: %s", i, r.Status.Message)
	}
}

func requireEntryEventCount(t *testing.T, ds datastore.DataStore, expected int) {
	t.Helper()
	resp, err := ds.ListRegistrationEntryEvents(ctx, &datastore.ListRegistrationEntryEventsRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Events, expected)
}

func requireRegistrationEntryCount(t *testing.T, ds datastore.DataStore, expected int) {
	t.Helper()
	resp, err := ds.ListRegistrationEntries(ctx, &datastore.ListRegistrationEntriesRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Entries, expected)
}

// A batch create with an operation ID can be retried: the retry returns the
// same confirmed results and never creates equivalent entries a second time.
func TestBatchCreateEntryOperationResume(t *testing.T) {
	ds := fakedatastore.New(t)
	test := setupServiceTest(t, ds)
	defer test.Cleanup()

	req := &entryv1.BatchCreateEntryRequest{
		Entries: []*types.Entry{
			createEntryProto("/workload-1", "uid:1001"),
			createEntryProto("/workload-2", "uid:1002"),
		},
	}

	var hdr metadata.MD
	resp1, err := test.client.BatchCreateEntry(withOperationID(context.Background(), "resume-create"), req, grpc.Header(&hdr))
	require.NoError(t, err)
	requireAllOK(t, resp1.Results)
	require.Equal(t, []string{"resume-create"}, hdr.Get(operationIDHeader))

	// The response can be recovered even if it never reached the client.
	resp2, err := test.client.BatchCreateEntry(withOperationID(context.Background(), "resume-create"), req)
	require.NoError(t, err)
	spiretest.AssertProtoEqual(t, resp1, resp2)

	requireRegistrationEntryCount(t, ds, 2)
	requireEntryEventCount(t, ds, 2)
}

// The output mask is not part of the operation identity: a retry with a
// different mask re-applies the new mask to the recovered results.
func TestBatchCreateEntryOperationResumeDifferentOutputMask(t *testing.T) {
	ds := fakedatastore.New(t)
	test := setupServiceTest(t, ds)
	defer test.Cleanup()

	req := &entryv1.BatchCreateEntryRequest{
		Entries:    []*types.Entry{createEntryProto("/workload-1", "uid:1001")},
		OutputMask: &types.EntryMask{RevisionNumber: true},
	}

	resp1, err := test.client.BatchCreateEntry(withOperationID(context.Background(), "resume-mask"), req)
	require.NoError(t, err)
	requireAllOK(t, resp1.Results)
	require.NotEmpty(t, resp1.Results[0].Entry.Id)
	require.Nil(t, resp1.Results[0].Entry.SpiffeId)

	retryReq := proto.Clone(req).(*entryv1.BatchCreateEntryRequest)
	retryReq.OutputMask = &types.EntryMask{RevisionNumber: true, SpiffeId: true}

	resp2, err := test.client.BatchCreateEntry(withOperationID(context.Background(), "resume-mask"), retryReq)
	require.NoError(t, err)
	requireAllOK(t, resp2.Results)
	require.Equal(t, resp1.Results[0].Entry.Id, resp2.Results[0].Entry.Id)
	require.Zero(t, resp1.Results[0].Entry.RevisionNumber)
	require.Nil(t, resp1.Results[0].Entry.SpiffeId)
	spiretest.AssertProtoEqual(t, req.Entries[0].SpiffeId, resp2.Results[0].Entry.SpiffeId)

	requireRegistrationEntryCount(t, ds, 1)
	requireEntryEventCount(t, ds, 1)
}

// When the datastore fails partway through, retrying the batch returns the
// already confirmed results and only executes the unfinished items.
func TestBatchCreateEntryOperationPartialFailureResume(t *testing.T) {
	ds := fakedatastore.New(t)
	test := setupServiceTest(t, ds)
	defer test.Cleanup()

	req := &entryv1.BatchCreateEntryRequest{
		Entries: []*types.Entry{
			createEntryProto("/workload-1", "uid:1001"),
			createEntryProto("/workload-2", "uid:1002"),
			createEntryProto("/workload-3", "uid:1003"),
		},
	}

	// CreateEntryBatchOperation succeeds, item 0 succeeds, item 1 hits a
	// transient datastore failure, item 2 succeeds.
	ds.AppendNextError(nil)
	ds.AppendNextError(nil)
	ds.AppendNextError(status.Error(codes.Internal, "datastore unavailable"))
	ds.AppendNextError(nil)

	resp1, err := test.client.BatchCreateEntry(withOperationID(context.Background(), "partial-create"), req)
	require.NoError(t, err)
	require.Len(t, resp1.Results, 3)
	require.Equal(t, codes.OK, codes.Code(resp1.Results[0].Status.Code))
	require.Equal(t, codes.Internal, codes.Code(resp1.Results[1].Status.Code))
	require.Contains(t, resp1.Results[1].Status.Message, "failed to create entry: datastore unavailable")
	require.Equal(t, codes.OK, codes.Code(resp1.Results[2].Status.Code))

	// Items 0 and 2 were committed before the failure; item 1 never executed.
	requireRegistrationEntryCount(t, ds, 2)
	requireEntryEventCount(t, ds, 2)

	// Retry the whole batch after the datastore recovered.
	resp2, err := test.client.BatchCreateEntry(withOperationID(context.Background(), "partial-create"), req)
	require.NoError(t, err)
	requireAllOK(t, resp2.Results)

	// Items 0 and 2 report their original result; item 1 executes for the
	// first time. Nothing was created twice.
	require.Equal(t, resp1.Results[0].Entry.Id, resp2.Results[0].Entry.Id)
	require.Equal(t, resp1.Results[2].Entry.Id, resp2.Results[2].Entry.Id)
	require.NotEmpty(t, resp2.Results[1].Entry.Id)
	requireRegistrationEntryCount(t, ds, 3)
	requireEntryEventCount(t, ds, 3)

	// One more retry is still fully idempotent.
	resp3, err := test.client.BatchCreateEntry(withOperationID(context.Background(), "partial-create"), req)
	require.NoError(t, err)
	spiretest.AssertProtoEqual(t, resp2, resp3)
	requireRegistrationEntryCount(t, ds, 3)
	requireEntryEventCount(t, ds, 3)
}

// A committed create result keeps reporting success even if the state of the
// datastore changed in the meantime: retrying must not create a second,
// equivalent entry.
func TestBatchCreateEntryOperationCommittedResultIsStable(t *testing.T) {
	ds := fakedatastore.New(t)
	test := setupServiceTest(t, ds)
	defer test.Cleanup()

	req := &entryv1.BatchCreateEntryRequest{
		Entries: []*types.Entry{createEntryProto("/workload-1", "uid:1001")},
	}

	resp1, err := test.client.BatchCreateEntry(withOperationID(context.Background(), "stable-create"), req)
	require.NoError(t, err)
	requireAllOK(t, resp1.Results)
	entryID := resp1.Results[0].Entry.Id

	// Delete the entry out-of-band after the first response was lost.
	_, err = ds.DeleteRegistrationEntry(ctx, entryID)
	require.NoError(t, err)
	requireRegistrationEntryCount(t, ds, 0)

	// Retrying replays the committed result instead of recreating the entry.
	resp2, err := test.client.BatchCreateEntry(withOperationID(context.Background(), "stable-create"), req)
	require.NoError(t, err)
	require.Equal(t, codes.OK, codes.Code(resp2.Results[0].Status.Code))
	require.Equal(t, entryID, resp2.Results[0].Entry.Id)
	requireRegistrationEntryCount(t, ds, 0)
	// One create event and one out-of-band delete event only.
	requireEntryEventCount(t, ds, 2)
}

// Reusing an operation ID with different input is explicitly rejected.
func TestBatchOperationIDReuseWithDifferentRequest(t *testing.T) {
	for _, tc := range []struct {
		name    string
		first   func(client entryv1.EntryClient, id string) error
		second  func(client entryv1.EntryClient, id string) error
	}{
		{
			name: "create different entries",
			first: func(c entryv1.EntryClient, id string) error {
				_, err := c.BatchCreateEntry(withOperationID(ctx, id), &entryv1.BatchCreateEntryRequest{
					Entries: []*types.Entry{createEntryProto("/a", "uid:1")},
				})
				return err
			},
			second: func(c entryv1.EntryClient, id string) error {
				_, err := c.BatchCreateEntry(withOperationID(ctx, id), &entryv1.BatchCreateEntryRequest{
					Entries: []*types.Entry{createEntryProto("/b", "uid:1")},
				})
				return err
			},
		},
		{
			name: "create different item count",
			first: func(c entryv1.EntryClient, id string) error {
				_, err := c.BatchCreateEntry(withOperationID(ctx, id), &entryv1.BatchCreateEntryRequest{
					Entries: []*types.Entry{createEntryProto("/a", "uid:1")},
				})
				return err
			},
			second: func(c entryv1.EntryClient, id string) error {
				_, err := c.BatchCreateEntry(withOperationID(ctx, id), &entryv1.BatchCreateEntryRequest{
					Entries: []*types.Entry{
						createEntryProto("/a", "uid:1"),
						createEntryProto("/b", "uid:2"),
					},
				})
				return err
			},
		},
		{
			name: "reuse create id on update",
			first: func(c entryv1.EntryClient, id string) error {
				_, err := c.BatchCreateEntry(withOperationID(ctx, id), &entryv1.BatchCreateEntryRequest{
					Entries: []*types.Entry{createEntryProto("/a", "uid:1")},
				})
				return err
			},
			second: func(c entryv1.EntryClient, id string) error {
				_, err := c.BatchUpdateEntry(withOperationID(ctx, id), &entryv1.BatchUpdateEntryRequest{
					Entries: []*types.Entry{{Id: "some-id"}},
				})
				return err
			},
		},
		{
			name: "reuse create id on delete",
			first: func(c entryv1.EntryClient, id string) error {
				_, err := c.BatchCreateEntry(withOperationID(ctx, id), &entryv1.BatchCreateEntryRequest{
					Entries: []*types.Entry{createEntryProto("/a", "uid:1")},
				})
				return err
			},
			second: func(c entryv1.EntryClient, id string) error {
				_, err := c.BatchDeleteEntry(withOperationID(ctx, id), &entryv1.BatchDeleteEntryRequest{
					Ids: []string{"some-id"},
				})
				return err
			},
		},
		{
			name: "update with different input mask",
			first: func(c entryv1.EntryClient, id string) error {
				_, err := c.BatchUpdateEntry(withOperationID(ctx, id), &entryv1.BatchUpdateEntryRequest{
					Entries:   []*types.Entry{{Id: "some-id", Hint: "a"}},
					InputMask: &types.EntryMask{Hint: true},
				})
				return err
			},
			second: func(c entryv1.EntryClient, id string) error {
				_, err := c.BatchUpdateEntry(withOperationID(ctx, id), &entryv1.BatchUpdateEntryRequest{
					Entries:   []*types.Entry{{Id: "some-id", Hint: "a"}},
					InputMask: &types.EntryMask{Admin: true},
				})
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ds := fakedatastore.New(t)
			test := setupServiceTest(t, ds)
			defer test.Cleanup()

			require.NoError(t, tc.first(test.client, "rejected-op"))
			err := tc.second(test.client, "rejected-op")
			spiretest.RequireGRPCStatusContains(t, err, codes.InvalidArgument, "operation ID is already in use for a different request")
		})
	}
}

// Malformed operation IDs are rejected as InvalidArgument before any entry is
// touched.
func TestBatchOperationIDInvalid(t *testing.T) {
	ds := fakedatastore.New(t)
	test := setupServiceTest(t, ds)
	defer test.Cleanup()

	req := &entryv1.BatchCreateEntryRequest{
		Entries: []*types.Entry{createEntryProto("/a", "uid:1")},
	}

	for _, operationID := range []string{
		"has spaces",
		"has/slash",
		"semi;colon",
		strings.Repeat("a", 129),
	} {
		_, err := test.client.BatchCreateEntry(withOperationID(ctx, operationID), req)
		spiretest.RequireGRPCStatusContains(t, err, codes.InvalidArgument, "invalid operation ID")
	}

	requireRegistrationEntryCount(t, ds, 0)
	requireEntryEventCount(t, ds, 0)
}

// A batch delete with an operation ID reports stable OK/NotFound results and
// never deletes twice.
func TestBatchDeleteEntryOperationResume(t *testing.T) {
	ds := fakedatastore.New(t)
	test := setupServiceTest(t, ds)
	defer test.Cleanup()

	created, err := ds.CreateRegistrationEntry(ctx, commonRegistrationEntry("delete-1", "uid:2001"))
	require.NoError(t, err)
	created2, err := ds.CreateRegistrationEntry(ctx, commonRegistrationEntry("delete-2", "uid:2002"))
	require.NoError(t, err)

	req := &entryv1.BatchDeleteEntryRequest{
		Ids: []string{created.EntryId, "missing", created2.EntryId},
	}

	resp1, err := test.client.BatchDeleteEntry(withOperationID(context.Background(), "resume-delete"), req)
	require.NoError(t, err)
	require.Equal(t, codes.OK, codes.Code(resp1.Results[0].Status.Code))
	require.Equal(t, codes.NotFound, codes.Code(resp1.Results[1].Status.Code))
	require.Equal(t, codes.OK, codes.Code(resp1.Results[2].Status.Code))
	requireRegistrationEntryCount(t, ds, 0)
	// Two creates and two deletes; the missing id never produces an event.
	requireEntryEventCount(t, ds, 4)

	resp2, err := test.client.BatchDeleteEntry(withOperationID(context.Background(), "resume-delete"), req)
	require.NoError(t, err)
	spiretest.AssertProtoEqual(t, resp1, resp2)

	// Missing id stays NotFound; no second delete event was emitted.
	requireEntryEventCount(t, ds, 4)
}

// A delete that already committed is not applied again even if a new entry
// with the same ID exists meanwhile.
func TestBatchDeleteEntryOperationNoDoubleDelete(t *testing.T) {
	ds := fakedatastore.New(t)
	test := setupServiceTest(t, ds)
	defer test.Cleanup()

	created, err := ds.CreateRegistrationEntry(ctx, commonRegistrationEntry("delete-1", "uid:2001"))
	require.NoError(t, err)

	req := &entryv1.BatchDeleteEntryRequest{Ids: []string{created.EntryId}}
	_, err = test.client.BatchDeleteEntry(withOperationID(ctx, "delete-once"), req)
	require.NoError(t, err)

	// A different, new entry reuses the same entry ID.
	recreated, err := ds.CreateRegistrationEntry(ctx, &common.RegistrationEntry{
		EntryId:  created.EntryId,
		ParentId: created.ParentId,
		SpiffeId: created.SpiffeId,
		Selectors: created.Selectors,
	})
	require.NoError(t, err)
	require.Equal(t, created.EntryId, recreated.EntryId)

	resp, err := test.client.BatchDeleteEntry(withOperationID(ctx, "delete-once"), req)
	require.NoError(t, err)
	require.Equal(t, codes.OK, codes.Code(resp.Results[0].Status.Code))

	// The recreated entry must still be there: no second delete was applied.
	fetched, err := ds.FetchRegistrationEntries(ctx, []string{created.EntryId})
	require.NoError(t, err)
	require.Contains(t, fetched, created.EntryId)
}

// A batch update bumps the revision exactly once regardless of retries, and a
// not-found update result stays not-found.
func TestBatchUpdateEntryOperationResume(t *testing.T) {
	ds := fakedatastore.New(t)
	test := setupServiceTest(t, ds)
	defer test.Cleanup()

	createResp, err := test.client.BatchCreateEntry(ctx, &entryv1.BatchCreateEntryRequest{
		Entries: []*types.Entry{createEntryProto("/update-1", "uid:3001")},
	})
	require.NoError(t, err)
	entryID := createResp.Results[0].Entry.Id

	updateEntry := proto.Clone(createResp.Results[0].Entry).(*types.Entry)
	updateEntry.X509SvidTtl = 42

	req := &entryv1.BatchUpdateEntryRequest{
		Entries: []*types.Entry{
			updateEntry,
			{Id: "missing", SpiffeId: &types.SPIFFEID{TrustDomain: td.Name(), Path: "/nope"}},
		},
	}
	// The missing entry is a full (unmasked) update which needs selectors.
	req.Entries[1].ParentId = api.ProtoFromID(agentID)
	req.Entries[1].Selectors = []*types.Selector{{Type: "unix", Value: "uid:nope"}}

	resp1, err := test.client.BatchUpdateEntry(withOperationID(context.Background(), "resume-update"), req)
	require.NoError(t, err)
	require.Len(t, resp1.Results, 2)
	require.Equal(t, codes.OK, codes.Code(resp1.Results[0].Status.Code))
	require.Equal(t, int64(1), resp1.Results[0].Entry.RevisionNumber)
	require.Equal(t, int32(42), resp1.Results[0].Entry.X509SvidTtl)
	require.Equal(t, codes.NotFound, codes.Code(resp1.Results[1].Status.Code))

	resp2, err := test.client.BatchUpdateEntry(withOperationID(context.Background(), "resume-update"), req)
	require.NoError(t, err)
	spiretest.AssertProtoEqual(t, resp1, resp2)

	fetched, err := ds.FetchRegistrationEntries(ctx, []string{entryID})
	require.NoError(t, err)
	require.Equal(t, int64(1), fetched[entryID].RevisionNumber)
	require.Equal(t, int32(42), fetched[entryID].X509SvidTtl)
	// One create, one update; the not-found item mutated nothing.
	requireEntryEventCount(t, ds, 2)
}

// Identified batch requests carry the operation ID in the audit log so every
// attempt can be correlated, while requests without an ID keep the legacy
// behavior and response.
func TestBatchOperationAuditAndLegacyCompatibility(t *testing.T) {
	ds := fakedatastore.New(t)
	test := setupServiceTest(t, ds)
	defer test.Cleanup()

	req := &entryv1.BatchCreateEntryRequest{
		Entries: []*types.Entry{
			createEntryProto("/audit-1", "uid:4001"),
			createEntryProto("/audit-2", "uid:4002"),
		},
	}

	var hdr metadata.MD
	_, err := test.client.BatchCreateEntry(withOperationID(context.Background(), "audit-op"), req, grpc.Header(&hdr))
	require.NoError(t, err)
	require.Equal(t, []string{"audit-op"}, hdr.Get(operationIDHeader))

	var audited int
	for _, e := range test.logHook.AllEntries() {
		if e.Data[telemetry.OperationID] == "audit-op" {
			audited++
		}
	}
	require.Equal(t, len(req.Entries), audited, "every item should be audited with the operation ID")

	// Legacy clients without an operation ID behave unchanged.
	test.logHook.Reset()
	var legacyHdr metadata.MD
	legacyResp, err := test.client.BatchCreateEntry(context.Background(), &entryv1.BatchCreateEntryRequest{
		Entries: []*types.Entry{createEntryProto("/legacy-1", "uid:5001")},
	}, grpc.Header(&legacyHdr))
	require.NoError(t, err)
	requireAllOK(t, legacyResp.Results)
	require.Empty(t, legacyHdr.Get(operationIDHeader))
	for _, e := range test.logHook.AllEntries() {
		assert.NotContains(t, e.Data, telemetry.OperationID)
	}
}

// Durable per-item results survive a server process restart: a new server
// instance over the same database resumes the operation from persisted
// results.
func TestBatchOperationRecoveredAfterRestart(t *testing.T) {
	dbFile := filepath.Join(t.TempDir(), "spire.db")

	newPlugin := func() *sqlstore.Plugin {
		log, _ := test.NewNullLogger()
		p := sqlstore.New(log)
		p.SetUseServerTimestamps(true)
		require.NoError(t, p.Configure(ctx, fmt.Sprintf(`
			database_type = "sqlite3"
			connection_string = "file:%s"
		`, url.PathEscape(dbFile))))
		return p
	}

	req := &entryv1.BatchCreateEntryRequest{
		Entries: []*types.Entry{
			createEntryProto("/restart-1", "uid:6001"),
			createEntryProto("/restart-2", "uid:6002"),
		},
	}

	// First attempt: the batch commits but the process exits before the
	// client learns the result.
	ds1 := newPlugin()
	test1 := setupServiceTest(t, ds1)
	resp1, err := test1.client.BatchCreateEntry(withOperationID(context.Background(), "restart-op"), req)
	require.NoError(t, err)
	requireAllOK(t, resp1.Results)
	test1.Cleanup()
	require.NoError(t, ds1.Close())

	// A fresh process reopens the database and receives the retry.
	ds2 := newPlugin()
	t.Cleanup(func() { _ = ds2.Close() })
	test2 := setupServiceTest(t, ds2)
	defer test2.Cleanup()

	resp2, err := test2.client.BatchCreateEntry(withOperationID(context.Background(), "restart-op"), req)
	require.NoError(t, err)
	spiretest.AssertProtoEqual(t, resp1, resp2)

	requireRegistrationEntryCount(t, ds2, 2)
	requireEntryEventCount(t, ds2, 2)

	// The stored operation can be inspected directly.
	op, err := ds2.FetchEntryBatchOperation(ctx, "restart-op")
	require.NoError(t, err)
	require.Equal(t, datastore.BatchCreateEntryOperation, op.Type)
	require.Equal(t, 2, op.ItemCount)
	require.NotEmpty(t, op.RequestHash)
}

// Expired operations and their item results are pruned together.
func TestPruneEntryBatchOperations(t *testing.T) {
	ds := fakedatastore.New(t)
	test := setupServiceTest(t, ds)
	defer test.Cleanup()

	req := &entryv1.BatchCreateEntryRequest{
		Entries: []*types.Entry{createEntryProto("/prune-1", "uid:7001")},
	}
	_, err := test.client.BatchCreateEntry(withOperationID(ctx, "prune-op"), req)
	require.NoError(t, err)

	op, err := ds.FetchEntryBatchOperation(ctx, "prune-op")
	require.NoError(t, err)

	// Nothing expires before the operation expiry.
	require.NoError(t, ds.PruneEntryBatchOperations(ctx, op.ExpiresAt))
	_, err = ds.FetchEntryBatchOperation(ctx, "prune-op")
	require.NoError(t, err)

	// Once expired, the operation and its item results are both removed.
	require.NoError(t, ds.PruneEntryBatchOperations(ctx, op.ExpiresAt.Add(time.Second)))
	_, err = ds.FetchEntryBatchOperation(ctx, "prune-op")
	require.Equal(t, codes.NotFound, status.Code(err))

	// After pruning, the same operation ID with the same request starts a new
	// operation rather than being rejected as a conflict. The entries still
	// exist, so the fresh operation reports them as already existing.
	resp, err := test.client.BatchCreateEntry(withOperationID(ctx, "prune-op"), req)
	require.NoError(t, err)
	require.Len(t, resp.Results, 1)
	require.Equal(t, codes.AlreadyExists, codes.Code(resp.Results[0].Status.Code))

	// The operation record exists again: it was re-created, not reused.
	op2, err := ds.FetchEntryBatchOperation(ctx, "prune-op")
	require.NoError(t, err)
	require.Equal(t, datastore.BatchCreateEntryOperation, op2.Type)
}
