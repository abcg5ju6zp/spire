package pubmanager

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/spire/pkg/server/plugin/bundlepublisher"
	"github.com/spiffe/spire/proto/spire/common"
	"github.com/spiffe/spire/test/clock"
	"github.com/spiffe/spire/test/fakes/fakedatastore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	td = spiffeid.RequireTrustDomainFromString("spiffe://example.org")
)

func newTestBundle(t *testing.T, seq uint64) *common.Bundle {
	return &common.Bundle{
		TrustDomainId:  td.IDString(),
		SequenceNumber: seq,
		RootCas:        []*common.Certificate{{DerBytes: []byte(fmt.Sprintf("root-%d", seq))}},
	}
}

// TestPublishSuccess covers successful publishing rounds with one or more
// publishers, the coalescing of same/older notifications and periodic
// refreshes (no redundant writes), and publishing a newer bundle version.
func TestPublishSuccess(t *testing.T) {
	for _, tt := range []struct {
		name      string
		publisher func() *fakeBundlePublisher
	}{
		{
			name:      "one bundle publisher",
			publisher: func() *fakeBundlePublisher { return newFakeBundlePublisher("plugin-1") },
		},
		{
			name: "more than one bundle publisher",
			publisher: func() *fakeBundlePublisher {
				// second publisher is added by the caller via setup
				return newFakeBundlePublisher("plugin-1")
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			bp1 := newFakeBundlePublisher("plugin-1")
			publishers := []bundlepublisher.BundlePublisher{bp1}
			fakes := []*fakeBundlePublisher{bp1}
			if tt.name == "more than one bundle publisher" {
				bp2 := newFakeBundlePublisher("plugin-2")
				publishers = append(publishers, bp2)
				fakes = append(fakes, bp2)
			}

			test := setupTest(t, publishers)
			done := runManager(t, test)
			defer done()

			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()

			bundle1 := newTestBundle(t, 1)
			_, err := test.datastore.CreateBundle(ctx, bundle1)
			require.NoError(t, err)

			// First update: every publisher writes the bundle.
			test.m.BundleUpdated()
			test.waitForRound(t, ctx, len(publishers), "")
			for _, bp := range fakes {
				assertPublished(t, bp, 1)
			}

			// A repeated notification for the same version merges: no
			// redundant writes.
			test.m.BundleUpdated()
			test.waitForRound(t, ctx, 0, "")
			for _, bp := range fakes {
				assertPublished(t, bp, 1)
			}

			// A periodic refresh while caught up also produces no writes.
			test.clockHook.Add(refreshInterval + time.Millisecond)
			test.waitForRound(t, ctx, 0, "")
			for _, bp := range fakes {
				assertPublished(t, bp, 1)
			}

			// A newer version is published once per publisher.
			bundle2 := newTestBundle(t, 2)
			_, err = test.datastore.AppendBundle(ctx, bundle2)
			require.NoError(t, err)

			test.m.BundleUpdated()
			test.waitForRound(t, ctx, len(publishers), "")
			for _, bp := range fakes {
				assertPublished(t, bp, 1, 2)
			}

			// Caught up again.
			test.m.BundleUpdated()
			test.waitForRound(t, ctx, 0, "")
			for _, bp := range fakes {
				assertPublished(t, bp, 1, 2)
			}
		})
	}
}

// TestPublishFailure verifies that a failed publisher keeps the exact bundle
// version it failed on and retries it, while successful publishers are never
// asked to write again. After the failed publisher recovers and confirms the
// old version, it converges to newer versions.
func TestPublishFailure(t *testing.T) {
	bp1 := newFakeBundlePublisher("plugin-1")
	bp1.setErr(errors.New("error publishing bundle"))
	bp2 := newFakeBundlePublisher("plugin-2")

	test := setupTest(t, []bundlepublisher.BundlePublisher{bp1, bp2})
	done := runManager(t, test)
	defer done()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	bundle1 := newTestBundle(t, 1)
	_, err := test.datastore.CreateBundle(ctx, bundle1)
	require.NoError(t, err)

	// First round: bp1 fails and keeps v1 pending, bp2 succeeds.
	test.m.BundleUpdated()
	results := test.collectResults(t, ctx, 2)
	require.EqualError(t, results["plugin-1"].err, "error publishing bundle")
	require.NoError(t, results["plugin-2"].err)
	test.waitForFinished(t, ctx, "")
	assertPublished(t, bp1, 1)
	assertPublished(t, bp2, 1)

	// Second round: bp1 retries the original v1 (still failing), bp2 is not
	// called.
	test.m.BundleUpdated()
	results = test.collectResults(t, ctx, 1)
	require.EqualError(t, results["plugin-1"].err, "error publishing bundle")
	test.waitForFinished(t, ctx, "")
	assertPublished(t, bp1, 1, 1)
	assertPublished(t, bp2, 1)

	// bp1 recovers. It retries v1 and confirms it; bp2 remains untouched.
	bp1.setErr(nil)
	test.m.BundleUpdated()
	results = test.collectResults(t, ctx, 1)
	require.NoError(t, results["plugin-1"].err)
	test.waitForFinished(t, ctx, "")
	assertPublished(t, bp1, 1, 1, 1)
	assertPublished(t, bp2, 1)

	// A newer version arrives: both publishers publish v2.
	bundle2 := newTestBundle(t, 2)
	_, err = test.datastore.AppendBundle(ctx, bundle2)
	require.NoError(t, err)

	test.m.BundleUpdated()
	results = test.collectResults(t, ctx, 2)
	require.NoError(t, results["plugin-1"].err)
	require.NoError(t, results["plugin-2"].err)
	test.waitForFinished(t, ctx, "")
	assertPublished(t, bp1, 1, 1, 1, 2)
	assertPublished(t, bp2, 1, 2)
}

// TestOrderedConvergence verifies that when a newer version arrives while an
// older one is still pending for a failed publisher, the publisher applies the
// versions in order (old then new) within a single round once recovered, and
// never skips to the newer version while the old one is failing.
func TestOrderedConvergence(t *testing.T) {
	bp1 := newFakeBundlePublisher("plugin-1")
	bp1.setErr(errors.New("error publishing bundle"))

	test := setupTest(t, []bundlepublisher.BundlePublisher{bp1})
	done := runManager(t, test)
	defer done()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	bundle1 := newTestBundle(t, 1)
	_, err := test.datastore.CreateBundle(ctx, bundle1)
	require.NoError(t, err)

	test.m.BundleUpdated()
	results := test.collectResults(t, ctx, 1)
	require.Error(t, results["plugin-1"].err)
	test.waitForFinished(t, ctx, "")
	assertPublished(t, bp1, 1)

	// A newer version arrives while v1 is still pending. The publisher keeps
	// retrying v1 and does not see v2.
	bundle2 := newTestBundle(t, 2)
	_, err = test.datastore.AppendBundle(ctx, bundle2)
	require.NoError(t, err)

	test.m.BundleUpdated()
	results = test.collectResults(t, ctx, 1)
	require.Error(t, results["plugin-1"].err)
	require.Equal(t, uint64(1), results["plugin-1"].bundle.SequenceNumber)
	test.waitForFinished(t, ctx, "")
	assertPublished(t, bp1, 1, 1)

	// Recovered: the publisher catches up in order, v1 then v2, within the
	// same round.
	bp1.setErr(nil)
	test.m.BundleUpdated()
	results = test.collectResults(t, ctx, 2)
	require.NoError(t, results["plugin-1"].err)
	test.waitForFinished(t, ctx, "")
	assertPublished(t, bp1, 1, 1, 1, 2)
}

// TestNoPublishers verifies that a manager with no bundle publishers performs
// no work and never touches the datastore.
func TestNoPublishers(t *testing.T) {
	test := setupTest(t, nil)
	done := runManager(t, test)
	defer done()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	bundle1 := newTestBundle(t, 1)
	_, err := test.datastore.CreateBundle(ctx, bundle1)
	require.NoError(t, err)

	test.m.BundleUpdated()
	test.waitForRound(t, ctx, 0, "")

	test.clockHook.Add(refreshInterval + time.Millisecond)
	test.waitForRound(t, ctx, 0, "")
}

// TestDatastoreFetchError verifies that a failure to fetch the current bundle
// fails the round (so it is retried on the next notification or tick) without
// losing any progress.
func TestDatastoreFetchError(t *testing.T) {
	bp1 := newFakeBundlePublisher("plugin-1")

	test := setupTest(t, []bundlepublisher.BundlePublisher{bp1})
	done := runManager(t, test)
	defer done()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	bundle1 := newTestBundle(t, 1)
	_, err := test.datastore.CreateBundle(ctx, bundle1)
	require.NoError(t, err)

	// First round succeeds.
	test.m.BundleUpdated()
	test.waitForRound(t, ctx, 1, "")
	assertPublished(t, bp1, 1)

	// The datastore becomes temporarily unreadable. The round fails and no
	// publisher is called.
	test.datastore.SetNextError(errors.New("error in datastore"))
	test.m.BundleUpdated()
	test.waitForFinished(t, ctx, "failed to fetch bundle from datastore: error in datastore")
	assertPublished(t, bp1, 1)

	// Once readable again, the caught-up publisher is not rewritten.
	test.m.BundleUpdated()
	test.waitForRound(t, ctx, 0, "")
	assertPublished(t, bp1, 1)
}

// TestBundleDoesNotExistYet verifies behavior when the local bundle does not
// exist at the time of a notification: no error and no publish; after the
// bundle appears, a notification publishes it.
func TestBundleDoesNotExistYet(t *testing.T) {
	bp1 := newFakeBundlePublisher("plugin-1")

	test := setupTest(t, []bundlepublisher.BundlePublisher{bp1})
	done := runManager(t, test)
	defer done()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	test.m.BundleUpdated()
	test.waitForRound(t, ctx, 0, "")
	assertPublished(t, bp1)

	bundle1 := newTestBundle(t, 1)
	_, err := test.datastore.CreateBundle(ctx, bundle1)
	require.NoError(t, err)

	test.m.BundleUpdated()
	test.waitForRound(t, ctx, 1, "")
	assertPublished(t, bp1, 1)
}

// TestInitializeError verifies that a failure to load persisted statuses is
// surfaced and retried: no publishing happens until the datastore recovers,
// after which a normal round proceeds.
func TestInitializeError(t *testing.T) {
	bp1 := newFakeBundlePublisher("plugin-1")

	test := setupTest(t, []bundlepublisher.BundlePublisher{bp1})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Run starts while the datastore is unreadable. The initialization
	// attempt fails.
	test.datastore.SetNextError(errors.New("error in datastore"))
	done := runManager(t, test)
	defer done()

	require.Eventually(t, func() bool {
		for _, entry := range test.logHook.AllEntries() {
			if entry.Message == "Failed to initialize bundle publishing state" {
				return true
			}
		}
		return false
	}, 5*time.Second, 10*time.Millisecond)

	// Nothing was published.
	assertPublished(t, bp1)

	bundle1 := newTestBundle(t, 1)
	_, err := test.datastore.CreateBundle(ctx, bundle1)
	require.NoError(t, err)

	// The next notification retries initialization and then publishes.
	test.m.BundleUpdated()
	test.waitForRound(t, ctx, 1, "")
	assertPublished(t, bp1, 1)
}

// TestPluginTimeout simulates a plugin call that times out. The timed-out
// version becomes the pending version and is retried once the plugin
// responds again.
func TestPluginTimeout(t *testing.T) {
	bp1 := newFakeBundlePublisher("plugin-1")
	release := bp1.block()

	test := setupTest(t, []bundlepublisher.BundlePublisher{bp1})

	bundle1 := newTestBundle(t, 1)
	_, err := test.datastore.CreateBundle(context.Background(), bundle1)
	require.NoError(t, err)

	// The plugin call exceeds its deadline.
	timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer timeoutCancel()

	roundDone := make(chan error, 1)
	go func() {
		roundDone <- test.m.publishBundle(timeoutCtx)
	}()

	select {
	case err := <-roundDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("round did not finish after plugin timeout")
	}
	assertPublished(t, bp1, 1)

	// The plugin responds again. The same, timed-out version is retried.
	close(release)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, test.m.publishBundle(ctx))
	assertPublished(t, bp1, 1, 1)
}

// TestRestartRecovery verifies recovery after the service exits with partial
// confirmations: a new manager continues from the stored bundle and persisted
// publishing progress, skipping publishers that had confirmed and retrying the
// unconfirmed publisher, after which all publishers converge to newer
// versions.
func TestRestartRecovery(t *testing.T) {
	ds := fakedatastore.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	bundle1 := newTestBundle(t, 1)
	_, err := ds.CreateBundle(ctx, bundle1)
	require.NoError(t, err)

	// First instance: bp1 confirms v1, bp2 fails.
	bp1 := newFakeBundlePublisher("plugin-1")
	bp2 := newFakeBundlePublisher("plugin-2")
	bp2.setErr(errors.New("error publishing bundle"))

	first := setupTestWithDataStore(t, []bundlepublisher.BundlePublisher{bp1, bp2}, ds)
	firstDone := runManager(t, first)

	first.m.BundleUpdated()
	results := first.collectResults(t, ctx, 2)
	require.NoError(t, results["plugin-1"].err)
	require.Error(t, results["plugin-2"].err)
	first.waitForFinished(t, ctx, "")
	assertPublished(t, bp1, 1)
	assertPublished(t, bp2, 1)

	// The service exits after the partial confirmation.
	firstDone()

	// Second instance with fresh plugin instances (as after a restart). bp2
	// is healthy now.
	newBP1 := newFakeBundlePublisher("plugin-1")
	newBP2 := newFakeBundlePublisher("plugin-2")
	second := setupTestWithDataStore(t, []bundlepublisher.BundlePublisher{newBP1, newBP2}, ds)
	secondDone := runManager(t, second)
	defer secondDone()

	// First restart round against v1: bp1 (confirmed) is not called, bp2
	// publishes v1.
	second.m.BundleUpdated()
	second.waitForRound(t, ctx, 1, "")
	assertPublished(t, newBP1)
	assertPublished(t, newBP2, 1)

	// Redundant notification: both caught up, no writes.
	second.m.BundleUpdated()
	second.waitForRound(t, ctx, 0, "")
	assertPublished(t, newBP1)
	assertPublished(t, newBP2, 1)

	// A newer version: both converge to v2.
	bundle2 := newTestBundle(t, 2)
	_, err = ds.AppendBundle(ctx, bundle2)
	require.NoError(t, err)

	second.m.BundleUpdated()
	second.waitForRound(t, ctx, 2, "")
	assertPublished(t, newBP1, 2)
	assertPublished(t, newBP2, 1, 2)
}

type managerTest struct {
	clockHook *clock.Mock
	datastore *fakedatastore.DataStore
	logHook   *test.Hook
	m         *Manager
}

// collectResults consumes the given number of publish result events and
// returns them keyed by plugin name.
func (test *managerTest) collectResults(t *testing.T, ctx context.Context, count int) map[string]*publishResult {
	results := make(map[string]*publishResult, count)
	for range count {
		select {
		case result := <-test.m.hooks.publishResultCh:
			results[result.pluginName] = result
		case <-ctx.Done():
			t.Fatalf("context finished waiting for publish result: %v", ctx.Err())
		}
	}
	return results
}

// waitForRound waits for the given number of publish results and the round
// completion, asserting the overall error (empty means no error).
func (test *managerTest) waitForRound(t *testing.T, ctx context.Context, resultCount int, expectedErr string) {
	test.collectResults(t, ctx, resultCount)
	test.waitForFinished(t, ctx, expectedErr)
}

func (test *managerTest) waitForFinished(t *testing.T, ctx context.Context, expectedErr string) {
	select {
	case err := <-test.m.hooks.publishedCh:
		if expectedErr != "" {
			require.EqualError(t, err, expectedErr)
			return
		}
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatalf("context finished waiting for publish completion: %v", ctx.Err())
	}
}

type fakeBundlePublisher struct {
	bundlepublisher.BundlePublisher

	mu         sync.Mutex
	pluginName string
	pluginType string
	err        error
	publishes  []*common.Bundle
	blocking   chan struct{}
}

func newFakeBundlePublisher(name string) *fakeBundlePublisher {
	return &fakeBundlePublisher{
		pluginName: name,
		pluginType: "fake_type",
	}
}

// setErr configures the error returned by subsequent PublishBundle calls.
func (p *fakeBundlePublisher) setErr(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.err = err
}

// block makes subsequent PublishBundle calls block until the returned channel
// is closed, unless the call context is canceled first (simulating a timeout).
func (p *fakeBundlePublisher) block() chan struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.blocking = make(chan struct{})
	return p.blocking
}

// PublishBundle is a fake implementation for the PublishBundle method.
func (p *fakeBundlePublisher) PublishBundle(ctx context.Context, b *common.Bundle) error {
	p.mu.Lock()
	p.publishes = append(p.publishes, b)
	err := p.err
	blocking := p.blocking
	p.mu.Unlock()

	if blocking != nil {
		select {
		case <-blocking:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return err
}

// Name returns the plugin name of the fake bundle publisher plugin.
func (p *fakeBundlePublisher) Name() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pluginName
}

// Type returns the plugin type of the fake bundle publisher plugin.
func (p *fakeBundlePublisher) Type() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pluginType
}

// publishedSeqs returns the sequence numbers of the bundles received, in order.
func (p *fakeBundlePublisher) publishedSeqs() []uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	var seqs []uint64
	for _, b := range p.publishes {
		seqs = append(seqs, b.SequenceNumber)
	}
	return seqs
}

func assertPublished(t *testing.T, p *fakeBundlePublisher, expectedSeqs ...uint64) {
	t.Helper()
	var expected []uint64
	if len(expectedSeqs) > 0 {
		expected = expectedSeqs
	}
	assert.Equal(t, expected, p.publishedSeqs())
}

func setupTest(t *testing.T, bundlePublishers []bundlepublisher.BundlePublisher) *managerTest {
	ds := fakedatastore.New(t)
	return setupTestWithDataStore(t, bundlePublishers, ds)
}

func setupTestWithDataStore(t *testing.T, bundlePublishers []bundlepublisher.BundlePublisher, ds *fakedatastore.DataStore) *managerTest {
	log, logHook := test.NewNullLogger()
	log.Level = logrus.DebugLevel

	clockHook := clock.NewMock(t)
	m, err := newManager(&ManagerConfig{
		BundlePublishers: bundlePublishers,
		DataStore:        ds,
		Clock:            clockHook,
		Log:              log,
		TrustDomain:      td,
	})
	require.NoError(t, err)

	m.hooks.publishResultCh = make(chan *publishResult, 100)
	m.hooks.publishedCh = make(chan error, 10)
	return &managerTest{
		clockHook: clockHook,
		datastore: ds,
		logHook:   logHook,
		m:         m,
	}
}

func runManager(t *testing.T, test *managerTest) (done func()) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_ = test.m.Run(ctx)
	}()
	return func() {
		cancel()
	}
}
