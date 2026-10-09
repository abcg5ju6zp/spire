// Package pubmanager manages the publishing of the trust bundle to external
// stores through the configured BundlePublisher plugins.
package pubmanager

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/andres-erbsen/clock"
	"github.com/sirupsen/logrus"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/spire/pkg/server/datastore"
	"github.com/spiffe/spire/pkg/server/plugin/bundlepublisher"
	"github.com/spiffe/spire/proto/spire/common"
	"google.golang.org/protobuf/proto"
)

const (
	// refreshInterval is the interval to check for an updated trust bundle.
	refreshInterval = 30 * time.Second
)

// NewManager creates a new bundle publishing manager.
func NewManager(c *ManagerConfig) (*Manager, error) {
	return newManager(c)
}

// ManagerConfig is the config for the bundle publishing manager.
type ManagerConfig struct {
	BundlePublishers []bundlepublisher.BundlePublisher
	DataStore        datastore.DataStore
	Clock            clock.Clock
	Log              logrus.FieldLogger
	TrustDomain      spiffeid.TrustDomain
}

// Manager is the manager for bundle publishing. It implements the PubManager
// interface. Publishing progress is tracked independently per bundle publisher:
// a failed publisher retries the exact bundle version it failed on before
// attempting a newer one, while a publisher that already confirmed a version
// is never asked to write it again. Confirmations are persisted in the
// datastore so that progress can be recovered after a restart.
type Manager struct {
	bundleUpdatedCh  chan struct{}
	bundlePublishers []bundlepublisher.BundlePublisher
	clock            clock.Clock
	dataStore        datastore.DataStore
	log              logrus.FieldLogger
	trustDomain      spiffeid.TrustDomain

	mtx    sync.Mutex
	states map[string]*publisherState

	// latestBundle is the most recent bundle successfully fetched from the
	// datastore. It may be nil if the datastore holds no bundle yet.
	latestBundle *common.Bundle

	// initialized is true once the persisted publishing statuses have been
	// loaded.
	initialized bool

	hooks struct {
		// Test hook used to indicate an attempt to publish a bundle using a
		// specific bundle publisher.
		publishResultCh chan *publishResult

		// Test hook used to indicate when the action of publishing a bundle
		// has finished.
		publishedCh chan error
	}
}

// publisherState holds the independent publishing progress of a single bundle
// publisher.
type publisherState struct {
	mu sync.Mutex

	bp   bundlepublisher.BundlePublisher
	name string

	// confirmed records whether any bundle version has ever been confirmed,
	// either loaded from a persisted status or produced by a successful
	// publish in this process. It is authoritative independently of the
	// sequence number value so that a confirmation at sequence zero is not
	// confused with the absence of a confirmation.
	confirmed bool

	// confirmedSeq is the sequence number of the last bundle version
	// confirmed published by this publisher. Zero means that no version has
	// been confirmed yet.
	confirmedSeq uint64

	// confirmedBundle is the content of the last bundle confirmed published.
	// It is used, in addition to the sequence number, to tell whether the
	// publisher is caught up with the current bundle. It is not available
	// when the confirmation was loaded from a persisted status after a
	// restart, in which case the sequence number is authoritative.
	confirmedBundle *common.Bundle

	// pending, when non-nil, is the exact bundle version that a previous
	// publish attempt failed on. It is retried before any newer version is
	// attempted so that versions are never skipped.
	pending *common.Bundle

	// dirty indicates that the in-memory confirmation could not be persisted
	// in a previous round and needs to be persisted again.
	dirty bool
}

// Run runs the bundle publishing manager.
func (m *Manager) Run(ctx context.Context) error {
	ticker := m.clock.Ticker(refreshInterval)
	defer ticker.Stop()

	for {
		// Load the persisted publishing statuses before waiting for work. If
		// the datastore is temporarily unavailable, the error is logged and
		// the load is retried on the next tick or update notification; the
		// manager never treats an unloaded state as ready.
		if err := m.initialize(ctx); err != nil && ctx.Err() == nil {
			m.log.WithError(err).Error("Failed to initialize bundle publishing state")
		}

		select {
		case <-ticker.C:
			m.callPublishBundle(ctx)
		case <-m.bundleUpdatedCh:
			m.callPublishBundle(ctx)
		case <-ctx.Done():
			return nil
		}
	}
}

// BundleUpdated tells the bundle publishing manager that the bundle has been
// updated. Notifications are coalesced: a notification that arrives while the
// manager is already working only merges into the pending round when it
// represents the same or an older bundle version. A strictly newer version
// always forces another round, because every round re-fetches the current
// bundle from the datastore and the per-publisher state decides what still
// needs writing.
func (m *Manager) BundleUpdated() {
	m.drainBundleUpdated()
	m.bundleUpdatedCh <- struct{}{}
}

// initialize loads the persisted bundle publishing statuses and builds the
// per-publisher state. It is a no-op once the state has been loaded.
func (m *Manager) initialize(ctx context.Context) error {
	m.mtx.Lock()
	if m.initialized {
		m.mtx.Unlock()
		return nil
	}
	m.mtx.Unlock()

	// With no publishers there is nothing to track. Stay compatible with the
	// previous behavior of never touching the datastore.
	if len(m.bundlePublishers) == 0 {
		m.mtx.Lock()
		m.states = make(map[string]*publisherState)
		m.initialized = true
		m.mtx.Unlock()
		return nil
	}

	statuses, err := m.dataStore.FetchBundlePublishStatuses(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch bundle publish statuses from datastore: %w", err)
	}

	m.mtx.Lock()
	defer m.mtx.Unlock()
	if m.initialized {
		return nil
	}

	states := make(map[string]*publisherState, len(m.bundlePublishers))
	for _, bp := range m.bundlePublishers {
		state := &publisherState{
			bp:   bp,
			name: bp.Name(),
		}
		for _, status := range statuses {
			if status.TrustDomainID == m.trustDomain.IDString() && status.PluginName == bp.Name() {
				// The existence of a persisted status row is authoritative:
				// the bundle version recorded on it was confirmed published.
				state.confirmed = true
				state.confirmedSeq = status.BundleSequence
				break
			}
		}
		states[bp.Name()] = state
	}
	m.states = states
	m.initialized = true
	return nil
}

// callPublishBundle calls the publishBundle function and logs if there was an
// error.
func (m *Manager) callPublishBundle(ctx context.Context) {
	if err := m.publishBundle(ctx); err != nil && ctx.Err() == nil {
		m.log.WithError(err).Error("Failed to publish bundle")
	}
}

// publishBundle reconciles every configured bundle publisher with the current
// bundle. Each publisher's progress is independent: this function only returns
// an error when the publishers cannot be reconciled because the persisted
// state cannot be loaded or the current bundle cannot be fetched from the
// datastore. Per-publisher failures do not fail the whole round and do not
// affect the other publishers.
func (m *Manager) publishBundle(ctx context.Context) (err error) {
	defer func() {
		m.publishDone(err)
	}()

	if len(m.bundlePublishers) == 0 {
		return nil
	}

	if err := m.initialize(ctx); err != nil {
		return err
	}

	bundle, err := m.dataStore.FetchBundle(ctx, m.trustDomain.IDString())
	if err != nil {
		return fmt.Errorf("failed to fetch bundle from datastore: %w", err)
	}
	if bundle == nil {
		// The local bundle does not exist yet. There is nothing to publish;
		// the next tick or update notification triggers a new attempt.
		m.log.Debug("Bundle does not exist yet")
		return nil
	}

	m.mtx.Lock()
	m.latestBundle = bundle
	states := make([]*publisherState, 0, len(m.states))
	for _, state := range m.states {
		states = append(states, state)
	}
	m.mtx.Unlock()

	var wg sync.WaitGroup
	for _, state := range states {
		wg.Go(func() {
			m.publishForPublisher(ctx, state, bundle)
		})
	}

	wg.Wait()
	return nil
}

// publishForPublisher drives a single publisher to convergence against the
// current bundle. Versions are applied in order: a previously failed version
// is retried first, and after a success the loop continues with the current
// version until the publisher is caught up.
func (m *Manager) publishForPublisher(ctx context.Context, state *publisherState, latest *common.Bundle) {
	log := m.log.WithField(state.bp.Type(), state.name)

	// Retry persisting a confirmation that the datastore could not store in
	// a previous round. If the datastore is still unavailable, stop here and
	// try again on the next round; the external bundle version is unchanged.
	if state.isDirty() {
		if err := m.persistState(ctx, state); err != nil && ctx.Err() == nil {
			log.WithError(err).Error("Failed to persist bundle publish status")
			return
		}
	}

	for {
		bundleToPublish := state.nextBundle(latest)
		if bundleToPublish == nil {
			// Already confirmed the current version. No redundant write.
			return
		}

		err := state.bp.PublishBundle(ctx, bundleToPublish)

		m.triggerPublishResultHook(&publishResult{
			pluginName: state.name,
			bundle:     bundleToPublish,
			err:        err,
		})

		if err != nil {
			// Remember the exact failed version so it is retried before any
			// newer version is attempted. Other publishers are unaffected.
			log.WithError(err).Error("Failed to publish bundle")
			state.recordFailure(bundleToPublish)
			return
		}

		state.recordSuccess(bundleToPublish)

		// Persist the independent confirmation. If persistence fails the
		// in-memory progress is kept and retried; if the service exits
		// before the confirmation is stored, a restart can at worst cause a
		// redundant, idempotent publish of the same version.
		if err := m.persistState(ctx, state); err != nil && ctx.Err() == nil {
			log.WithError(err).Error("Failed to persist bundle publish status")
		}
	}
}

// nextBundle returns the bundle version that still needs publishing, or nil if
// the publisher is already caught up. A pending failed version always takes
// precedence; otherwise the current bundle is selected only when it is newer
// than the confirmed version.
func (s *publisherState) nextBundle(latest *common.Bundle) *common.Bundle {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.pending != nil {
		return s.pending
	}

	if !s.confirmed {
		// No version has ever been confirmed. The current bundle needs a
		// first publish even if its sequence number is zero.
		return latest
	}

	switch {
	case latest.SequenceNumber < s.confirmedSeq:
		// The current bundle is older than what was already confirmed. Do
		// not publish stale content.
		return nil
	case latest.SequenceNumber > s.confirmedSeq:
		return latest
	case s.confirmedBundle != nil && !proto.Equal(s.confirmedBundle, latest):
		// Same sequence number but different content. The version was
		// rewritten in place and still needs publishing.
		return latest
	default:
		// Already caught up: same or older version with known content. No
		// redundant write, including when the content is unknown because the
		// confirmation was loaded from a persisted status.
		return nil
	}
}

// recordSuccess marks the given bundle as confirmed published and clears any
// pending version.
func (s *publisherState) recordSuccess(bundle *common.Bundle) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.confirmed = true
	s.confirmedSeq = bundle.SequenceNumber
	s.confirmedBundle = bundle
	s.pending = nil
}

// recordFailure keeps the given bundle as the pending version to retry.
func (s *publisherState) recordFailure(bundle *common.Bundle) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pending = bundle
}

func (s *publisherState) isDirty() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dirty
}

// persistState stores the confirmation of the publisher in the datastore.
func (m *Manager) persistState(ctx context.Context, state *publisherState) error {
	state.mu.Lock()
	confirmedSeq := state.confirmedSeq
	state.dirty = false
	state.mu.Unlock()

	_, err := m.dataStore.SetBundlePublishStatus(ctx, &datastore.BundlePublishStatus{
		TrustDomainID:  m.trustDomain.IDString(),
		PluginName:     state.name,
		BundleSequence: confirmedSeq,
	})
	if err != nil {
		state.mu.Lock()
		state.dirty = true
		state.mu.Unlock()
		return err
	}
	return nil
}

// triggerPublishResultHook is called to know when the publish action using a
// specific bundle publisher has happened. It informs the result of calling the
// PublishBundle method to a bundle publisher.
func (m *Manager) triggerPublishResultHook(result *publishResult) {
	if m.hooks.publishResultCh != nil {
		m.hooks.publishResultCh <- result
	}
}

// publishDone is called to know when a publish action has finished and informs
// if there was an error in the overall action (not specific to a bundle
// publisher). A publish action happens periodically (every refreshInterval) and
// also when BundleUpdated() is called.
func (m *Manager) publishDone(err error) {
	if m.hooks.publishedCh != nil {
		m.hooks.publishedCh <- err
	}
}

// publishResult holds information about the result of trying to publish a
// bundle using a specific bundle publisher.
type publishResult struct {
	pluginName string
	bundle     *common.Bundle
	err        error
}

func (m *Manager) drainBundleUpdated() {
	select {
	case <-m.bundleUpdatedCh:
	default:
	}
}

func newManager(c *ManagerConfig) (*Manager, error) {
	if c.DataStore == nil {
		return nil, errors.New("missing datastore")
	}

	if c.TrustDomain.IsZero() {
		return nil, errors.New("missing trust domain")
	}

	if c.Clock == nil {
		c.Clock = clock.New()
	}

	return &Manager{
		bundleUpdatedCh:  make(chan struct{}, 1),
		bundlePublishers: c.BundlePublishers,
		clock:            c.Clock,
		dataStore:        c.DataStore,
		log:              c.Log,
		trustDomain:      c.TrustDomain,
		states:           make(map[string]*publisherState),
	}, nil
}
