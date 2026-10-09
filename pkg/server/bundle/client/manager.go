package client

import (
	"context"
	"errors"
	"maps"
	"sync"
	"time"

	"github.com/andres-erbsen/clock"
	"github.com/sirupsen/logrus"
	"github.com/spiffe/go-spiffe/v2/bundle/spiffebundle"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/spire/pkg/common/bundleutil"
	"github.com/spiffe/spire/pkg/common/telemetry"
	telemetry_server "github.com/spiffe/spire/pkg/common/telemetry/server"
	"github.com/spiffe/spire/pkg/server/datastore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// attemptsPerRefreshHint is the number of attempts within the returned
	// refresh hint period that the manager will attempt to refresh the
	// bundle. It is important to try more than once within a refresh hint
	// period so we can be resilient to temporary downtime or failures.
	attemptsPerRefreshHint = 4

	// configRefreshInterval is how often the manager reloads trust domain
	// configs from the source and reconciles it against the current bundle
	// updaters.
	configRefreshInterval = time.Second * 10

	// defaultRefreshInterval is how often the manager reloads the trust bundle
	// for a trust domain if that trust domain does not specify a refresh hint in
	// its current trust bundle.
	defaultRefreshInterval = time.Minute * 5

	// defaultStopTimeout is how long the manager waits for a superseded
	// updater generation to come to a stop before moving on. All datastore
	// writes performed by a stopped generation are rejected, so waiting
	// only bounds bookkeeping; it is not required for correctness.
	defaultStopTimeout = time.Second * 10
)

type TrustDomainConfig struct {
	// EndpointURL is the URL used to fetch the bundle of the federated
	// trust domain. Is served by a SPIFFE bundle endpoint server.
	EndpointURL string

	// EndpointProfile is the bundle endpoint profile used by the
	// SPIFFE bundle endpoint server.
	EndpointProfile EndpointProfileInfo

	// BootstrapBundlePath is an optional path to a bundle used to authenticate
	// the first https_spiffe fetch when the datastore has no copy of the
	// federated bundle. It is not used after that fetch succeeds.
	BootstrapBundlePath string

	// BootstrapBundleFormat is the format of the file at BootstrapBundlePath.
	// "pem" (default) or "spiffe".
	BootstrapBundleFormat string
}

type EndpointProfileInfo interface {
	// The name of the endpoint profile (e.g. "https_spiffe").
	Name() string
}

type HTTPSWebProfile struct{}

func (p HTTPSWebProfile) Name() string {
	return "https_web"
}

type HTTPSSPIFFEProfile struct {
	// EndpointSPIFFEID is the expected SPIFFE ID of the bundle endpoint server.
	EndpointSPIFFEID spiffeid.ID
}

func (p HTTPSSPIFFEProfile) Name() string {
	return "https_spiffe"
}

type ManagerConfig struct {
	Log       logrus.FieldLogger
	Metrics   telemetry.Metrics
	DataStore datastore.DataStore
	Clock     clock.Clock
	Source    TrustDomainConfigSource

	// newBundleUpdater is a test hook to inject updater behavior
	newBundleUpdater func(BundleUpdaterConfig) BundleUpdater

	// configRefreshedCh is a test hook to learn when the trust domain config
	// has been refreshed and be apprised of the next scheduled refresh.
	configRefreshedCh chan time.Duration

	// bundleRefreshedCh is a test hook to learn when a bundle has been
	// refreshed and be apprised of the next scheduled refresh.
	bundleRefreshedCh chan time.Duration

	// stopTimeout is a test hook to control how long the manager waits for
	// superseded updater generations to stop.
	stopTimeout time.Duration
}

type Manager struct {
	log              logrus.FieldLogger
	metrics          telemetry.Metrics
	clock            clock.Clock
	ds               datastore.DataStore
	source           TrustDomainConfigSource
	stopTimeout      time.Duration
	configRefreshCh  chan struct{}
	configRefreshMtx sync.Mutex
	updatersMtx      sync.RWMutex
	updaters         map[spiffeid.TrustDomain]*managedTrustDomain

	// test hooks
	newBundleUpdater  func(BundleUpdaterConfig) BundleUpdater
	configRefreshedCh chan time.Duration
	bundleRefreshedCh chan time.Duration
}

// bundleUpdateResult is the outcome of a single bundle update attempt.
type bundleUpdateResult struct {
	localBundle    *spiffebundle.Bundle
	endpointBundle *spiffebundle.Bundle
	err            error
}

// managedBundleUpdater is a single configuration generation of a bundle
// updater. The generation is bound to one immutable TrustDomainConfig and
// uses one cancellation context. It starts as a candidate; once it has
// successfully connected, validated, and (when needed) persisted a bundle
// using that generation's configuration and authentication roots, it is
// promoted and takes over periodic refreshes. A generation that has been
// stopped can never persist a bundle again.
type managedBundleUpdater struct {
	BundleUpdater

	td     spiffeid.TrustDomain
	gen    uint64
	cfg    TrustDomainConfig
	ctx    context.Context
	cancel context.CancelFunc

	// updateMtx serializes periodic and on-demand update attempts so a
	// manual refresh never runs concurrently with the refresh loop.
	updateMtx sync.Mutex

	mtx      sync.Mutex
	stopped  bool
	promoted bool

	wg sync.WaitGroup
}

func (u *managedBundleUpdater) isStopped() bool {
	u.mtx.Lock()
	defer u.mtx.Unlock()
	return u.stopped
}

func (u *managedBundleUpdater) isStoppedLocked() bool {
	return u.stopped
}

// markStoppedLocked marks the generation as stopped and cancels its
// context. The caller must hold the owning managedTrustDomain lock, which
// serializes stopping with bundle persistence.
func (u *managedBundleUpdater) markStoppedLocked() {
	u.mtx.Lock()
	u.stopped = true
	u.mtx.Unlock()
	u.cancel()
}

// refreshNow runs an on-demand update for the generation. The update is
// serialized with the periodic refresh loop and is bound to both the
// caller context and the generation context, so an update requested on a
// stopped generation can never persist a bundle.
func (u *managedBundleUpdater) refreshNow(ctx context.Context) error {
	u.updateMtx.Lock()
	defer u.updateMtx.Unlock()

	if u.isStopped() {
		return errBundleGenerationStopped
	}

	updateCtx, cancel := context.WithCancel(u.ctx)
	defer cancel()
	stop := context.AfterFunc(ctx, cancel)
	defer stop()

	_, _, err := u.UpdateBundle(updateCtx)
	return err
}

// managedTrustDomain tracks the updater generations for one federated
// trust domain. The current generation owns periodic refreshes; the
// candidate generation, if any, is validating a new configuration and
// takes over atomically once its first update succeeds.
type managedTrustDomain struct {
	td  spiffeid.TrustDomain
	mtx sync.Mutex

	// genNum is the number of the most recently created generation.
	genNum uint64

	current   *managedBundleUpdater
	candidate *managedBundleUpdater
}

// reconcileHolder updates the holder to reflect the desired configuration.
// It returns:
//   - next: a candidate generation that must be started, if the desired
//     configuration is new or changed;
//   - stop: a generation that must be stopped (a superseded candidate);
//   - changed: whether the configuration of the promoted generation
//     changed.
func (m *Manager) reconcileHolder(ctx context.Context, holder *managedTrustDomain, cfg TrustDomainConfig) (next, stop *managedBundleUpdater, changed bool) {
	holder.mtx.Lock()
	defer holder.mtx.Unlock()

	// The desired configuration matches the promoted generation. Any
	// pending candidate is obsolete (e.g. the configuration changed back
	// before the candidate was promoted).
	if holder.current != nil && holder.current.cfg == cfg {
		if holder.candidate != nil {
			stop = holder.candidate
			stop.markStoppedLocked()
			holder.candidate = nil
		}
		return nil, stop, false
	}

	// A candidate is already validating this exact configuration.
	if holder.candidate != nil && holder.candidate.cfg == cfg {
		return nil, nil, false
	}

	// The desired configuration is new or changed: retire any pending
	// candidate and create a new generation for this configuration.
	if holder.candidate != nil {
		stop = holder.candidate
		stop.markStoppedLocked()
		holder.candidate = nil
	}
	holder.genNum++
	next = m.newGeneration(ctx, holder, holder.genNum, cfg)
	holder.candidate = next
	return next, stop, holder.current != nil
}

// teardown stops every generation of the trust domain and returns the
// generations that must be drained.
func (h *managedTrustDomain) teardown() []*managedBundleUpdater {
	h.mtx.Lock()
	defer h.mtx.Unlock()

	var stopped []*managedBundleUpdater
	if h.candidate != nil {
		h.candidate.markStoppedLocked()
		stopped = append(stopped, h.candidate)
		h.candidate = nil
	}
	if h.current != nil {
		h.current.markStoppedLocked()
		stopped = append(stopped, h.current)
		h.current = nil
	}
	return stopped
}

func NewManager(config ManagerConfig) *Manager {
	if config.Clock == nil {
		config.Clock = clock.New()
	}
	if config.newBundleUpdater == nil {
		config.newBundleUpdater = NewBundleUpdater
	}
	if config.stopTimeout == 0 {
		config.stopTimeout = defaultStopTimeout
	}

	return &Manager{
		log:               config.Log,
		metrics:           config.Metrics,
		clock:             config.Clock,
		ds:                config.DataStore,
		source:            config.Source,
		stopTimeout:       config.stopTimeout,
		newBundleUpdater:  config.newBundleUpdater,
		configRefreshCh:   make(chan struct{}, 1),
		configRefreshedCh: config.configRefreshedCh,
		bundleRefreshedCh: config.bundleRefreshedCh,
		updaters:          make(map[spiffeid.TrustDomain]*managedTrustDomain),
	}
}

func (m *Manager) Run(ctx context.Context) error {
	// Initialize the timer that will reload the configs. The initial duration
	// isn't very important since we'll reset it after the reload has
	// completed.
	timer := m.clock.Timer(configRefreshInterval)
	defer timer.Stop()

	for {
		if err := m.refreshConfigs(ctx); err != nil {
			m.log.WithError(err).Error("Failed to reload configs")
		}
		timer.Reset(configRefreshInterval)
		m.notifyConfigRefreshed(ctx, configRefreshInterval)
		select {
		case <-m.configRefreshCh:
		case <-timer.C:
		case <-ctx.Done():
			m.log.Info("Shutting down")
			return ctx.Err()
		}
	}
}

// TriggerConfigReload triggers the manager to reload the configuration
func (m *Manager) TriggerConfigReload() {
	select {
	case m.configRefreshCh <- struct{}{}:
	default:
	}
}

// RefreshBundleFor refreshes the trust domain bundle for the given trust
// domain. If the trust domain is not managed by the manager, false is returned.
// The update runs on the most recent generation for the trust domain: when a
// configuration change is being validated, the candidate generation is
// driven so that configuration deletion, endpoint switches, and manual
// refreshes all converge on the same generation.
func (m *Manager) RefreshBundleFor(ctx context.Context, td spiffeid.TrustDomain) (bool, error) {
	if err := m.refreshConfigs(ctx); err != nil {
		m.log.WithError(err).Error("Failed to reload configs")
	}

	m.updatersMtx.RLock()
	holder := m.updaters[td]
	m.updatersMtx.RUnlock()

	if holder == nil {
		return false, nil
	}

	holder.mtx.Lock()
	generation := holder.candidate
	if generation == nil {
		generation = holder.current
	}
	holder.mtx.Unlock()

	if generation == nil {
		return false, nil
	}

	err := generation.refreshNow(ctx)
	if errors.Is(err, errBundleGenerationStopped) {
		// The generation was superseded or the trust domain was removed
		// while the update was being requested. If the trust domain is
		// still managed, the next call converges on the new generation.
		m.updatersMtx.RLock()
		_, managed := m.updaters[td]
		m.updatersMtx.RUnlock()
		if !managed {
			return false, nil
		}
	}
	return true, err
}

func (m *Manager) refreshConfigs(ctx context.Context) error {
	m.configRefreshMtx.Lock()
	defer m.configRefreshMtx.Unlock()

	configs, err := m.source.GetTrustDomainConfigs(ctx)
	if err != nil {
		return err
	}

	// Duplicate the configs map since we're going to mutate it while figuring
	// out what needs to be started/updated/stopped.
	configs = cloneTrustDomainConfigs(configs)

	var toStop []*managedBundleUpdater
	defer func() {
		if len(toStop) == 0 {
			return
		}
		m.log.Debug("Stopping stale bundle updater generations")
		for _, generation := range toStop {
			m.drainUpdater(ctx, m.generationLog(generation.td, generation.gen), generation)
		}
		m.log.Debug("Done stopping stale bundle updater generations")
	}()

	m.updatersMtx.Lock()
	defer m.updatersMtx.Unlock()

	for td, holder := range m.updaters {
		tdLog := m.log.WithField(telemetry.Entry, td)
		if config, ok := configs[td]; ok {
			// Updater still needed. Reconcile the desired configuration
			// against the current and candidate generations, then remove the
			// config from the configs list so a new updater isn't started.
			next, stop, changed := m.reconcileHolder(ctx, holder, config)
			if stop != nil {
				toStop = append(toStop, stop)
			}
			if next != nil {
				if changed {
					tdLog.WithFields(logrus.Fields{
						telemetry.BundleEndpointURL:     config.EndpointURL,
						telemetry.BundleEndpointProfile: config.EndpointProfile.Name(),
					}).Info("Updated configuration for managed trust domain")
				}
				m.startGeneration(next)
			}
			delete(configs, td)
		} else {
			// Trust domain no longer managed. Stop every generation and
			// remove it from the updaters list.
			tdLog.Info("Trust domain no longer managed")
			toStop = append(toStop, holder.teardown()...)
			delete(m.updaters, td)
		}
	}

	// The remaining configs are for newly managed trust domains. Create and
	// start a candidate updater for each of them.
	for td, config := range configs {
		m.log.WithFields(logrus.Fields{
			telemetry.BundleEndpointURL:     config.EndpointURL,
			telemetry.BundleEndpointProfile: config.EndpointProfile.Name(),
			telemetry.TrustDomain:           td,
		}).Info("Trust domain is now managed")
		holder := &managedTrustDomain{
			td:     td,
			genNum: 1,
		}
		generation := m.newGeneration(ctx, holder, 1, config)
		holder.candidate = generation
		m.updaters[td] = holder
		m.startGeneration(generation)
	}
	return nil
}

func (m *Manager) newGeneration(parent context.Context, holder *managedTrustDomain, genNum uint64, config TrustDomainConfig) *managedBundleUpdater {
	ctx, cancel := context.WithCancel(parent)
	generation := &managedBundleUpdater{
		td:     holder.td,
		gen:    genNum,
		cfg:    config,
		ctx:    ctx,
		cancel: cancel,
	}
	generation.BundleUpdater = m.newBundleUpdater(BundleUpdaterConfig{
		TrustDomainConfig: config,
		TrustDomain:       holder.td,
		DataStore:         m.ds,
		commitBundleHook: func(ctx context.Context, local, fetched *spiffebundle.Bundle, usedBootstrap bool) (*spiffebundle.Bundle, *spiffebundle.Bundle, error) {
			return m.commitFetchedBundle(ctx, holder, generation, local, fetched, usedBootstrap)
		},
	})
	return generation
}

func (m *Manager) startGeneration(generation *managedBundleUpdater) {
	generation.wg.Go(func() {
		m.runUpdater(generation)
	})
}

// commitFetchedBundle is the generation-scoped persistence hook used by
// managed updaters. It is serialized per trust domain with generation
// promotion and stopping, which guarantees:
//   - a candidate takes over atomically with the first bundle it
//     successfully validates and persists, and
//   - once a generation has been stopped it can never write to the
//     datastore, even if a slow download finishes afterwards.
func (m *Manager) commitFetchedBundle(ctx context.Context, holder *managedTrustDomain, generation *managedBundleUpdater, localFederatedBundleOrNil, fetchedFederatedBundle *spiffebundle.Bundle, usedBootstrap bool) (*spiffebundle.Bundle, *spiffebundle.Bundle, error) {
	holder.mtx.Lock()

	if generation.isStoppedLocked() || ctx.Err() != nil {
		holder.mtx.Unlock()
		return localFederatedBundleOrNil, nil, errBundleGenerationStopped
	}

	// The fetched bundle matches the local one: no write is required, but
	// the candidate has proven it can connect and validate with its
	// generation's configuration and authentication roots, so it can take
	// over.
	if localFederatedBundleOrNil != nil && fetchedFederatedBundle.Equal(localFederatedBundleOrNil) {
		previous := m.promoteLocked(holder, generation)
		holder.mtx.Unlock()
		m.drainPreviousGeneration(ctx, previous)
		return localFederatedBundleOrNil, nil, nil
	}

	bundle, err := bundleutil.SPIFFEBundleToProto(fetchedFederatedBundle)
	if err != nil {
		holder.mtx.Unlock()
		return localFederatedBundleOrNil, nil, err
	}

	var previous *managedBundleUpdater
	if usedBootstrap {
		_, err = m.ds.CreateBundle(ctx, bundle)
		if err != nil {
			if status.Code(err) == codes.AlreadyExists {
				// sqlstore maps any constraint violation to AlreadyExists.
				existing, fetchErr := fetchBundleIfExists(ctx, m.ds, holder.td)
				if fetchErr != nil {
					holder.mtx.Unlock()
					return localFederatedBundleOrNil, nil, fetchErr
				}
				if existing != nil {
					previous = m.promoteLocked(holder, generation)
					holder.mtx.Unlock()
					m.drainPreviousGeneration(ctx, previous)
					return existing, nil, nil
				}
			}
			holder.mtx.Unlock()
			return localFederatedBundleOrNil, nil, err
		}
	} else {
		if _, err = m.ds.SetBundle(ctx, bundle); err != nil {
			holder.mtx.Unlock()
			return localFederatedBundleOrNil, nil, err
		}
	}

	previous = m.promoteLocked(holder, generation)
	holder.mtx.Unlock()
	m.drainPreviousGeneration(ctx, previous)
	return localFederatedBundleOrNil, fetchedFederatedBundle, nil
}

// promoteLocked promotes the candidate generation. The caller must hold
// the holder lock. It returns the generation that was displaced and must
// be drained.
func (m *Manager) promoteLocked(holder *managedTrustDomain, generation *managedBundleUpdater) *managedBundleUpdater {
	if generation.isStoppedLocked() || holder.candidate != generation {
		return nil
	}
	previous := holder.current
	holder.current = generation
	holder.candidate = nil
	generation.promoted = true
	if previous != nil && previous != generation {
		previous.markStoppedLocked()
	}
	return previous
}

func (m *Manager) drainPreviousGeneration(ctx context.Context, previous *managedBundleUpdater) {
	if previous == nil {
		return
	}
	m.drainUpdater(ctx, m.generationLog(previous.td, previous.gen), previous)
}

// drainUpdater cancels (via a prior markStoppedLocked) and waits for a
// generation to stop, bounded by stopTimeout. Whether or not the
// generation stops in time, it is marked stopped and its writes are
// rejected, so it cannot overwrite confirmed bundles.
func (m *Manager) drainUpdater(ctx context.Context, log logrus.FieldLogger, generation *managedBundleUpdater) {
	done := make(chan struct{})
	go func() {
		generation.wg.Wait()
		close(done)
	}()

	// The stop timeout is a safety bound on bookkeeping, not on
	// correctness: a stopped generation is never able to persist bundles
	// regardless of whether its goroutine exits in time. Wall-clock time
	// is used on purpose so that a generation stuck in an uninterruptible
	// call does not delay reconciliation indefinitely.
	timer := time.NewTimer(m.stopTimeout)
	defer timer.Stop()

	select {
	case <-done:
		log.Debug("Bundle updater generation stopped")
	case <-timer.C:
		log.Warn("Timed out waiting for the bundle updater generation to stop; the stopped generation can no longer persist federated bundles")
	case <-ctx.Done():
		log.Debug("Interrupted while waiting for the bundle updater generation to stop")
	}
}

func (m *Manager) runUpdater(generation *managedBundleUpdater) {
	ctx := generation.ctx
	// Initialize the timer. The initial duration does not matter since it will
	// be reset with the actual refresh interval before first use.
	timer := m.clock.Timer(time.Hour)
	defer timer.Stop()

	log := m.generationLog(generation.td, generation.gen)

	for {
		_, nextRefresh := m.updateGenerationOnce(ctx, log, generation)
		if ctx.Err() != nil || generation.isStopped() {
			log.Debug("No longer polling for updates")
			return
		}

		log.WithFields(logrus.Fields{
			"at": m.clock.Now().Add(nextRefresh).UTC().Format(time.RFC3339),
		}).Debug("Scheduling next bundle refresh")

		// Notify the test hook
		timer.Reset(nextRefresh)
		m.notifyBundleRefreshed(ctx, nextRefresh)

		select {
		case <-timer.C:
		case <-ctx.Done():
			log.Info("No longer polling for updates")
			return
		}
	}
}

// updateGenerationOnce runs one update attempt, serializing periodic and
// on-demand refreshes for the generation.
func (m *Manager) updateGenerationOnce(ctx context.Context, log logrus.FieldLogger, generation *managedBundleUpdater) (bundleUpdateResult, time.Duration) {
	generation.updateMtx.Lock()
	defer generation.updateMtx.Unlock()

	if generation.isStopped() || ctx.Err() != nil {
		return bundleUpdateResult{err: errBundleGenerationStopped}, 0
	}
	return m.runUpdateOnce(ctx, log, generation.td, generation)
}

func (m *Manager) runUpdateOnce(ctx context.Context, log logrus.FieldLogger, trustDomain spiffeid.TrustDomain, updater BundleUpdater) (bundleUpdateResult, time.Duration) {
	log.Debug("Polling for bundle update")

	counter := telemetry_server.StartBundleManagerFetchFederatedBundleCall(m.metrics)
	counter.AddLabel(telemetry.TrustDomainID, trustDomain.Name())
	result := bundleUpdateResult{}
	var err error
	defer func() {
		counter.Done(&err)
	}()

	result.localBundle, result.endpointBundle, err = updater.UpdateBundle(ctx)
	result.err = err
	if err != nil {
		log.WithError(err).Error("Error updating bundle")
	}

	if result.endpointBundle != nil {
		telemetry_server.IncrBundleManagerUpdateFederatedBundleCounter(m.metrics, trustDomain.Name())
		log.Info("Bundle refreshed")

		return result, calculateNextUpdate(result.endpointBundle)
	}

	if result.localBundle != nil {
		return result, calculateNextUpdate(result.localBundle)
	}

	// We have no bundle to use to calculate the refresh hint. Since
	// the endpoint cannot be reached without the local bundle (until
	// we implement web auth), we can retry more aggressively. This
	// refresh period determines how fast we'll respond to the local
	// bundle being bootstrapped.
	// TODO: reevaluate once we support web auth
	return result, bundleutil.MinimumRefreshHint
}

func (m *Manager) generationLog(trustDomain spiffeid.TrustDomain, gen uint64) logrus.FieldLogger {
	return m.log.WithFields(logrus.Fields{
		telemetry.TrustDomain: trustDomain.Name(),
		"generation":          gen,
	})
}

func (m *Manager) notifyConfigRefreshed(ctx context.Context, nextRefresh time.Duration) {
	if m.configRefreshedCh != nil {
		select {
		case m.configRefreshedCh <- nextRefresh:
		case <-ctx.Done():
		}
	}
}

func (m *Manager) notifyBundleRefreshed(ctx context.Context, nextRefresh time.Duration) {
	if m.bundleRefreshedCh != nil {
		select {
		case m.bundleRefreshedCh <- nextRefresh:
		case <-ctx.Done():
		}
	}
}

func calculateNextUpdate(b *spiffebundle.Bundle) time.Duration {
	if _, ok := b.RefreshHint(); !ok {
		return defaultRefreshInterval
	}
	return bundleutil.CalculateRefreshHint(b) / attemptsPerRefreshHint
}

func cloneTrustDomainConfigs(configs map[spiffeid.TrustDomain]TrustDomainConfig) map[spiffeid.TrustDomain]TrustDomainConfig {
	clone := make(map[spiffeid.TrustDomain]TrustDomainConfig, len(configs))
	maps.Copy(clone, configs)
	return clone
}
