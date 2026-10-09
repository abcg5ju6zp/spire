package client

import (
	"bytes"
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/spiffe/go-spiffe/v2/bundle/spiffebundle"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/spire/pkg/common/bundleutil"
	"github.com/spiffe/spire/pkg/common/telemetry"
	"github.com/spiffe/spire/pkg/server/datastore"
	"github.com/spiffe/spire/proto/spire/common"
	"github.com/spiffe/spire/test/clock"
	"github.com/spiffe/spire/test/fakes/fakedatastore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func webConfig(endpointURL string) TrustDomainConfig {
	return TrustDomainConfig{
		EndpointURL:     endpointURL,
		EndpointProfile: HTTPSWebProfile{},
	}
}

func TestManagerPeriodicBundleRefresh(t *testing.T) {
	// create a pair of bundles with distinct refresh hints so we can assert
	// that the manager selected the correct refresh hint.
	localBundle := spiffebundle.FromX509Authorities(trustDomain, []*x509.Certificate{createCACertificate(t, "local")})
	localBundle.SetRefreshHint(time.Hour)
	endpointBundle := spiffebundle.FromX509Authorities(trustDomain, []*x509.Certificate{createCACertificate(t, "endpoint")})
	endpointBundle.SetRefreshHint(time.Hour * 2)
	noRefreshBundle := spiffebundle.FromX509Authorities(trustDomain, []*x509.Certificate{createCACertificate(t, "endpoint")})

	source := NewTrustDomainConfigSet(TrustDomainConfigMap{
		trustDomain: TrustDomainConfig{
			EndpointURL:     "https://example.org/bundle",
			EndpointProfile: HTTPSWebProfile{},
		},
	})

	testCases := []struct {
		name           string
		localBundle    *spiffebundle.Bundle
		endpointBundle *spiffebundle.Bundle
		nextRefresh    time.Duration
	}{
		{
			name:        "update failed to obtain local bundle",
			nextRefresh: bundleutil.MinimumRefreshHint,
		},
		{
			name:        "update failed to obtain endpoint bundle",
			localBundle: localBundle,
			nextRefresh: calculateNextUpdate(localBundle),
		},
		{
			name:           "update obtained endpoint bundle",
			localBundle:    localBundle,
			endpointBundle: endpointBundle,
			nextRefresh:    calculateNextUpdate(endpointBundle),
		},
		{
			name:           "endpoint bundle does not specify refresh_hint",
			localBundle:    localBundle,
			endpointBundle: noRefreshBundle,
			nextRefresh:    time.Minute * 5,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			test := newManagerTest(t, source,
				func(spiffeid.TrustDomain) *spiffebundle.Bundle {
					return testCase.localBundle
				},
				func(spiffeid.TrustDomain) *spiffebundle.Bundle {
					return testCase.endpointBundle
				},
			)

			// Wait for the config to be refreshed
			test.WaitForConfigRefresh()

			// wait for the initial bundle refresh
			test.WaitForBundleRefresh(testCase.nextRefresh)
			require.Equal(t, 1, test.UpdateCount(trustDomain))

			// advance time and make sure another bundle refresh happens
			test.AdvanceTime(testCase.nextRefresh + time.Millisecond)
			test.WaitForBundleRefresh(testCase.nextRefresh)
			require.Equal(t, 2, test.UpdateCount(trustDomain))
		})
	}
}

func TestManagerOnDemandBundleRefresh(t *testing.T) {
	configSet := NewTrustDomainConfigSet(nil)

	test := newManagerTest(t, configSet, nil, nil)

	// Wait for the config to be refreshed
	test.WaitForConfigRefresh()

	// Assert the trust domain is not known to the manager
	has, err := test.RefreshBundleFor(trustDomain)
	assert.False(t, has, "manager should not know about the trust domain")
	assert.NoError(t, err)
	assert.Equal(t, -1, test.UpdateCount(trustDomain))

	// Now, add the trust domain configuration to the source and assert
	// that refreshing the bundle reloads configs from the source.
	configSet.Set(trustDomain, TrustDomainConfig{
		EndpointURL:     "https://some-domain.test/bundle",
		EndpointProfile: HTTPSWebProfile{},
	})

	has, err = test.RefreshBundleFor(trustDomain)
	assert.True(t, has, "manager should know about the trust domain")
	assert.EqualError(t, err, "OHNO")

	// The update count may be more than 1, since RefreshBundle will update the
	// bundle, but also, since the trust domain is newly managed, kick off a
	// goroutine that will refresh it as well.
	assert.Greater(t, test.UpdateCount(trustDomain), 0)
}

func TestManagerConfigPeriodicRefresh(t *testing.T) {
	td1 := spiffeid.RequireTrustDomainFromString("domain1.test")
	td2 := spiffeid.RequireTrustDomainFromString("domain2.test")
	td3 := spiffeid.RequireTrustDomainFromString("domain3.test")

	configWebA := TrustDomainConfig{
		EndpointURL:     "https://some-domain.test/webA",
		EndpointProfile: HTTPSWebProfile{},
	}
	configWebB := TrustDomainConfig{
		EndpointURL:     "https://some-domain.test/webB",
		EndpointProfile: HTTPSWebProfile{},
	}
	configSPIFFEA := TrustDomainConfig{
		EndpointURL: "https://some-domain.test/spiffeA",
		EndpointProfile: HTTPSSPIFFEProfile{
			EndpointSPIFFEID: spiffeid.RequireFromString("spiffe://some-domain.test/spiffeA"),
		},
	}
	configSPIFFEB := TrustDomainConfig{
		EndpointURL: "https://some-domain.test/spiffeB",
		EndpointProfile: HTTPSSPIFFEProfile{
			EndpointSPIFFEID: spiffeid.RequireFromString("spiffe://some-domain.test/spiffeB"),
		},
	}

	configSet := NewTrustDomainConfigSet(TrustDomainConfigMap{
		td1: configSPIFFEA,
		td2: configWebA,
	})

	test := newManagerTest(t, configSet, nil, nil)

	// Wait until the config is refreshed and a bundle refresh happens
	test.WaitForConfigRefresh()
	test.WaitForBundleRefresh(bundleutil.MinimumRefreshHint) // td1
	test.WaitForBundleRefresh(bundleutil.MinimumRefreshHint) // td2

	// Assert that we have configuration for td1 and td2, but not td3 and that
	// update attempts were made on td1 and td2, but that td3 is unknown to
	// the manager.
	require.Equal(t, map[spiffeid.TrustDomain]TrustDomainConfig{
		td1: configSPIFFEA,
		td2: configWebA,
	}, test.GetTrustDomainConfigs())
	assert.Equal(t, 1, test.UpdateCount(td1))
	assert.Equal(t, 1, test.UpdateCount(td2))
	assert.Equal(t, -1, test.UpdateCount(td3))

	// Now adjust the configuration to drop td1, change td2, and introduce td3.
	// Both td2 and td3 should have an extra update count. td1 update count will
	// remain the same.
	configSet.SetAll(TrustDomainConfigMap{
		td2: configSPIFFEB,
		td3: configWebB,
	})

	// Wait until the config is refreshed. The td2 configuration change is
	// validated by a new candidate generation that refreshes immediately,
	// just like the newly managed td3, so two bundle refreshes occur.
	test.AdvanceTime(configRefreshInterval + time.Millisecond)
	test.WaitForConfigRefresh()
	test.WaitForBundleRefresh(bundleutil.MinimumRefreshHint) // td2 candidate or td3
	test.WaitForBundleRefresh(bundleutil.MinimumRefreshHint) // td2 candidate or td3
	assert.Equal(t, 1, test.UpdateCount(td1))
	assert.Equal(t, 1, test.UpdateCount(td2))
	assert.Equal(t, 1, test.UpdateCount(td3))

	test.AdvanceTime(bundleutil.MinimumRefreshHint + time.Millisecond)
	test.WaitForBundleRefresh(bundleutil.MinimumRefreshHint) // td2
	test.WaitForBundleRefresh(bundleutil.MinimumRefreshHint) // td3

	require.Equal(t, map[spiffeid.TrustDomain]TrustDomainConfig{
		td1: configSPIFFEA,
		td2: configSPIFFEB,
		td3: configWebB,
	}, test.GetTrustDomainConfigs())
	assert.Equal(t, 1, test.UpdateCount(td1))
	assert.Equal(t, 2, test.UpdateCount(td2))
	assert.Equal(t, 2, test.UpdateCount(td3))
}

func TestManagerConfigManualRefresh(t *testing.T) {
	td1 := spiffeid.RequireTrustDomainFromString("domain1.test")
	td2 := spiffeid.RequireTrustDomainFromString("domain2.test")
	config1 := TrustDomainConfig{
		EndpointURL:     "https://domain1.test/bundle",
		EndpointProfile: HTTPSWebProfile{},
	}
	config2 := TrustDomainConfig{
		EndpointURL:     "https://domain2.test/bundle",
		EndpointProfile: HTTPSWebProfile{},
	}

	configSet := NewTrustDomainConfigSet(TrustDomainConfigMap{
		td1: config1,
	})

	test := newManagerTest(t, configSet, nil, nil)

	// Wait for the original config to be loaded
	test.WaitForConfigRefresh()
	require.Equal(t, map[spiffeid.TrustDomain]TrustDomainConfig{
		td1: config1,
	}, test.GetTrustDomainConfigs())

	// Update config and trigger the reload
	configSet.Set(td2, config2)
	test.manager.TriggerConfigReload()
	test.WaitForConfigRefresh()
	require.Equal(t, map[spiffeid.TrustDomain]TrustDomainConfig{
		td1: config1,
		td2: config2,
	}, test.GetTrustDomainConfigs())
}

type managerTest struct {
	t                 *testing.T
	clock             *clock.Mock
	localBundles      func(spiffeid.TrustDomain) *spiffebundle.Bundle
	endpointBundles   func(spiffeid.TrustDomain) *spiffebundle.Bundle
	bundleUpdatersMtx sync.Mutex
	bundleUpdaters    map[spiffeid.TrustDomain]*fakeBundleUpdater
	configRefreshedCh chan time.Duration
	bundleRefreshedCh chan time.Duration
	manager           *Manager
}

func newManagerTest(t *testing.T, source TrustDomainConfigSource, localBundles, endpointBundles func(spiffeid.TrustDomain) *spiffebundle.Bundle) *managerTest {
	log, _ := test.NewNullLogger()

	if localBundles == nil {
		localBundles = func(spiffeid.TrustDomain) *spiffebundle.Bundle { return nil }
	}
	if endpointBundles == nil {
		endpointBundles = func(spiffeid.TrustDomain) *spiffebundle.Bundle { return nil }
	}

	test := &managerTest{
		t:                 t,
		clock:             clock.NewMock(t),
		localBundles:      localBundles,
		endpointBundles:   endpointBundles,
		bundleUpdaters:    make(map[spiffeid.TrustDomain]*fakeBundleUpdater),
		configRefreshedCh: make(chan time.Duration),
		bundleRefreshedCh: make(chan time.Duration),
	}

	test.manager = NewManager(ManagerConfig{
		Log:               log,
		Metrics:           telemetry.Blackhole{},
		DataStore:         fakedatastore.New(t),
		Clock:             test.clock,
		Source:            source,
		newBundleUpdater:  test.newBundleUpdater,
		configRefreshedCh: test.configRefreshedCh,
		bundleRefreshedCh: test.bundleRefreshedCh,
	})

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				errCh <- fmt.Errorf("%+v", r)
			}
		}()
		errCh <- test.manager.Run(ctx)
	}()

	t.Cleanup(func() {
		cancel()
		select {
		case err := <-errCh:
			require.EqualError(t, err, "context canceled")
		case <-time.After(time.Minute):
			require.Fail(t, "timed out waiting for run to complete")
		}
	})

	return test
}

func (test *managerTest) AdvanceTime(dt time.Duration) {
	test.clock.Add(dt)
}

func (test *managerTest) UpdateCount(td spiffeid.TrustDomain) int {
	bundleUpdater, ok := test.bundleUpdaterFor(td)
	if !ok {
		return -1
	}
	return bundleUpdater.UpdateCount()
}

func (test *managerTest) GetTrustDomainConfigs() map[spiffeid.TrustDomain]TrustDomainConfig {
	test.bundleUpdatersMtx.Lock()
	defer test.bundleUpdatersMtx.Unlock()

	configs := make(map[spiffeid.TrustDomain]TrustDomainConfig)
	for td, bundleUpdater := range test.bundleUpdaters {
		configs[td] = bundleUpdater.GetTrustDomainConfig()
	}
	return configs
}

func (test *managerTest) WaitForConfigRefresh() {
	select {
	case d := <-test.configRefreshedCh:
		require.Equal(test.t, configRefreshInterval, d, "next config refresh not at the expected interval")
	case <-time.After(time.Second * 10):
		require.Fail(test.t, "timed out waiting for config refresh")
	}
}

func (test *managerTest) WaitForBundleRefresh(expectNextRefresh time.Duration) {
	select {
	case d := <-test.bundleRefreshedCh:
		require.Equal(test.t, expectNextRefresh, d, "next bundle refresh not at the expected interval")
	case <-time.After(time.Second * 10):
		require.Fail(test.t, "timed out waiting for bundle refresh")
	}
}

func (test *managerTest) RefreshBundleFor(td spiffeid.TrustDomain) (bool, error) {
	return test.manager.RefreshBundleFor(context.Background(), td)
}

func (test *managerTest) newBundleUpdater(config BundleUpdaterConfig) BundleUpdater {
	bundleUpdater := newFakeBundleUpdater(config)
	bundleUpdater.SetBundles(
		test.localBundles(config.TrustDomain),
		test.endpointBundles(config.TrustDomain),
	)

	test.bundleUpdatersMtx.Lock()
	defer test.bundleUpdatersMtx.Unlock()
	test.bundleUpdaters[config.TrustDomain] = bundleUpdater
	return bundleUpdater
}

func (test *managerTest) bundleUpdaterFor(td spiffeid.TrustDomain) (*fakeBundleUpdater, bool) {
	test.bundleUpdatersMtx.Lock()
	defer test.bundleUpdatersMtx.Unlock()
	updater, ok := test.bundleUpdaters[td]
	return updater, ok
}

type fakeBundleUpdater struct {
	mtx            sync.Mutex
	localBundle    *spiffebundle.Bundle
	endpointBundle *spiffebundle.Bundle
	updateCount    int
	config         BundleUpdaterConfig
}

func newFakeBundleUpdater(config BundleUpdaterConfig) *fakeBundleUpdater {
	return &fakeBundleUpdater{
		config: config,
	}
}

func (u *fakeBundleUpdater) SetBundles(localBundle, endpointBundle *spiffebundle.Bundle) {
	u.mtx.Lock()
	defer u.mtx.Unlock()
	u.localBundle = localBundle
	u.endpointBundle = endpointBundle
}

func (u *fakeBundleUpdater) UpdateCount() int {
	u.mtx.Lock()
	defer u.mtx.Unlock()
	return u.updateCount
}

func (u *fakeBundleUpdater) UpdateBundle(context.Context) (*spiffebundle.Bundle, *spiffebundle.Bundle, error) {
	u.mtx.Lock()
	defer u.mtx.Unlock()
	u.updateCount++
	return u.localBundle, u.endpointBundle, errors.New("OHNO")
}

func (u *fakeBundleUpdater) GetTrustDomainConfig() TrustDomainConfig {
	u.mtx.Lock()
	defer u.mtx.Unlock()
	return u.config.TrustDomainConfig
}

func (u *fakeBundleUpdater) SetTrustDomainConfig(trustDomainConfig TrustDomainConfig) bool {
	u.mtx.Lock()
	defer u.mtx.Unlock()
	if u.config.TrustDomainConfig != trustDomainConfig {
		u.config.TrustDomainConfig = trustDomainConfig
		return true
	}
	return false
}

// TestManagerGenerationSwitchPreventsStaleWrites verifies the core
// generational guarantee: when the endpoint changes while a refresh of
// the old generation is still in flight, the slow old generation can
// never overwrite the bundle confirmed by the new generation, even when
// the old goroutine ignores cancellation and the stop wait times out.
func TestManagerGenerationSwitchPreventsStaleWrites(t *testing.T) {
	v2 := genBundle(t, "v2")
	v3 := genBundle(t, "v3")
	genTest := newGenerationTest(t, webConfig("https://a.example/bundle"), nil, map[string]*spiffebundle.Bundle{
		"A": v2,
		"B": v3,
	})
	defer genTest.Stop()

	// The first generation confirms v2.
	genTest.requireStoredBundle(v2)
	require.Equal(t, 1, genTest.ds.setCount())

	// Make the old generation start a slow second download that ignores
	// cancellation, then switch the endpoint.
	a := genTest.endpoint("A")
	blockA := make(chan struct{})
	startedA := make(chan struct{}, 1)
	a.setBlock(blockA, startedA, true)
	genTest.advance(defaultRefreshInterval + time.Millisecond)
	waitForSignal(t, startedA, "old generation to start download")

	genTest.source.Set(trustDomain, webConfig("https://b.example/bundle"))
	genTest.manager.TriggerConfigReload()

	// The candidate generation confirms v3 and promotes; the old
	// generation is stopped and its drain times out.
	genTest.requireStoredBundle(v3)
	require.Equal(t, 2, genTest.ds.setCount())

	// Let the slow old download complete with v2. It must be rejected by
	// the generation gate and never reach the datastore.
	close(blockA)
	genTest.requireEndpointCalls(a, 2)
	genTest.requireStoredBundle(v3)
	require.Equal(t, 2, genTest.ds.setCount())

	// The new generation took over and the old one is retired.
	current, candidate, _ := genTest.generations()
	require.NotNil(t, current)
	require.Equal(t, "https://b.example/bundle", current.cfg.EndpointURL)
	require.Nil(t, candidate)
	require.True(t, current.promoted)
}

// TestManagerGenerationDeletionPreventsLateWrites verifies that deleting
// the federation relationship stops both the current and candidate
// generations and that an in-flight download cannot recreate the bundle
// afterwards.
func TestManagerGenerationDeletionPreventsLateWrites(t *testing.T) {
	v2 := genBundle(t, "v2")
	v3 := genBundle(t, "v3")
	genTest := newGenerationTest(t, webConfig("https://a.example/bundle"), nil, map[string]*spiffebundle.Bundle{
		"A": v2,
	})
	defer genTest.Stop()

	genTest.requireStoredBundle(v2)
	require.Equal(t, 1, genTest.ds.setCount())

	a := genTest.endpoint("A")
	blockA := make(chan struct{})
	startedA := make(chan struct{}, 1)
	a.setBlock(blockA, startedA, true)
	genTest.advance(defaultRefreshInterval + time.Millisecond)
	waitForSignal(t, startedA, "generation to start download")

	a.setBundle(v3)
	genTest.source.SetAll(nil)
	genTest.manager.TriggerConfigReload()

	// The reconciliation that removes the trust domain completes despite
	// the generation stuck in an uninterruptible download.
	genTest.waitForConfigReload()
	require.Eventually(t, func() bool {
		genTest.manager.updatersMtx.RLock()
		defer genTest.manager.updatersMtx.RUnlock()
		_, ok := genTest.manager.updaters[trustDomain]
		return !ok
	}, time.Second*5, time.Millisecond*10, "trust domain should be removed")

	close(blockA)
	genTest.requireEndpointCalls(a, 2)

	// The late download must not have overwritten the stored bundle.
	genTest.requireStoredBundle(v2)
	require.Equal(t, 1, genTest.ds.setCount())
}

// TestManagerCandidateRetriesDownloadFailures verifies that a candidate
// that cannot download retains the last good bundle, keeps retrying, and
// the current generation keeps refreshing until the candidate promotes.
func TestManagerCandidateRetriesDownloadFailures(t *testing.T) {
	v1 := genBundle(t, "v1")
	genTest := newGenerationTest(t, webConfig("https://a.example/bundle"), v1, map[string]*spiffebundle.Bundle{
		"A": v1,
	})
	defer genTest.Stop()

	a := genTest.endpoint("A")
	b := genTest.endpoint("B")
	b.err = errors.New("endpoint B unavailable")

	genTest.requirePromoted("https://a.example/bundle")
	firstGen, _, _ := genTest.generations()

	// Switch to a failing endpoint: the candidate cannot confirm its
	// configuration and the old generation stays current.
	genTest.source.Set(trustDomain, webConfig("https://b.example/bundle"))
	genTest.manager.TriggerConfigReload()
	genTest.requireCandidate()
	require.Eventually(t, func() bool { return b.callCount() >= 1 }, time.Second*5, time.Millisecond*10)

	// Advance through a retry cycle: the candidate fails again while the
	// old current generation keeps refreshing from endpoint A.
	genTest.advance(defaultRefreshInterval + time.Millisecond)
	require.Eventually(t, func() bool { return b.callCount() >= 2 && a.callCount() >= 2 }, time.Second*5, time.Millisecond*10)
	genTest.requireStoredBundle(v1)

	currentGen, candidateGen, _ := genTest.generations()
	require.Same(t, firstGen, currentGen)
	require.NotNil(t, candidateGen)

	// Once the candidate can download, it persists the new bundle and
	// takes over atomically.
	v3 := genBundle(t, "v3")
	b.setBundle(v3)
	genTest.advance(defaultRefreshInterval + time.Millisecond)
	genTest.requireStoredBundle(v3)
	genTest.requirePromoted("https://b.example/bundle")
}

// TestManagerCandidateRetriesDatastoreFailures verifies that a datastore
// write failure leaves the candidate unpromoted and the previous bundle
// intact until a retry succeeds.
func TestManagerCandidateRetriesDatastoreFailures(t *testing.T) {
	v1 := genBundle(t, "v1")
	v3 := genBundle(t, "v3")
	genTest := newGenerationTest(t, webConfig("https://a.example/bundle"), v1, map[string]*spiffebundle.Bundle{
		"A": v1,
		"B": v3,
	})
	defer genTest.Stop()

	genTest.requirePromoted("https://a.example/bundle")

	genTest.ds.failNextSets(1, errors.New("datastore unavailable"))
	genTest.source.Set(trustDomain, webConfig("https://b.example/bundle"))
	genTest.manager.TriggerConfigReload()

	// The candidate attempted the write but did not promote: the previous
	// generation stays current with the last good bundle.
	require.Eventually(t, func() bool { return genTest.ds.setCount() == 1 }, time.Second*5, time.Millisecond*10)
	genTest.requireStoredBundle(v1)
	currentGen, candidateGen, _ := genTest.generations()
	require.Equal(t, "https://a.example/bundle", currentGen.cfg.EndpointURL)
	require.False(t, candidateGen.promoted, "candidate must not promote after a datastore write failure")

	// A later retry persists the bundle and promotes the candidate.
	genTest.advance(defaultRefreshInterval + time.Millisecond)
	genTest.requireStoredBundle(v3)
	genTest.requirePromoted("https://b.example/bundle")
}

// TestManagerManualRefreshConvergesOnCandidate verifies that an on-demand
// refresh drives the newest generation for the trust domain and promotes
// it on success.
func TestManagerManualRefreshConvergesOnCandidate(t *testing.T) {
	v1 := genBundle(t, "v1")
	v3 := genBundle(t, "v3")
	genTest := newGenerationTest(t, webConfig("https://a.example/bundle"), v1, map[string]*spiffebundle.Bundle{
		"A": v1,
	})
	defer genTest.Stop()

	b := genTest.endpoint("B")
	b.err = errors.New("endpoint B unavailable")

	genTest.requirePromoted("https://a.example/bundle")

	genTest.source.Set(trustDomain, webConfig("https://b.example/bundle"))
	has, err := genTest.manager.RefreshBundleFor(context.Background(), trustDomain)
	require.True(t, has)
	require.ErrorContains(t, err, "endpoint B unavailable")

	// Once B serves the bundle, the manual refresh confirms and promotes
	// the candidate generation.
	b.setBundle(v3)
	has, err = genTest.manager.RefreshBundleFor(context.Background(), trustDomain)
	require.True(t, has)
	require.NoError(t, err)
	genTest.requireStoredBundle(v3)
	genTest.requirePromoted("https://b.example/bundle")
}

// TestManagerManualRefreshAfterDeletion verifies that an on-demand
// refresh after the federation relationship is removed reports the trust
// domain as unmanaged instead of driving a stopped generation.
func TestManagerManualRefreshAfterDeletion(t *testing.T) {
	v1 := genBundle(t, "v1")
	genTest := newGenerationTest(t, webConfig("https://a.example/bundle"), nil, map[string]*spiffebundle.Bundle{
		"A": v1,
	})
	defer genTest.Stop()

	genTest.requireStoredBundle(v1)
	genTest.source.SetAll(nil)

	has, err := genTest.manager.RefreshBundleFor(context.Background(), trustDomain)
	require.NoError(t, err)
	require.False(t, has, "removed trust domain should not be refreshed")

	require.Eventually(t, func() bool {
		return genTest.holderOrNil() == nil
	}, time.Second*5, time.Millisecond*10)
}

// TestManagerUnchangedConfigKeepsGeneration verifies that a configuration
// refresh that does not change the configuration keeps the same, single
// updater generation.
func TestManagerUnchangedConfigKeepsGeneration(t *testing.T) {
	v1 := genBundle(t, "v1")
	genTest := newGenerationTest(t, webConfig("https://a.example/bundle"), nil, map[string]*spiffebundle.Bundle{
		"A": v1,
	})
	defer genTest.Stop()

	genTest.requireStoredBundle(v1)

	current, _, genNum := genTest.generations()
	require.Equal(t, uint64(1), current.gen)
	require.Equal(t, uint64(1), genNum)
	first := current

	genTest.drainConfigNotifications()
	genTest.advance(configRefreshInterval + time.Millisecond)
	genTest.waitForConfigReload()

	current, candidate, genNum := genTest.generations()
	require.Same(t, first, current)
	require.Nil(t, candidate)
	require.Equal(t, uint64(1), genNum)
}

// TestManagerRestartRebuildsSingleUpdater verifies that on startup the
// manager rebuilds exactly one updater per trust domain from the current
// configuration and the stored bundle, using the stored bundle's
// authorities for https_spiffe authentication.
func TestManagerRestartRebuildsSingleUpdater(t *testing.T) {
	v1 := genBundle(t, "stored")
	genTest := newGenerationTest(t,
		TrustDomainConfig{
			EndpointURL: "https://a.example/bundle",
			EndpointProfile: HTTPSSPIFFEProfile{
				EndpointSPIFFEID: spiffeid.RequireFromString("spiffe://domain.test/endpoint"),
			},
		},
		v1,
		map[string]*spiffebundle.Bundle{"A": v1},
	)
	defer genTest.Stop()

	a := genTest.endpoint("A")

	genTest.requirePromoted("https://a.example/bundle")

	// One generation only, authenticated with the stored bundle roots.
	current, candidate, _ := genTest.generations()
	require.Nil(t, candidate)
	require.Equal(t, uint64(1), current.gen)
	require.Eventually(t, func() bool {
		return len(a.recordedRootCAs()) == 1 && bytes.Equal(a.recordedRootCAs()[0].Raw, v1.X509Authorities()[0].Raw)
	}, time.Second*5, time.Millisecond*10, "endpoint should be authenticated with stored bundle roots")

	// No bootstrap write happened.
	require.Equal(t, 0, genTest.ds.setCount())
	require.Equal(t, 0, genTest.ds.createCount())
}

// TestManagerFirstBootstrapPromotesGeneration verifies that the normal
// first https_spiffe bootstrap path still works and promotes the
// bootstrapping generation.
func TestManagerFirstBootstrapPromotesGeneration(t *testing.T) {
	bootstrap := genBundle(t, "bootstrap")
	v2 := genBundle(t, "v2")
	cfg := TrustDomainConfig{
		EndpointURL: "https://a.example/bundle",
		EndpointProfile: HTTPSSPIFFEProfile{
			EndpointSPIFFEID: spiffeid.RequireFromString("spiffe://domain.test/endpoint"),
		},
		BootstrapBundlePath: writeBootstrapPEM(t, bootstrap.X509Authorities()[0]),
	}
	genTest := newGenerationTest(t, cfg, nil, map[string]*spiffebundle.Bundle{
		"A": v2,
	})
	defer genTest.Stop()

	a := genTest.endpoint("A")

	genTest.requireStoredBundle(v2)
	require.Equal(t, 1, genTest.ds.createCount())
	genTest.requirePromoted("https://a.example/bundle")
	require.Eventually(t, func() bool {
		roots := a.recordedRootCAs()
		return len(roots) == 1 && bytes.Equal(roots[0].Raw, bootstrap.X509Authorities()[0].Raw)
	}, time.Second*5, time.Millisecond*10, "endpoint should be authenticated with bootstrap roots")
}

// --- generation test harness ---

const generationTestStopTimeout = 50 * time.Millisecond

type generationTest struct {
	t         *testing.T
	manager   *Manager
	clk       *clock.Mock
	ds        *recordingDataStore
	source    *TrustDomainConfigSet
	endpoints map[string]*endpointState
}

func newGenerationTest(t *testing.T, cfg TrustDomainConfig, initialBundle *spiffebundle.Bundle, initialEndpointBundles map[string]*spiffebundle.Bundle) *generationTest {
	log, _ := test.NewNullLogger()
	mockClock := clock.NewMock(t)
	fakeDS := fakedatastore.New(t)
	ds := newRecordingDataStore(fakeDS)

	if initialBundle != nil {
		proto, err := bundleutil.SPIFFEBundleToProto(initialBundle)
		require.NoError(t, err)
		_, err = fakeDS.CreateBundle(context.Background(), proto)
		require.NoError(t, err)
	}

	gt := &generationTest{
		t:         t,
		clk:       mockClock,
		ds:        ds,
		source:    NewTrustDomainConfigSet(TrustDomainConfigMap{trustDomain: cfg}),
		endpoints: make(map[string]*endpointState),
	}
	for _, url := range []string{"https://a.example/bundle", "https://b.example/bundle"} {
		gt.endpoints[url] = &endpointState{}
	}
	for name, bundle := range initialEndpointBundles {
		// Normalize through a proto round trip so the endpoint serves the
		// same representation a real decoded endpoint bundle would have;
		// this keeps bundle equality stable against a datastore round trip.
		proto, err := bundleutil.SPIFFEBundleToProto(bundle)
		require.NoError(t, err)
		served, err := bundleutil.SPIFFEBundleFromProto(proto)
		require.NoError(t, err)
		gt.endpoint(name).bundle = served
	}

	gt.manager = NewManager(ManagerConfig{
		Log:         log,
		Metrics:     telemetry.Blackhole{},
		DataStore:   ds,
		Clock:       mockClock,
		Source:      gt.source,
		stopTimeout: generationTestStopTimeout,
		newBundleUpdater: func(config BundleUpdaterConfig) BundleUpdater {
			config.newClientHook = func(clientConfig ClientConfig) (Client, error) {
				state := gt.endpoints[clientConfig.EndpointURL]
				require.NotNil(t, state, "no endpoint configured for %q", clientConfig.EndpointURL)
				if clientConfig.SPIFFEAuth != nil {
					state.recordRootCAs(clientConfig.SPIFFEAuth.RootCAs)
				}
				return &controllableClient{state: state}, nil
			}
			return NewBundleUpdater(config)
		},
		configRefreshedCh: make(chan time.Duration, 100),
		bundleRefreshedCh: make(chan time.Duration, 100),
	})

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- gt.manager.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-errCh:
			require.EqualError(t, err, "context canceled")
		case <-time.After(time.Minute):
			require.Fail(t, "timed out waiting for manager run to complete")
		}
	})

	return gt
}

func (gt *generationTest) Stop() {}

func (gt *generationTest) endpoint(name string) *endpointState {
	var url string
	switch name {
	case "A":
		url = "https://a.example/bundle"
	case "B":
		url = "https://b.example/bundle"
	default:
		gt.t.Fatalf("unknown endpoint %q", name)
	}
	return gt.endpoints[url]
}

func (gt *generationTest) advance(d time.Duration) {
	gt.clk.Add(d)
}

func (gt *generationTest) drainConfigNotifications() {
	for {
		select {
		case <-gt.manager.configRefreshedCh:
		default:
			return
		}
	}
}

func (gt *generationTest) waitForConfigReload() {
	select {
	case <-gt.manager.configRefreshedCh:
	case <-time.After(time.Second * 5):
		require.Fail(gt.t, "timed out waiting for config reload")
	}
}

func (gt *generationTest) requireStoredBundle(want *spiffebundle.Bundle) {
	require.Eventually(gt.t, func() bool {
		proto, err := gt.ds.FetchBundle(context.Background(), trustDomain.IDString())
		if err != nil || proto == nil {
			return false
		}
		got, err := bundleutil.SPIFFEBundleFromProto(proto)
		if err != nil {
			return false
		}
		return sameX509Authorities(got, want)
	}, time.Second*5, time.Millisecond*10, "stored bundle did not converge")
}

// sameX509Authorities compares the X.509 authorities of two bundles. It
// deliberately ignores refresh hint/sequence-number presence metadata,
// which is normalized by the datastore round trip.
func sameX509Authorities(a, b *spiffebundle.Bundle) bool {
	aCAs := a.X509Authorities()
	bCAs := b.X509Authorities()
	if len(aCAs) != len(bCAs) {
		return false
	}
	for i := range aCAs {
		if !bytes.Equal(aCAs[i].Raw, bCAs[i].Raw) {
			return false
		}
	}
	return true
}

func (gt *generationTest) requireHolder() *managedTrustDomain {
	gt.manager.updatersMtx.RLock()
	defer gt.manager.updatersMtx.RUnlock()
	holder, ok := gt.manager.updaters[trustDomain]
	require.True(gt.t, ok, "trust domain is not managed")
	return holder
}

func (gt *generationTest) holderOrNil() *managedTrustDomain {
	gt.manager.updatersMtx.RLock()
	defer gt.manager.updatersMtx.RUnlock()
	return gt.manager.updaters[trustDomain]
}

func (gt *generationTest) generations() (current, candidate *managedBundleUpdater, genNum uint64) {
	holder := gt.requireHolder()
	holder.mtx.Lock()
	defer holder.mtx.Unlock()
	return holder.current, holder.candidate, holder.genNum
}

func (gt *generationTest) requirePromoted(endpointURL string) {
	require.Eventually(gt.t, func() bool {
		holder := gt.holderOrNil()
		if holder == nil {
			return false
		}
		holder.mtx.Lock()
		defer holder.mtx.Unlock()
		return holder.current != nil && holder.current.promoted && holder.current.cfg.EndpointURL == endpointURL
	}, time.Second*5, time.Millisecond*10, "generation for %q not promoted", endpointURL)
}

func (gt *generationTest) requireCandidate() *managedBundleUpdater {
	var candidate *managedBundleUpdater
	require.Eventually(gt.t, func() bool {
		holder := gt.holderOrNil()
		if holder == nil {
			return false
		}
		holder.mtx.Lock()
		defer holder.mtx.Unlock()
		candidate = holder.candidate
		return candidate != nil
	}, time.Second*5, time.Millisecond*10, "candidate generation missing")
	return candidate
}

func (gt *generationTest) requireEndpointCalls(state *endpointState, calls int) {
	require.Eventually(gt.t, func() bool { return state.callCount() == calls }, time.Second*5, time.Millisecond*10)
}

func genBundle(t *testing.T, cn string) *spiffebundle.Bundle {
	return spiffebundle.FromX509Authorities(trustDomain, []*x509.Certificate{createCACertificate(t, cn)})
}

func waitForSignal(t *testing.T, ch <-chan struct{}, what string) {
	select {
	case <-ch:
	case <-time.After(time.Second * 5):
		require.Fail(t, "timed out waiting for %s", what)
	}
}

type endpointState struct {
	mu      sync.Mutex
	bundle  *spiffebundle.Bundle
	err     error
	calls   int
	roots   []*x509.Certificate
	block   chan struct{}
	started chan struct{}
	// ignoreCtx, when true, makes the endpoint ignore context cancellation
	// while blocked, simulating an uninterruptible slow request.
	ignoreCtx bool
}

func (s *endpointState) setBlock(block, started chan struct{}, ignoreCtx bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.block = block
	s.started = started
	s.ignoreCtx = ignoreCtx
}

func (s *endpointState) setBundle(b *spiffebundle.Bundle) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bundle = b
	s.err = nil
}

func (s *endpointState) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func (s *endpointState) recordRootCAs(roots []*x509.Certificate) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.roots = roots
}

func (s *endpointState) recordedRootCAs() []*x509.Certificate {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.roots
}

type controllableClient struct {
	state *endpointState
}

func (c *controllableClient) FetchBundle(ctx context.Context) (*spiffebundle.Bundle, error) {
	c.state.mu.Lock()
	block := c.state.block
	started := c.state.started
	ignoreCtx := c.state.ignoreCtx
	c.state.mu.Unlock()

	if started != nil {
		select {
		case started <- struct{}{}:
		default:
		}
	}
	if block != nil {
		if ignoreCtx {
			<-block
		} else {
			select {
			case <-block:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	c.state.calls++
	if c.state.err != nil {
		return nil, c.state.err
	}
	if c.state.bundle == nil {
		return nil, errors.New("test endpoint has no bundle configured")
	}
	return c.state.bundle, nil
}

type recordingDataStore struct {
	datastore.DataStore
	mu          sync.Mutex
	sets        int
	creates     int
	setFailures int
	failure     error
}

func newRecordingDataStore(inner datastore.DataStore) *recordingDataStore {
	return &recordingDataStore{DataStore: inner}
}

func (d *recordingDataStore) SetBundle(ctx context.Context, bundle *common.Bundle) (*common.Bundle, error) {
	d.mu.Lock()
	d.sets++
	err := error(nil)
	if d.setFailures > 0 {
		d.setFailures--
		err = d.failure
	}
	d.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return d.DataStore.SetBundle(ctx, bundle)
}

func (d *recordingDataStore) CreateBundle(ctx context.Context, bundle *common.Bundle) (*common.Bundle, error) {
	d.mu.Lock()
	d.creates++
	d.mu.Unlock()
	return d.DataStore.CreateBundle(ctx, bundle)
}

func (d *recordingDataStore) setCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.sets
}

func (d *recordingDataStore) createCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.creates
}

func (d *recordingDataStore) failNextSets(n int, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.setFailures = n
	d.failure = err
}
