package client

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"sync"

	"github.com/spiffe/go-spiffe/v2/bundle/spiffebundle"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/spire/pkg/common/bundleutil"
	"github.com/spiffe/spire/pkg/server/datastore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type BundleUpdaterConfig struct {
	TrustDomain spiffeid.TrustDomain
	DataStore   datastore.DataStore

	TrustDomainConfig TrustDomainConfig

	// newClientHook is a test hook for injecting client behavior
	newClientHook func(ClientConfig) (Client, error)

	// commitBundleHook, if set, is invoked after the endpoint bundle has been
	// successfully downloaded and validated. The hook decides whether the
	// fetched bundle must be persisted and performs the persistence. It
	// allows the manager to scope writes to the configuration generation
	// that fetched the bundle: once a generation has been superseded or
	// deleted, its hook refuses to write. When unset, the updater persists
	// directly through the datastore.
	commitBundleHook commitBundleFunc
}

// commitBundleFunc commits a freshly fetched endpoint bundle after it has
// been downloaded and validated. It returns the local bundle to report
// (which may be a bundle concurrently discovered in the datastore), the
// endpoint bundle if one was adopted, or an error if the fetched bundle
// could not be committed.
type commitBundleFunc func(ctx context.Context, local, fetched *spiffebundle.Bundle, usedBootstrap bool) (reportedLocal, endpoint *spiffebundle.Bundle, err error)

// errBundleGenerationStopped is returned when a bundle updater generation
// is asked to persist a bundle after the generation has been stopped.
var errBundleGenerationStopped = errors.New("bundle updater generation is stopped; fetched bundle was not persisted")

type BundleUpdater interface {
	// UpdateBundle fetches the local bundle from the datastore and the
	// endpoint bundle from the endpoint. The function will return an error if
	// the local bundle cannot be fetched, the endpoint bundle cannot be
	// downloaded, or there is a problem persisting the bundle. The local
	// bundle will always be returned if it was fetched, independent of any
	// other failures performing the update. The endpoint bundle is ONLY
	// returned if it can be successfully downloaded, is different from the
	// local bundle, and is successfully stored.
	UpdateBundle(ctx context.Context) (*spiffebundle.Bundle, *spiffebundle.Bundle, error)

	// GetTrustDomainConfig returns the configuration for the updater
	GetTrustDomainConfig() TrustDomainConfig

	// SetTrustDomainConfig sets the configuration for the updater
	SetTrustDomainConfig(TrustDomainConfig) bool
}

type bundleUpdater struct {
	td               spiffeid.TrustDomain
	ds               datastore.DataStore
	newClientHook    func(ClientConfig) (Client, error)
	commitBundleHook commitBundleFunc

	trustDomainConfigMtx sync.Mutex
	trustDomainConfig    TrustDomainConfig
}

func NewBundleUpdater(config BundleUpdaterConfig) BundleUpdater {
	if config.newClientHook == nil {
		config.newClientHook = NewClient
	}
	return &bundleUpdater{
		td:                config.TrustDomain,
		ds:                config.DataStore,
		newClientHook:     config.newClientHook,
		commitBundleHook:  config.commitBundleHook,
		trustDomainConfig: config.TrustDomainConfig,
	}
}

func (u *bundleUpdater) UpdateBundle(ctx context.Context) (*spiffebundle.Bundle, *spiffebundle.Bundle, error) {
	trustDomainConfig := u.GetTrustDomainConfig()

	client, usedBootstrap, err := u.newClient(ctx, trustDomainConfig)
	if err != nil {
		return nil, nil, err
	}

	localFederatedBundleOrNil, err := fetchBundleIfExists(ctx, u.ds, u.td)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to fetch local federated bundle: %w", err)
	}

	fetchedFederatedBundle, err := client.FetchBundle(ctx)
	if err != nil {
		return localFederatedBundleOrNil, nil, fmt.Errorf("failed to fetch federated bundle from endpoint: %w", err)
	}

	commit := u.commitBundleHook
	if commit == nil {
		commit = u.commitFetchedBundle
	}
	reportedLocalBundle, endpointFederatedBundleOrNil, err := commit(ctx, localFederatedBundleOrNil, fetchedFederatedBundle, usedBootstrap)
	if err != nil {
		return reportedLocalBundle, nil, fmt.Errorf("failed to store fetched federated bundle: %w", err)
	}

	return reportedLocalBundle, endpointFederatedBundleOrNil, nil
}

// commitFetchedBundle is the default commit implementation used when the
// updater is not managed by a generation-aware manager. Writes go directly
// to the datastore.
func (u *bundleUpdater) commitFetchedBundle(ctx context.Context, localFederatedBundleOrNil, fetchedFederatedBundle *spiffebundle.Bundle, usedBootstrap bool) (*spiffebundle.Bundle, *spiffebundle.Bundle, error) {
	if localFederatedBundleOrNil != nil && fetchedFederatedBundle.Equal(localFederatedBundleOrNil) {
		return localFederatedBundleOrNil, nil, nil
	}

	bundle, err := bundleutil.SPIFFEBundleToProto(fetchedFederatedBundle)
	if err != nil {
		return localFederatedBundleOrNil, nil, err
	}
	if usedBootstrap {
		_, err = u.ds.CreateBundle(ctx, bundle)
		if err != nil {
			if status.Code(err) == codes.AlreadyExists {
				// sqlstore maps any constraint violation to AlreadyExists.
				existing, fetchErr := fetchBundleIfExists(ctx, u.ds, u.td)
				if fetchErr != nil {
					return localFederatedBundleOrNil, nil, fetchErr
				}
				if existing != nil {
					return existing, nil, nil
				}
			}
			return localFederatedBundleOrNil, nil, err
		}
	} else {
		_, err = u.ds.SetBundle(ctx, bundle)
		if err != nil {
			return localFederatedBundleOrNil, nil, err
		}
	}

	return localFederatedBundleOrNil, fetchedFederatedBundle, nil
}

func (u *bundleUpdater) GetTrustDomainConfig() TrustDomainConfig {
	u.trustDomainConfigMtx.Lock()
	trustDomainConfig := u.trustDomainConfig
	u.trustDomainConfigMtx.Unlock()
	return trustDomainConfig
}

func (u *bundleUpdater) SetTrustDomainConfig(trustDomainConfig TrustDomainConfig) bool {
	u.trustDomainConfigMtx.Lock()
	defer u.trustDomainConfigMtx.Unlock()
	if u.trustDomainConfig != trustDomainConfig {
		u.trustDomainConfig = trustDomainConfig
		return true
	}
	return false
}

func (u *bundleUpdater) newClient(ctx context.Context, trustDomainConfig TrustDomainConfig) (Client, bool, error) {
	clientConfig := ClientConfig{
		TrustDomain: u.td,
		EndpointURL: trustDomainConfig.EndpointURL,
	}

	usedBootstrap := false
	if spiffeAuth, ok := trustDomainConfig.EndpointProfile.(HTTPSSPIFFEProfile); ok {
		endpointTD := spiffeAuth.EndpointSPIFFEID.TrustDomain()
		localEndpointBundle, err := fetchBundleIfExists(ctx, u.ds, endpointTD)
		if err != nil {
			return nil, false, fmt.Errorf("failed to fetch local copy of bundle for %q: %w", endpointTD, err)
		}

		var rootCAs []*x509.Certificate
		rootCAs, usedBootstrap, err = rootCAsForSPIFFEAuth(localEndpointBundle, trustDomainConfig, endpointTD, u.td)
		if err != nil {
			return nil, false, err
		}
		clientConfig.SPIFFEAuth = &SPIFFEAuthConfig{
			EndpointSpiffeID: spiffeAuth.EndpointSPIFFEID,
			RootCAs:          rootCAs,
		}
	}
	client, err := u.newClientHook(clientConfig)
	return client, usedBootstrap, err
}

func rootCAsForSPIFFEAuth(local *spiffebundle.Bundle, cfg TrustDomainConfig, endpointTD, federatedTD spiffeid.TrustDomain) ([]*x509.Certificate, bool, error) {
	if local != nil {
		return local.X509Authorities(), false, nil
	}
	if cfg.BootstrapBundlePath == "" {
		return nil, false, errors.New("can't perform SPIFFE Authentication: local copy of bundle not found")
	}
	if endpointTD != federatedTD {
		return nil, false, errors.New("can't perform SPIFFE Authentication: bootstrap bundle cannot be used because the endpoint SPIFFE ID is in a different trust domain")
	}
	certs, err := loadBootstrapX509Authorities(cfg.BootstrapBundlePath, cfg.BootstrapBundleFormat, endpointTD)
	if err != nil {
		return nil, false, fmt.Errorf("can't perform SPIFFE Authentication: %w", err)
	}
	return certs, true, nil
}

func fetchBundleIfExists(ctx context.Context, ds datastore.DataStore, trustDomain spiffeid.TrustDomain) (*spiffebundle.Bundle, error) {
	// Load the current bundle and extract the root CA certificates
	bundle, err := ds.FetchBundle(ctx, trustDomain.IDString())
	if err != nil {
		return nil, err
	}
	if bundle == nil {
		return nil, nil
	}
	return bundleutil.SPIFFEBundleFromProto(bundle)
}
