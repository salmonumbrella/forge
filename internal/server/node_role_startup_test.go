package server

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.kenn.io/forge/internal/config"
	"go.kenn.io/forge/internal/gitclone"
	"go.kenn.io/forge/internal/platform"
	"go.kenn.io/forge/internal/server/httpapi"
	"go.kenn.io/forge/internal/testutil/dbtest"
)

type countingNodeRoleTransport struct {
	requests atomic.Int32
}

func (t *countingNodeRoleTransport) RoundTrip(*http.Request) (*http.Response, error) {
	t.requests.Add(1)
	return nil, assert.AnError
}

func TestInactiveFleetNodeKeepsLocalServicesWithoutProviderPlane(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	transport := &countingNodeRoleTransport{}
	clones := gitclone.New(t.TempDir(), nil)
	cfg := &config.Config{Fleet: config.Fleet{
		Enabled: true, Role: config.FleetRoleNode,
		Coordinator: &config.FleetCoordinator{
			NodeID: proxyTestCoordinatorID, BaseURL: "https://coordinator.example",
		},
	}}
	srv := New(dbtest.Open(t), nil, nil, "/", cfg, ServerOptions{
		FederationNodeID:                   proxyTestNodeID,
		FederationNodeUnavailableReason:    "activation required",
		FederationHTTPClient:               &http.Client{Transport: transport},
		Clones:                             clones,
		WorktreeDir:                        t.TempDir(),
		HostCheckAllowLoopbackAnyPort:      true,
		DisableWorkspaceBackgroundMonitors: true,
	})
	t.Cleanup(func() { gracefulShutdown(t, srv) })

	assert.Nil(srv.syncer)
	assert.Nil(srv.archive)
	assert.Same(clones, srv.clones)
	require.NotNil(srv.providerSource)
	assert.Nil(srv.providerSource.client)
	assert.Nil(srv.providerProxy)
	assert.Nil(srv.coordinatorEvents)
	assert.Equal(httpapi.ProviderCapabilitiesResponse{}, srv.repoResolver.Capabilities(
		platform.KindGitHub, platform.DefaultGitHubHost,
	))
	assert.NotNil(srv.MCPBackend())

	httpServer := httptest.NewServer(srv)
	t.Cleanup(httpServer.Close)
	for _, path := range []string{
		"/api/v1/workspaces",
		"/api/v1/snapshot?include_peers=true",
	} {
		response, err := httpServer.Client().Get(httpServer.URL + path)
		require.NoError(err)
		response.Body.Close()
		assert.Equal(http.StatusOK, response.StatusCode, path)
	}
	providerResponse, err := httpServer.Client().Get(httpServer.URL + "/api/v1/pulls")
	require.NoError(err)
	providerResponse.Body.Close()
	assert.Equal(http.StatusServiceUnavailable, providerResponse.StatusCode)
	assert.Zero(transport.requests.Load(),
		"an unvalidated node must not contact its configured coordinator")
}
