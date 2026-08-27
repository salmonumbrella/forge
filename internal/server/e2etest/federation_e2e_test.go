package e2etest

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.kenn.io/forge/internal/config"
	"go.kenn.io/forge/internal/db"
	"go.kenn.io/forge/internal/federation"
	"go.kenn.io/forge/internal/federationauth"
	"go.kenn.io/forge/internal/fleet"
	ghclient "go.kenn.io/forge/internal/github"
	"go.kenn.io/forge/internal/mcpserver"
	"go.kenn.io/forge/internal/platform"
	"go.kenn.io/forge/internal/providerplane"
	"go.kenn.io/forge/internal/server"
	"go.kenn.io/forge/internal/server/httpapi"
	"go.kenn.io/forge/internal/server/pullapi"
	"go.kenn.io/forge/internal/testutil/dbtest"
)

const (
	federatedCoordinatorID = "10101010101010101010101010101010"
	federatedNodeAID       = "20202020202020202020202020202020"
	federatedNodeBID       = "30303030303030303030303030303030"
)

type federationHandlerBox struct {
	handler http.Handler
}

// switchableFederationHandler lets the fixture model a coordinator outage
// without changing an origin or rebuilding either node's HTTP client.
type switchableFederationHandler struct {
	current atomic.Pointer[federationHandlerBox]
	offline atomic.Bool
}

func newSwitchableFederationHandler() *switchableFederationHandler {
	handler := &switchableFederationHandler{}
	handler.Set(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "federation daemon is starting", http.StatusServiceUnavailable)
	}))
	return handler
}

func (h *switchableFederationHandler) Set(handler http.Handler) {
	h.current.Store(&federationHandlerBox{handler: handler})
}

func (h *switchableFederationHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.offline.Load() {
		w.Header().Set("Content-Type", "application/problem+json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": http.StatusServiceUnavailable,
			"code":   httpapi.CodeCoordinatorUnavailable,
			"detail": "the federation coordinator is unavailable",
		})
		return
	}
	h.current.Load().handler.ServeHTTP(w, r)
}

type federatedDaemonFixture struct {
	NodeID      string
	Name        string
	LocalToken  string
	Database    *db.DB
	Credentials *federationauth.Store
	Server      *server.Server
	HTTP        *httptest.Server
	Switch      *switchableFederationHandler
}

type countingSyntheticProvider struct {
}

func (p *countingSyntheticProvider) Seed(t *testing.T, database *db.DB) int64 {
	t.Helper()
	identity := verifiedRepoIdentity(db.GitHubRepoIdentity(
		"github.com", "acme", "widget",
	))
	repoID, err := database.UpsertRepo(t.Context(), identity)
	require.NoError(t, err)
	require.NoError(t, database.UpdateRepoProviderMetadata(
		t.Context(), repoID, db.RepoProviderMetadata{
			PlatformRepoID: identity.PlatformRepoID,
			WebURL:         "https://github.com/acme/widget",
			CloneURL:       "https://github.com/acme/widget.git",
			DefaultBranch:  "main",
		},
	))
	now := time.Now().UTC().Truncate(time.Second)
	for _, pull := range []db.MergeRequest{
		{
			RepoID: repoID, PlatformID: 1001, PlatformExternalID: "pull-1",
			Number: 1, URL: "https://github.com/acme/widget/pull/1",
			Title: "Draft federation", Author: "ada", State: db.MergeRequestStateOpen,
			IsDraft: true, HeadBranch: "draft-federation", BaseBranch: "main",
			SnapshotRevision: 1, CreatedAt: now.Add(-2 * time.Hour),
			UpdatedAt: now.Add(-2 * time.Hour), LastActivityAt: now.Add(-2 * time.Hour),
		},
		{
			RepoID: repoID, PlatformID: 1002, PlatformExternalID: "pull-2",
			Number: 2, URL: "https://github.com/acme/widget/pull/2",
			Title: "Newer federation", Author: "grace", State: db.MergeRequestStateOpen,
			HeadBranch: "newer-federation", BaseBranch: "main",
			SnapshotRevision: 1, CreatedAt: now.Add(-time.Hour),
			UpdatedAt: now.Add(-time.Hour), LastActivityAt: now.Add(-time.Hour),
		},
	} {
		_, err := database.UpsertMergeRequest(t.Context(), &pull)
		require.NoError(t, err)
	}
	return repoID
}

type federatedForgesFixture struct {
	Coordinator *federatedDaemonFixture
	NodeA       *federatedDaemonFixture
	NodeB       *federatedDaemonFixture
	Provider    *countingSyntheticProvider
	HTTPClient  *http.Client
	nodeAToken  string
	nodeBToken  string
}

func newFederatedForgesFixture(t *testing.T) *federatedForgesFixture {
	t.Helper()

	coordinator := newFederatedDaemonOrigin(t, federatedCoordinatorID, "coordinator")
	nodeA := newFederatedDaemonOrigin(t, federatedNodeAID, "node-a")
	nodeB := newFederatedDaemonOrigin(t, federatedNodeBID, "node-b")
	client := federationTLSClient(t, coordinator.HTTP, nodeA.HTTP, nodeB.HTTP)

	nodeAToken := connectFederationCredentials(
		t, coordinator, nodeA,
	)
	nodeBToken := connectFederationCredentials(
		t, coordinator, nodeB,
	)

	provider := &countingSyntheticProvider{}
	provider.Seed(t, coordinator.Database)
	seedFederatedNodeRepository(t, nodeA.Database)
	seedFederatedNodeRepository(t, nodeB.Database)
	seedFederatedWorkspace(t, nodeA.Database, "ws-node-a", 1, "draft-federation")
	seedFederatedWorkspace(t, nodeB.Database, "ws-node-b", 2, "newer-federation")

	coordinatorConfig := &config.Config{
		BasePath: "/",
		Tmux:     config.Tmux{Command: []string{"kenn-forge-no-such-tmux"}},
		Repos: []config.Repo{{
			Platform: "github", PlatformHost: "github.com",
			Owner: "acme", Name: "widget",
		}},
		Fleet: config.Fleet{
			Enabled: true, Role: config.FleetRoleCoordinator, PeerTimeout: "500ms",
			Members: []config.FleetMember{
				{NodeID: nodeA.NodeID, Name: nodeA.Name, BaseURL: nodeA.HTTP.URL, State: federation.EnrollmentActive},
				{NodeID: nodeB.NodeID, Name: nodeB.Name, BaseURL: nodeB.HTTP.URL, State: federation.EnrollmentActive},
			},
		},
	}
	coordinator.Server = newFederatedDaemonServer(
		t, coordinator, coordinatorConfig, client, false,
	)
	coordinator.Switch.Set(coordinator.Server)

	for _, node := range []*federatedDaemonFixture{nodeA, nodeB} {
		nodeConfig := &config.Config{
			BasePath: "/",
			Tmux:     config.Tmux{Command: []string{"kenn-forge-no-such-tmux"}},
			Fleet: config.Fleet{
				Enabled: true, Role: config.FleetRoleNode, PeerTimeout: "500ms",
				Coordinator: &config.FleetCoordinator{
					NodeID: coordinator.NodeID, Name: coordinator.Name,
					BaseURL: coordinator.HTTP.URL,
				},
			},
		}
		node.Server = newFederatedDaemonServer(t, node, nodeConfig, client, true)
		node.Switch.Set(node.Server)
	}

	return &federatedForgesFixture{
		Coordinator: coordinator, NodeA: nodeA, NodeB: nodeB,
		Provider: provider, HTTPClient: client,
		nodeAToken: nodeAToken, nodeBToken: nodeBToken,
	}
}

func newFederatedDaemonOrigin(
	t *testing.T, nodeID, name string,
) *federatedDaemonFixture {
	t.Helper()
	handler := newSwitchableFederationHandler()
	httpServer := httptest.NewUnstartedServer(handler)
	httpServer.StartTLS()
	t.Cleanup(httpServer.Close)
	credentials, err := federationauth.Open(filepath.Join(
		t.TempDir(), "federation-credentials.json",
	))
	require.NoError(t, err)
	return &federatedDaemonFixture{
		NodeID: nodeID, Name: name, LocalToken: name + "-local-secret",
		Database: dbtest.Open(t), Credentials: credentials,
		HTTP: httpServer, Switch: handler,
	}
}

func federationTLSClient(t *testing.T, origins ...*httptest.Server) *http.Client {
	t.Helper()
	roots := x509.NewCertPool()
	for _, origin := range origins {
		require.NotNil(t, origin.Certificate())
		roots.AddCert(origin.Certificate())
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{
		RootCAs: roots, MinVersion: tls.VersionTLS12,
	}}
	t.Cleanup(transport.CloseIdleConnections)
	return &http.Client{Transport: transport, Timeout: 5 * time.Second}
}

// connectFederationCredentials installs both directed credentials and returns
// the node-to-coordinator bearer so revocation can be asserted later.
func connectFederationCredentials(
	t *testing.T,
	coordinator, node *federatedDaemonFixture,
) string {
	t.Helper()
	nodeToCoordinator, err := coordinator.Credentials.MintInbound(
		node.NodeID, federationauth.NodeToCoordinatorScopes(),
	)
	require.NoError(t, err)
	require.NoError(t, node.Credentials.StoreOutbound(
		coordinator.NodeID, nodeToCoordinator,
		federationauth.NodeToCoordinatorScopes(),
	))
	coordinatorToNode, err := node.Credentials.MintInbound(
		coordinator.NodeID, federationauth.CoordinatorToNodeScopes(),
	)
	require.NoError(t, err)
	require.NoError(t, coordinator.Credentials.StoreOutbound(
		node.NodeID, coordinatorToNode,
		federationauth.CoordinatorToNodeScopes(),
	))
	return nodeToCoordinator
}

func newFederatedDaemonServer(
	t *testing.T,
	daemon *federatedDaemonFixture,
	cfg *config.Config,
	client *http.Client,
	activeNode bool,
) *server.Server {
	t.Helper()
	var syncer *ghclient.Syncer
	if !activeNode {
		syncer = ghclient.NewSyncer(
			nil, daemon.Database, nil, []ghclient.RepoRef{{
				Platform: platform.KindGitHub, PlatformHost: "github.com",
				Owner: "acme", Name: "widget", RepoPath: "acme/widget",
				PlatformExternalID: verifiedRepoIdentity(db.GitHubRepoIdentity(
					"github.com", "acme", "widget",
				)).PlatformRepoID,
			}}, time.Minute, nil, nil,
		)
		t.Cleanup(syncer.Stop)
	}
	srv := server.New(daemon.Database, syncer, nil, "/", cfg, server.ServerOptions{
		DaemonAccess: server.DaemonAccessOptions{
			Token: daemon.LocalToken, RequireAPIAuth: true,
		},
		FederationNodeID: daemon.NodeID, FederationNodeActive: activeNode,
		FederationCredentials: daemon.Credentials, FederationHTTPClient: client,
		WorktreeDir:                        filepath.Join(t.TempDir(), "worktrees"),
		DisableWorkspaceBackgroundMonitors: true,
		HostCheck: server.HostCheckOptions{
			Bind:                 config.HostKey{Host: "127.0.0.1", Port: "8091"},
			AllowLoopbackAnyPort: true,
		},
	})
	t.Cleanup(func() { gracefulShutdown(t, srv) })
	return srv
}

func seedFederatedNodeRepository(t *testing.T, database *db.DB) {
	t.Helper()
	identity := verifiedRepoIdentity(db.GitHubRepoIdentity(
		"github.com", "acme", "widget",
	))
	repoID, err := database.UpsertRepo(t.Context(), identity)
	require.NoError(t, err)
	require.NoError(t, database.UpdateRepoProviderMetadata(
		t.Context(), repoID, db.RepoProviderMetadata{
			PlatformRepoID: identity.PlatformRepoID,
			WebURL:         "https://github.com/acme/widget",
			CloneURL:       "https://github.com/acme/widget.git",
			DefaultBranch:  "main",
		},
	))
}

func seedFederatedWorkspace(
	t *testing.T, database *db.DB,
	id string, number int, branch string,
) {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Second)
	workspace := &db.Workspace{
		ID: id, Platform: "github", PlatformHost: "github.com",
		RepoOwner: "acme", RepoName: "widget",
		ItemType: db.WorkspaceItemTypePullRequest, ItemNumber: number,
		ItemKey: fmt.Sprint(number), GitHeadRef: branch, WorkspaceBranch: branch,
		WorktreePath: filepath.Join(t.TempDir(), id), Status: "ready", CreatedAt: now,
	}
	require.NoError(t, database.CreateWorkspaceWithLaunchSpec(
		t.Context(), workspace, db.WorkspaceLaunchSpec{
			Version: db.WorkspaceLaunchSpecVersion,
			Repository: db.WorkspaceLaunchRepository{
				Provider: "github", PlatformHost: "github.com",
				PlatformRepoID: verifiedRepoIdentity(db.GitHubRepoIdentity(
					"github.com", "acme", "widget",
				)).PlatformRepoID,
				Owner: "acme", Name: "widget",
				CloneURL: "https://github.com/acme/widget.git", DefaultBranch: "main",
			},
			ItemType: db.WorkspaceItemTypePullRequest, ItemNumber: number,
			ItemKey: fmt.Sprint(number), GitHeadRef: branch,
			Pull: &db.WorkspaceLaunchPull{
				HeadBranch: branch, HeadRepoKind: "same_repo", SnapshotRevision: 1,
			},
			SourceVisible: true, IssuedAt: now,
			SourceVisibleUntil: now.Add(db.WorkspaceLaunchSpecVisibilityLease),
		},
	))
}

func (f *federatedForgesFixture) request(
	t *testing.T,
	daemon *federatedDaemonFixture,
	method, path string,
	body io.Reader,
) *http.Response {
	t.Helper()
	request, err := http.NewRequestWithContext(
		t.Context(), method, daemon.HTTP.URL+path, body,
	)
	require.NoError(t, err)
	request.Header.Set("Authorization", "Bearer "+daemon.LocalToken)
	response, err := f.HTTPClient.Do(request)
	require.NoError(t, err)
	return response
}

func (f *federatedForgesFixture) pulls(
	t *testing.T, daemon *federatedDaemonFixture,
) []pullapi.MergeRequestResponse {
	t.Helper()
	response := f.request(t, daemon, http.MethodGet, "/api/v1/pulls?state=open", nil)
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode, string(body))
	var pulls []pullapi.MergeRequestResponse
	require.NoError(t, json.Unmarshal(body, &pulls))
	return pulls
}

func (f *federatedForgesFixture) snapshot(
	t *testing.T, daemon *federatedDaemonFixture,
) fleet.Snapshot {
	t.Helper()
	response := f.request(
		t, daemon, http.MethodGet, "/api/v1/snapshot?include_peers=true", nil,
	)
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode, string(body))
	var snapshot fleet.Snapshot
	require.NoError(t, json.Unmarshal(body, &snapshot))
	return snapshot
}

func pullNumbers(rows []pullapi.MergeRequestResponse) []int {
	numbers := make([]int, len(rows))
	for index := range rows {
		numbers[index] = rows[index].Number
	}
	return numbers
}

func pullByNumber(
	rows []pullapi.MergeRequestResponse, number int,
) *pullapi.MergeRequestResponse {
	for index := range rows {
		if rows[index].Number == number {
			return &rows[index]
		}
	}
	return nil
}

func workspaceByID(rows []fleet.WorkspaceSummary, id string) *fleet.WorkspaceSummary {
	for index := range rows {
		if rows[index].ID == id {
			return &rows[index]
		}
	}
	return nil
}

// TestFederatedForgesE2E exercises the contracts that only emerge when three
// real daemons communicate: shared provider ownership, observer-local
// workspace overlays, one-hop fleet projection, outage behavior, event
// re-stamping, protocol enforcement, and synchronous credential revocation.
func TestFederatedForgesE2E(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	fixture := newFederatedForgesFixture(t)

	coordinatorPulls := fixture.pulls(t, fixture.Coordinator)
	nodeAPulls := fixture.pulls(t, fixture.NodeA)
	nodeBPulls := fixture.pulls(t, fixture.NodeB)
	for _, rows := range [][]pullapi.MergeRequestResponse{
		coordinatorPulls, nodeAPulls, nodeBPulls,
	} {
		assert.Equal([]int{2, 1}, pullNumbers(rows),
			"every daemon preserves coordinator ordering")
	}
	assert.Nil(pullByNumber(coordinatorPulls, 1).Workspace)
	assert.Nil(pullByNumber(coordinatorPulls, 2).Workspace)
	require.NotNil(pullByNumber(nodeAPulls, 1).Workspace)
	assert.Equal("ws-node-a", pullByNumber(nodeAPulls, 1).Workspace.ID)
	assert.Nil(pullByNumber(nodeAPulls, 2).Workspace)
	require.NotNil(pullByNumber(nodeBPulls, 2).Workspace)
	assert.Equal("ws-node-b", pullByNumber(nodeBPulls, 2).Workspace.ID)
	assert.Nil(pullByNumber(nodeBPulls, 1).Workspace)
	assert.True(pullByNumber(nodeAPulls, 1).IsDraft)

	for _, daemon := range []*federatedDaemonFixture{
		fixture.Coordinator, fixture.NodeA, fixture.NodeB,
	} {
		snapshot := fixture.snapshot(t, daemon)
		assert.Equal(federation.ProtocolVersion, snapshot.ProtocolVersion)
		require.Len(snapshot.Hosts, 3)
		require.NotNil(workspaceByID(snapshot.Workspaces, "ws-node-a"))
		require.NotNil(workspaceByID(snapshot.Workspaces, "ws-node-b"))
	}
	nodeAView := fixture.snapshot(t, fixture.NodeA)
	assert.Empty(workspaceByID(nodeAView.Workspaces, "ws-node-a").FleetHostKey,
		"the connected node projects its own workspace as local")
	assert.Equal(federatedNodeBID,
		workspaceByID(nodeAView.Workspaces, "ws-node-b").FleetHostKey)

	proxied := fixture.request(
		t, fixture.Coordinator, http.MethodGet,
		"/api/v1/fleet/hosts/"+federatedNodeAID+"/workspaces", nil,
	)
	proxiedBody, err := io.ReadAll(proxied.Body)
	proxied.Body.Close()
	require.NoError(err)
	require.Equal(http.StatusOK, proxied.StatusCode, string(proxiedBody))
	assert.Contains(string(proxiedBody), "ws-node-a")
	assert.NotContains(string(proxiedBody), "ws-node-b")

	for _, daemon := range []*federatedDaemonFixture{fixture.NodeA, fixture.NodeB} {
		pulls, err := daemon.Server.MCPBackend().ListPulls(
			t.Context(), mcpserver.ItemListQuery{State: "open"},
		)
		require.NoError(err)
		assert.Equal([]int{2, 1}, []int{pulls[0].Number, pulls[1].Number})
	}
	item := mcpserver.ItemIdentity{
		Type: "pr", Provider: "github", PlatformHost: "github.com",
		PlatformRepoID: verifiedRepoIdentity(db.GitHubRepoIdentity(
			"github.com", "acme", "widget",
		)).PlatformRepoID,
		Owner: "acme", Name: "widget", Number: 1,
	}
	mutation, err := fixture.NodeA.Server.MCPBackend().SetWorkflowState(
		t.Context(), item, mcpserver.WorkflowUpdate{
			Status: "reviewing", ExpectedStatus: "new", Source: "mcp", Actor: "node-a",
		},
	)
	require.NoError(err)
	assert.Equal("reviewing", mutation.State.Status)
	workflow, err := fixture.NodeB.Server.MCPBackend().ListWorkflowStates(
		t.Context(), mcpserver.WorkflowQuery{
			Repository: mcpRepositoryIdentity(item), ItemTypes: []string{"pr"},
		},
	)
	require.NoError(err)
	require.Len(workflow.Items, 2)
	var workflowStatuses []string
	for _, row := range workflow.Items {
		if row.Identity.Number == 1 {
			workflowStatuses = append(workflowStatuses, row.Workflow.Status)
		}
	}
	assert.Equal([]string{"reviewing"}, workflowStatuses)

	require.Eventually(func() bool {
		records, _ := fixture.NodeA.Server.Hub().RingSnapshotSince(0)
		return slices.ContainsFunc(records, func(record server.RecordedEvent) bool {
			return record.Event.Type == "coordinator_connection_changed"
		})
	}, 3*time.Second, 10*time.Millisecond)
	fixture.Coordinator.Switch.offline.Store(true)
	fixture.Coordinator.HTTP.CloseClientConnections()
	outage := fixture.request(t, fixture.NodeA, http.MethodGet, "/api/v1/pulls?state=open", nil)
	outageBody, err := io.ReadAll(outage.Body)
	outage.Body.Close()
	require.NoError(err)
	assert.Equal(http.StatusServiceUnavailable, outage.StatusCode, string(outageBody))
	local := fixture.request(t, fixture.NodeA, http.MethodGet, "/api/v1/workspaces", nil)
	localBody, err := io.ReadAll(local.Body)
	local.Body.Close()
	require.NoError(err)
	assert.Equal(http.StatusOK, local.StatusCode, string(localBody))
	assert.Contains(string(localBody), "ws-node-a")

	fixture.Coordinator.Switch.offline.Store(false)
	localFloor := fixture.NodeA.Server.Hub().Generation()
	require.Eventually(func() bool {
		fixture.Coordinator.Server.Hub().Broadcast(server.Event{
			Type: "data_changed", Data: map[string]string{"source": "coordinator"},
		})
		records, stale := fixture.NodeA.Server.Hub().RingSnapshotSince(localFloor)
		return !stale && slices.ContainsFunc(records, func(record server.RecordedEvent) bool {
			return record.ID > localFloor && record.Event.Type == "data_changed"
		})
	}, 8*time.Second, 200*time.Millisecond)
	assert.Len(fixture.pulls(t, fixture.NodeA), 2, "provider reads recover after reconnect")

	protocolRequest, err := http.NewRequestWithContext(
		t.Context(), http.MethodGet,
		fixture.Coordinator.HTTP.URL+"/api/v1/pulls", nil,
	)
	require.NoError(err)
	protocolRequest.Header.Set("Authorization", "Bearer "+fixture.nodeBToken)
	protocolRequest.Header.Set(federationauth.NodeIDHeader, federatedNodeBID)
	protocolRequest.Header.Set(providerplane.ProtocolVersionHeader, "999")
	protocolResponse, err := fixture.HTTPClient.Do(protocolRequest)
	require.NoError(err)
	protocolResponse.Body.Close()
	assert.Equal(http.StatusConflict, protocolResponse.StatusCode)

	require.NoError(fixture.Coordinator.Credentials.RevokeInbound(fixture.nodeAToken))
	revokedRequest, err := http.NewRequestWithContext(
		t.Context(), http.MethodGet,
		fixture.Coordinator.HTTP.URL+"/api/v1/pulls", nil,
	)
	require.NoError(err)
	revokedRequest.Header.Set("Authorization", "Bearer "+fixture.nodeAToken)
	revokedRequest.Header.Set(federationauth.NodeIDHeader, federatedNodeAID)
	revokedRequest.Header.Set(
		providerplane.ProtocolVersionHeader,
		providerplane.ProtocolVersionHeaderValue(),
	)
	revokedResponse, err := fixture.HTTPClient.Do(revokedRequest)
	require.NoError(err)
	revokedResponse.Body.Close()
	assert.Equal(http.StatusUnauthorized, revokedResponse.StatusCode)
}

func mcpRepositoryIdentity(item mcpserver.ItemIdentity) mcpserver.RepositoryIdentity {
	return mcpserver.RepositoryIdentity{
		Provider: item.Provider, PlatformHost: item.PlatformHost,
		PlatformRepoID: item.PlatformRepoID, Owner: item.Owner, Name: item.Name,
	}
}
