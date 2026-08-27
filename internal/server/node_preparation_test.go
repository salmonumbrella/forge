package server

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/forge/internal/config"
	"go.kenn.io/forge/internal/db"
	"go.kenn.io/forge/internal/federation"
	"go.kenn.io/forge/internal/federationauth"
	"go.kenn.io/forge/internal/gitclone"
	"go.kenn.io/forge/internal/providerplane"
	"go.kenn.io/forge/internal/testutil/dbtest"
	"go.kenn.io/forge/internal/tokenauth"
)

const (
	preparationCoordinatorNodeID = "0123456789abcdef0123456789abcdef"
	preparationLocalNodeID       = "fedcba9876543210fedcba9876543210"
	preparationEnrollmentID      = "11111111111111111111111111111111"
)

func openFederationPreparationStores(
	t *testing.T, name string,
) (*federation.Store, *federationauth.Store) {
	t.Helper()
	dir := t.TempDir()
	enrollments, err := federation.Open(
		filepath.Join(dir, name+"-enrollments.json"), federation.StoreOptions{},
	)
	require.NoError(t, err)
	credentials, err := federationauth.Open(filepath.Join(dir, name+"-credentials.json"))
	require.NoError(t, err)
	return enrollments, credentials
}

func prepareNodeRequest(
	t *testing.T, server *httptest.Server,
) (NodePreparationReport, int) {
	t.Helper()
	request, err := http.NewRequestWithContext(
		t.Context(), http.MethodPost, server.URL+"/api/v1/fleet/prepare-node",
		bytes.NewReader([]byte(`{}`)),
	)
	require.NoError(t, err)
	request.Header.Set("Authorization", "Bearer local-secret")
	request.Header.Set("Content-Type", "application/json")
	response, err := server.Client().Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	var report NodePreparationReport
	if response.StatusCode == http.StatusOK {
		require.NoError(t, json.NewDecoder(response.Body).Decode(&report))
	}
	return report, response.StatusCode
}

func TestAbortPreparationFromNodeShapedServerRequiresRestart(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	enrollments, credentials := openFederationPreparationStores(t, "abort-node")
	require.NoError(enrollments.SaveLocal(t.Context(), federation.LocalEnrollment{
		EnrollmentID: preparationEnrollmentID, NodeID: preparationLocalNodeID,
		NodeBaseURL:     "https://node.example",
		CoordinatorID:   preparationCoordinatorNodeID,
		CoordinatorURL:  "https://coordinator.example",
		ProtocolVersion: federation.ProtocolVersion, State: federation.EnrollmentPending,
		ExpiresAt: time.Now().Add(-time.Minute),
	}))
	srv, _, _ := setupTestServerWithConfigContentAndOptions(t, `
host = "127.0.0.1"
port = 8091

[api]
require_auth = true

[fleet]
enabled = true
role = "node"
base_url = "https://node.example"

[fleet.coordinator]
node_id = "0123456789abcdef0123456789abcdef"
base_url = "https://coordinator.example"
`, &mockGH{}, ServerOptions{
		DaemonAccess:          DaemonAccessOptions{Token: "local-secret", RequireAPIAuth: true},
		FederationCredentials: credentials, FederationEnrollments: enrollments,
		FederationNodeID: preparationLocalNodeID, HostCheckAllowLoopbackAnyPort: true,
	})
	daemon := httptest.NewServer(srv)
	t.Cleanup(daemon.Close)
	request, err := http.NewRequestWithContext(
		t.Context(), http.MethodPost, daemon.URL+"/api/v1/fleet/prepare-node/abort",
		bytes.NewReader([]byte(`{}`)),
	)
	require.NoError(err)
	request.Header.Set("Authorization", "Bearer local-secret")
	request.Header.Set("Content-Type", "application/json")
	response, err := daemon.Client().Do(request)
	require.NoError(err)
	t.Cleanup(func() { require.NoError(response.Body.Close()) })
	var report struct {
		ProviderWritesOpen bool `json:"provider_writes_open"`
		RestartRequired    bool `json:"restart_required"`
	}
	require.NoError(json.NewDecoder(response.Body).Decode(&report))

	assert.Equal(http.StatusOK, response.StatusCode)
	assert.False(report.ProviderWritesOpen)
	assert.True(report.RestartRequired)
}

func TestPrepareFederationNodeSealsAndPersistsRoleThroughDaemon(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	coordinatorDB := dbtest.Open(t)
	coordinatorEnrollments, coordinatorCredentials := openFederationPreparationStores(t, "coordinator")
	coordinatorConfig := &config.Config{Fleet: config.Fleet{
		Enabled: true, Role: config.FleetRoleCoordinator,
	}}
	coordinator := New(coordinatorDB, nil, nil, "/", coordinatorConfig, ServerOptions{
		DaemonAccess:                  DaemonAccessOptions{Token: "coordinator-local", RequireAPIAuth: true},
		FederationCredentials:         coordinatorCredentials,
		FederationEnrollments:         coordinatorEnrollments,
		FederationNodeID:              preparationCoordinatorNodeID,
		HostCheckAllowLoopbackAnyPort: true,
	})
	t.Cleanup(func() { gracefulShutdown(t, coordinator) })
	coordinatorHTTP := httptest.NewTLSServer(coordinator)
	t.Cleanup(coordinatorHTTP.Close)

	nodeEnrollments, nodeCredentials := openFederationPreparationStores(t, "node")
	nodeToCoordinator := "node-to-coordinator-preparation-token"
	coordinatorToNode := "coordinator-to-node-preparation-token"
	require.NoError(coordinatorCredentials.StoreInbound(
		preparationLocalNodeID, nodeToCoordinator,
		federationauth.PendingNodeToCoordinatorScopes(),
	))
	require.NoError(nodeCredentials.StoreOutbound(
		preparationCoordinatorNodeID, nodeToCoordinator,
		federationauth.PendingNodeToCoordinatorScopes(),
	))
	require.NoError(nodeCredentials.StoreInbound(
		preparationCoordinatorNodeID, coordinatorToNode,
		federationauth.PendingCoordinatorToNodeScopes(),
	))
	require.NoError(coordinatorCredentials.StoreOutbound(
		preparationLocalNodeID, coordinatorToNode,
		federationauth.PendingCoordinatorToNodeScopes(),
	))

	token, err := coordinatorEnrollments.CreateOneTimeToken(federation.Identity{
		NodeID: preparationCoordinatorNodeID, BaseURL: coordinatorHTTP.URL,
	}, time.Now().Add(time.Minute))
	require.NoError(err)
	_, err = coordinatorEnrollments.Begin(t.Context(), token.Token, federation.JoinRequest{
		EnrollmentID: preparationEnrollmentID, NodeID: preparationLocalNodeID,
		Platform: "linux", BaseURL: "https://node.example",
		ProtocolVersion:       federation.ProtocolVersion,
		CoordinatorCredential: coordinatorToNode,
	})
	require.NoError(err)
	require.NoError(nodeEnrollments.SaveLocal(t.Context(), federation.LocalEnrollment{
		EnrollmentID: preparationEnrollmentID, NodeID: preparationLocalNodeID,
		NodePlatform: "linux", NodeBaseURL: "https://node.example",
		CoordinatorID:   preparationCoordinatorNodeID,
		CoordinatorURL:  coordinatorHTTP.URL,
		ProtocolVersion: federation.ProtocolVersion, State: federation.EnrollmentPending,
		ExpiresAt: token.ExpiresAt, PreparationRequired: true,
	}))

	nodeDB := dbtest.Open(t)
	nodeMRID := seedPR(t, nodeDB, "acme", "widget", 7)
	seedPR(t, coordinatorDB, "acme", "widget", 7)
	seedPR(t, coordinatorDB, "acme", "project-only", 8)
	projectRepoID, err := nodeDB.UpsertRepo(t.Context(), db.RepoIdentity{
		Platform: "github", PlatformHost: "github.com",
		Owner: "acme", Name: "project-only",
	})
	require.NoError(err)
	_, err = nodeDB.CreateProject(t.Context(), db.CreateProjectInput{
		DisplayName: "Project only", LocalPath: t.TempDir(),
		RepoID: sql.NullInt64{Int64: projectRepoID, Valid: true}, DefaultBranch: "main",
	})
	require.NoError(err)
	_, err = coordinatorDB.WriteDB().ExecContext(
		t.Context(), "DELETE FROM forge_item_workflow_state",
	)
	require.NoError(err)
	require.NoError(nodeDB.SetKanbanState(t.Context(), nodeMRID, "reviewing"))
	nodeConfigPath := filepath.Join(t.TempDir(), "forge.toml")
	writeConfigToml(t, nodeConfigPath, fmt.Sprintf(`
host = "127.0.0.1"
port = 8091
data_dir = %q

[api]
require_auth = true

[fleet]
enabled = true
role = "coordinator"
base_url = "https://coordinator.example"

[fleet.coordinator]
node_id = %q
base_url = %q
`, t.TempDir(), preparationCoordinatorNodeID, coordinatorHTTP.URL))
	nodeConfig, err := config.Load(nodeConfigPath)
	require.NoError(err)
	node := NewWithConfig(nodeDB, nil, nil, nil, nodeConfig, nodeConfigPath, ServerOptions{
		DaemonAccess:                  DaemonAccessOptions{Token: "local-secret", RequireAPIAuth: true},
		FederationCredentials:         nodeCredentials,
		FederationEnrollments:         nodeEnrollments,
		FederationNodeID:              preparationLocalNodeID,
		FederationHTTPClient:          coordinatorHTTP.Client(),
		HostCheckAllowLoopbackAnyPort: true,
	})
	t.Cleanup(func() { gracefulShutdown(t, node) })
	nodeHTTP := httptest.NewServer(node)
	t.Cleanup(nodeHTTP.Close)

	releaseWrite, err := node.providerWriteGate.Admit(t.Context())
	require.NoError(err)
	t.Cleanup(releaseWrite)
	draining, status := prepareNodeRequest(t, nodeHTTP)
	require.Equal(http.StatusOK, status)
	assert.False(draining.ReadyToActivate)
	assert.Equal(1, draining.InFlightProviderWrites)
	receipts, err := nodeDB.ListNodePreparationReceipts(t.Context())
	require.NoError(err)
	assert.Empty(receipts, "provider state must not be handed off while an admitted write is active")

	require.NoError(nodeDB.SetKanbanState(t.Context(), nodeMRID, "waiting"))
	releaseWrite()
	first, status := prepareNodeRequest(t, nodeHTTP)
	require.Equal(http.StatusOK, status)
	assert.True(first.ReadyToActivate)
	assert.NotEmpty(first.PreparationSeal)
	assert.Empty(first.HandoffConflicts)
	assert.Empty(first.HandoffErrors)
	assert.True(first.RestartRequired)
	receipts, err = nodeDB.ListNodePreparationReceipts(t.Context())
	require.NoError(err)
	assert.NotEmpty(receipts, "non-empty provider state must traverse the pending handoff client")
	persistedConfig, err := config.Load(nodeConfigPath)
	require.NoError(err)
	assert.Equal(config.FleetRoleNode, persistedConfig.Fleet.RoleOrDefault())
	projectRepo, err := nodeDB.GetRepoByID(t.Context(), projectRepoID)
	require.NoError(err)
	require.NotNil(projectRepo)
	assert.NotEmpty(projectRepo.PlatformRepoID)

	localState, err := nodeDB.GetNodePreparation(t.Context())
	require.NoError(err)
	assert.Equal(db.NodePreparationSealed, localState.Phase)
	assert.Equal(first.PreparationSeal, localState.PreparationSeal)
	localEnrollment, ok := nodeEnrollments.Local()
	require.True(ok)
	require.NotNil(localEnrollment.Preparation)
	assert.Equal(first.PreparationSeal, localEnrollment.Preparation.Seal)
	assert.Equal(localState.PreparationDigest, localEnrollment.Preparation.PreparationDigest)
	coordinatorSeal, err := coordinatorDB.GetNodePreparationSeal(
		t.Context(), preparationEnrollmentID,
	)
	require.NoError(err)
	require.NotNil(coordinatorSeal)
	assert.Equal(first.PreparationSeal, coordinatorSeal.Seal)

	retry, status := prepareNodeRequest(t, nodeHTTP)
	require.Equal(http.StatusOK, status)
	assert.True(retry.ReadyToActivate)
	assert.Equal(first.PreparationSeal, retry.PreparationSeal)
	assert.Equal(config.FleetRoleNode, nodeConfig.Fleet.Role)
}

func TestPersistPreparedNodeRoleKeepsSealAndMembershipGuards(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	enrollments, credentials := openFederationPreparationStores(t, "persist-role")
	local := federation.LocalEnrollment{
		EnrollmentID: preparationEnrollmentID, NodeID: preparationLocalNodeID,
		NodeBaseURL:   "https://node.example",
		CoordinatorID: preparationCoordinatorNodeID, CoordinatorURL: "https://coordinator.example",
		ProtocolVersion: federation.ProtocolVersion, State: federation.EnrollmentPending,
		ExpiresAt: time.Now().Add(time.Minute), PreparationStarted: true,
		PreparationRequired: true,
	}
	require.NoError(enrollments.SaveLocal(t.Context(), local))
	require.NoError(enrollments.SaveLocalPreparationSeal(
		t.Context(), federation.LocalPreparationSeal{
			EnrollmentID: local.EnrollmentID, NodeID: local.NodeID,
			CoordinatorID: local.CoordinatorID, ProtocolVersion: local.ProtocolVersion,
			PreparationDigest: "digest", Seal: "sealed-proof",
		},
	))
	configPath := filepath.Join(t.TempDir(), "forge.toml")
	writeConfigToml(t, configPath, fmt.Sprintf(`
host = "127.0.0.1"
port = 8091
data_dir = %q

[api]
require_auth = true

[fleet]
enabled = true
role = "coordinator"
base_url = "https://coordinator.example"

[fleet.coordinator]
node_id = %q
base_url = %q

[[fleet.members]]
node_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
base_url = "https://member.example"
state = "active"
`, t.TempDir(), local.CoordinatorID, local.CoordinatorURL))
	cfg, err := config.Load(configPath)
	require.NoError(err)
	srv := NewWithConfig(dbtest.Open(t), nil, nil, nil, cfg, configPath, ServerOptions{
		FederationEnrollments: enrollments, FederationCredentials: credentials,
		FederationNodeID: local.NodeID, DisableWorkspaceBackgroundMonitors: true,
	})
	t.Cleanup(func() { gracefulShutdown(t, srv) })
	sealRequest := db.NodePreparationSealRequest{
		EnrollmentID: local.EnrollmentID, NodeID: local.NodeID,
		CoordinatorNodeID: local.CoordinatorID, ProtocolVersion: local.ProtocolVersion,
	}
	seal := db.NodePreparationSeal{
		NodePreparationSealRequest: sealRequest,
		Seal:                       "sealed-proof",
	}

	mismatched := seal
	mismatched.Seal = "different-proof"
	require.ErrorIs(
		srv.persistPreparedNodeRole(t.Context(), local, mismatched),
		federation.ErrPreparationSealMismatch,
	)
	require.ErrorContains(
		srv.persistPreparedNodeRole(t.Context(), local, seal),
		"revoke or migrate coordinator members",
	)
	unchanged, err := config.Load(configPath)
	require.NoError(err)
	assert.Equal(config.FleetRoleCoordinator, unchanged.Fleet.RoleOrDefault())
	assert.Len(unchanged.Fleet.Members, 1)
}

func TestNodePreparationRejectsFilesystemLaunchSpecBeforePersistence(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	database := dbtest.Open(t)
	seedWorkspace(t, database, "invalid-launch-spec", "acme", "widget", db.WorkspaceItemTypePullRequest, 42)
	issuedAt := time.Now().UTC().Truncate(time.Second)
	spec := db.WorkspaceLaunchSpec{
		Version: db.WorkspaceLaunchSpecVersion,
		Repository: db.WorkspaceLaunchRepository{
			Provider: "github", PlatformHost: "github.com", PlatformRepoID: "repo-acme-widget",
			Owner: "acme", Name: "widget", CloneURL: "file:///tmp/acme/widget.git",
			DefaultBranch: "main",
		},
		ItemType: db.WorkspaceItemTypePullRequest, ItemNumber: 42,
		ItemKey: "42", GitHeadRef: "feature/invalid-launch-spec",
		Pull: &db.WorkspaceLaunchPull{
			HeadBranch: "feature/invalid-launch-spec", HeadRepoKind: "same_repo",
			SnapshotRevision: 1,
		},
		SourceVisible: true, IssuedAt: issuedAt,
		SourceVisibleUntil: issuedAt.Add(db.WorkspaceLaunchSpecVisibilityLease),
	}
	encoded, err := json.Marshal(spec)
	require.NoError(err)
	client := providerPlaneClientFunc(func(
		_ context.Context, scope federationauth.Scope, _ *http.Request,
	) (*http.Response, error) {
		assert.Equal(federationauth.ScopeProviderRead, scope)
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewReader(encoded)),
		}, nil
	})
	report := NodePreparationReport{HandoffErrors: []string{}}
	server := &Server{db: database, now: time.Now}

	server.refreshNodePreparationLaunchSpecs(t.Context(), client, &report)

	require.Len(report.HandoffErrors, 1)
	assert.Contains(report.HandoffErrors[0], "invalid coordinator launch specification")
	persisted, err := database.GetWorkspaceLaunchSpec(t.Context(), "invalid-launch-spec")
	require.NoError(err)
	assert.Nil(persisted)
}

func TestNodePreparationRequiresCredentialBeforePersistingLaunchSpec(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	database := dbtest.Open(t)
	seedWorkspace(t, database, "missing-credential", "acme", "widget", db.WorkspaceItemTypePullRequest, 42)
	issuedAt := time.Now().UTC().Truncate(time.Second)
	spec := db.WorkspaceLaunchSpec{
		Version: db.WorkspaceLaunchSpecVersion,
		Repository: db.WorkspaceLaunchRepository{
			Provider: "github", PlatformHost: "github.com", PlatformRepoID: "repo-acme-widget",
			Owner: "acme", Name: "widget", CloneURL: "https://github.com/acme/widget.git",
			DefaultBranch: "main",
		},
		ItemType: db.WorkspaceItemTypePullRequest, ItemNumber: 42,
		ItemKey: "42", GitHeadRef: "feature/missing-credential",
		Pull: &db.WorkspaceLaunchPull{
			HeadBranch: "feature/missing-credential", HeadRepoKind: "same_repo",
			SnapshotRevision: 1,
		},
		SourceVisible: true, IssuedAt: issuedAt,
		SourceVisibleUntil: issuedAt.Add(db.WorkspaceLaunchSpecVisibilityLease),
	}
	encoded, err := json.Marshal(spec)
	require.NoError(err)
	client := providerPlaneClientFunc(func(
		_ context.Context, _ federationauth.Scope, _ *http.Request,
	) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(encoded)),
		}, nil
	})
	report := NodePreparationReport{HandoffErrors: []string{}}
	server := &Server{
		db: database, now: time.Now, clones: gitclone.New(t.TempDir(), nil),
	}

	server.refreshNodePreparationLaunchSpecs(t.Context(), client, &report)

	require.Len(report.HandoffErrors, 1)
	assert.Contains(report.HandoffErrors[0], "git credential unavailable")
	persisted, err := database.GetWorkspaceLaunchSpec(t.Context(), "missing-credential")
	require.NoError(err)
	assert.Nil(persisted)
}

func TestNodePreparationRefreshFollowsStableRepositoryRename(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	database := dbtest.Open(t)
	const workspaceID = "renamed-launch-spec"
	seedWorkspace(t, database, workspaceID, "acme", "widget", db.WorkspaceItemTypePullRequest, 42)
	now := time.Now().UTC().Truncate(time.Second)
	current := db.WorkspaceLaunchSpec{
		Version: db.WorkspaceLaunchSpecVersion,
		Repository: db.WorkspaceLaunchRepository{
			Provider: "github", PlatformHost: "github.com", PlatformRepoID: "repo-acme-widget",
			Owner: "acme", Name: "widget", CloneURL: "https://github.com/acme/widget.git",
			DefaultBranch: "main",
		},
		ItemType: db.WorkspaceItemTypePullRequest, ItemNumber: 42,
		ItemKey: "42", GitHeadRef: "feature/" + workspaceID,
		Pull: &db.WorkspaceLaunchPull{
			HeadBranch: "feature/" + workspaceID, HeadRepoKind: "same_repo",
			SnapshotRevision: 1,
		},
		SourceVisible: true,
		IssuedAt:      now.Add(-db.WorkspaceLaunchSpecVisibilityLease - time.Minute),
	}
	current.SourceVisibleUntil = current.IssuedAt.Add(db.WorkspaceLaunchSpecVisibilityLease)
	_, accepted, err := database.ReconcileRepositoryObservation(
		t.Context(), db.RepoIdentity{
			Platform: "github", PlatformHost: "github.com",
			PlatformRepoID: current.Repository.PlatformRepoID,
			Owner:          current.Repository.Owner, Name: current.Repository.Name,
		}, current.IssuedAt,
	)
	require.NoError(err)
	require.True(accepted)
	require.NoError(database.PutWorkspaceLaunchSpec(t.Context(), workspaceID, current))

	refreshed := current
	refreshed.Repository.Owner = "acme-renamed"
	refreshed.Repository.Name = "widget-renamed"
	refreshed.Repository.CloneURL = "https://github.com/acme-renamed/widget-renamed.git"
	refreshed.IssuedAt = now
	refreshed.SourceVisibleUntil = now.Add(db.WorkspaceLaunchSpecVisibilityLease)
	_, accepted, err = database.ReconcileRepositoryObservation(
		t.Context(), db.RepoIdentity{
			Platform: "github", PlatformHost: "github.com",
			PlatformRepoID: refreshed.Repository.PlatformRepoID,
			Owner:          refreshed.Repository.Owner, Name: refreshed.Repository.Name,
		}, now,
	)
	require.NoError(err)
	require.True(accepted)
	encoded, err := json.Marshal(refreshed)
	require.NoError(err)
	client := providerPlaneClientFunc(func(
		_ context.Context, _ federationauth.Scope, request *http.Request,
	) (*http.Response, error) {
		var body providerplane.WorkspaceLaunchRequest
		require.NoError(json.NewDecoder(request.Body).Decode(&body))
		assert.Equal(current.Repository.PlatformRepoID, body.PlatformRepoID)
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewReader(encoded)),
		}, nil
	})
	report := NodePreparationReport{HandoffErrors: []string{}}
	const tokenEnv = "KENN_FORGE_TEST_PREPARATION_GIT_TOKEN"
	t.Setenv(tokenEnv, "token")
	source := tokenauth.NewManagedSource(tokenauth.Descriptor{
		Key: tokenauth.Key{Platform: "github", Host: "github.com"},
		Candidates: []tokenauth.Candidate{{
			Kind: tokenauth.SourceKindEnv, EnvName: tokenEnv,
		}},
	}, tokenauth.Options{})
	server := &Server{
		db: database, now: func() time.Time { return now },
		clones: gitclone.New(t.TempDir(), gitclone.HostSources{"github.com": source}),
	}

	server.refreshNodePreparationLaunchSpecs(t.Context(), client, &report)

	assert.Empty(report.HandoffErrors)
	workspace, err := database.GetWorkspace(t.Context(), workspaceID)
	require.NoError(err)
	require.NotNil(workspace)
	assert.Equal("acme-renamed", workspace.RepoOwner)
	assert.Equal("widget-renamed", workspace.RepoName)
}

func TestCoordinatorPreparationSealMustMatchRequestedBinding(t *testing.T) {
	require := require.New(t)
	request := db.NodePreparationSealRequest{
		EnrollmentID: preparationEnrollmentID, NodeID: preparationLocalNodeID,
		CoordinatorNodeID: preparationCoordinatorNodeID,
		ProtocolVersion:   federation.ProtocolVersion,
		MigrationVersion:  db.WorkspaceLaunchSpecMigrationVersion,
		ReceiptsDigest:    "receipts", DrainedAckGeneration: 1,
	}
	var err error
	request.PreparationDigest, err = db.NodePreparationSealDigest(request)
	require.NoError(err)
	valid := db.NodePreparationSeal{
		NodePreparationSealRequest: request,
		Seal:                       "opaque-seal", CreatedAt: time.Now().UTC(),
	}
	require.NoError(validateCoordinatorPreparationSeal(request, valid))

	different := valid
	different.ReceiptsDigest = "different"
	require.Error(validateCoordinatorPreparationSeal(request, different))
	incomplete := valid
	incomplete.Seal = ""
	require.Error(validateCoordinatorPreparationSeal(request, incomplete))
}
