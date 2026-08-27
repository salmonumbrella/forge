package fleetapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.kenn.io/forge/internal/config"
	"go.kenn.io/forge/internal/federation"
	"go.kenn.io/forge/internal/federationauth"
	"go.kenn.io/forge/internal/fleet"
	"go.kenn.io/forge/internal/server/workspaceapi"
	"go.kenn.io/forge/internal/testutil/dbtest"
)

func TestBuildFleetSnapshotMergesMemberAndDegrades(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	peer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/snapshot/raw" {
			http.Error(w, "no", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"protocolVersion":3,"nodeID":"` + testMemberNodeID + `","baseURL":"https://untrusted.example","host":{"hostname":"mbp","platform":"macos"},"projects":[{"scopedKey":"repo:/x","name":"x","rootPath":"/x"}]}`))
	}))
	defer peer.Close()

	srv := New(Deps{DB: dbtest.Open(t)})
	configureTestMembers(t, srv, testTLSClient(t, peer),
		config.FleetMember{NodeID: testMemberNodeID, Name: "mbp", BaseURL: peer.URL},
		config.FleetMember{NodeID: "cccccccccccccccccccccccccccccccc", Name: "epyc", BaseURL: "https://127.0.0.1:1"},
	)

	snap, err := srv.buildFleetSnapshot(context.Background(), true)
	require.NoError(err)
	var reachable, down int
	for _, h := range snap.Hosts {
		assert.Equal(h.ConfigKey, h.NodeID)
		if h.NodeID == testCoordinatorNodeID {
			assert.Equal(fleet.RoleCoordinator, h.FederationRole)
			assert.Equal("https://coordinator.example", h.BaseURL)
		}
		if h.NodeID == testMemberNodeID {
			assert.Equal(fleet.RoleNode, h.FederationRole)
			assert.Equal(peer.URL, h.BaseURL)
		}
		if h.Reachable {
			reachable++
		} else {
			down++
		}
	}
	assert.GreaterOrEqual(reachable, 2, "want self+mbp reachable, hosts=%+v", snap.Hosts)
	assert.Equal(1, down, "want 1 unreachable (epyc)")
}

func TestBuildFleetSnapshotSkipsMembersWhenFederationDisabled(t *testing.T) {
	peerRequests := 0
	peer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		peerRequests++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"protocolVersion":3,"nodeID":"` + testMemberNodeID + `","host":{"hostname":"mbp","platform":"macos"}}`))
	}))
	defer peer.Close()

	srv := New(Deps{DB: dbtest.Open(t), NodeID: testCoordinatorNodeID})
	srv.ApplyConfig(ConfigSnapshot{Fleet: config.Fleet{
		Role:    config.FleetRoleCoordinator,
		Members: []config.FleetMember{{NodeID: testMemberNodeID, Name: "mbp", BaseURL: peer.URL}},
	}})

	snap, err := srv.buildFleetSnapshot(context.Background(), true)
	require.NoError(t, err)
	require.Len(t, snap.Hosts, 1, "disabled federation must return local host only")
	assert.Equal(t, 0, peerRequests, "disabled federation must not fetch members")
}

func TestBuildFleetSnapshotLocalOnly(t *testing.T) {
	srv := &Handler{db: dbtest.Open(t), config: ConfigSnapshot{}}
	snap, err := srv.buildFleetSnapshot(context.Background(), false)
	require.NoError(t, err)
	require.Len(t, snap.Hosts, 1, "local-only build must yield exactly one self host")
	assert.True(t, snap.Hosts[0].Reachable, "self host must be reachable")
}

func TestBuildFleetSnapshotDefaultsToFleetNamespace(t *testing.T) {
	// The snapshot derives IDs under the default fleet namespace and the
	// daemon's stable node ID, so output is byte-stable.
	require := require.New(t)
	assert := assert.New(t)
	srv := New(Deps{DB: dbtest.Open(t), NodeID: testCoordinatorNodeID})

	snap, err := srv.buildFleetSnapshot(context.Background(), false)
	require.NoError(err)
	require.Len(snap.Hosts, 1)
	assert.Equal(testCoordinatorNodeID, snap.Hosts[0].ConfigKey)
	assert.Equal(fleet.DefaultIdentity().HostID(testCoordinatorNodeID), snap.Hosts[0].ID)
}

func TestCoordinatorProjectionKeepsDirectMemberWorkspaceRouting(t *testing.T) {
	peer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"protocolVersion":3,"nodeID":"` +
			testMemberNodeID + `","host":{"hostname":"mbp","platform":"macos"}}`))
	}))
	defer peer.Close()

	server := New(Deps{DB: dbtest.Open(t)})
	configureTestMembers(t, server, testTLSClient(t, peer), config.FleetMember{
		NodeID: testMemberNodeID, Name: "mbp", BaseURL: peer.URL,
	})
	snapshot, err := server.buildFleetSnapshot(t.Context(), true)
	require.NoError(t, err)
	for _, host := range snapshot.Hosts {
		if host.ConfigKey == testMemberNodeID {
			assert.True(t, host.OperationAvailability[fleet.OpWorkspaceWrite].Available)
			return
		}
	}
	require.FailNow(t, "member host missing from coordinator projection")
}

func TestNodeProjectionConsumesCoordinatorAggregateAndRefreshesSelf(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	coordinatorID := fleet.NodeID(testCoordinatorNodeID)
	nodeID := fleet.NodeID(testMemberNodeID)
	aggregate := fleet.NeutralSnapshot{
		ProtocolVersion: federation.ProtocolVersion,
		Hosts: []fleet.NeutralHost{
			{
				NodeID: coordinatorID, FederationRole: fleet.RoleCoordinator,
				Name: "coordinator", BaseURL: "https://coordinator.example", Reachable: true,
			},
			{
				NodeID: nodeID, FederationRole: fleet.RoleNode,
				Name: "stale-node", BaseURL: "https://node.example", Reachable: true,
			},
		},
		Workspaces: []fleet.RawWorkspace{
			{HostKey: string(coordinatorID), ID: "ws-coordinator", Status: "ready"},
			{HostKey: string(nodeID), ID: "ws-stale-self", Status: "ready"},
		},
	}
	token := "node-calls-coordinator-token-000000000000000"
	coordinator := httptest.NewTLSServer(http.HandlerFunc(func(
		writer http.ResponseWriter,
		request *http.Request,
	) {
		assert.Equal("/api/v1/snapshot/aggregate", request.URL.Path)
		assert.Equal("Bearer "+token, request.Header.Get("Authorization"))
		writer.Header().Set("Content-Type", "application/json")
		assert.NoError(json.NewEncoder(writer).Encode(aggregate))
	}))
	defer coordinator.Close()

	credentials, err := federationauth.Open(filepath.Join(
		t.TempDir(), "federation-credentials.json",
	))
	require.NoError(err)
	require.NoError(credentials.StoreOutbound(
		testCoordinatorNodeID, token, federationauth.NodeToCoordinatorScopes(),
	))
	server := New(Deps{
		DB: dbtest.Open(t), NodeID: testMemberNodeID,
		FederationActive: true,
		Credentials:      credentials, FederationHTTPClient: testTLSClient(t, coordinator),
		Config: ConfigSnapshot{Fleet: config.Fleet{
			Enabled: true, Role: config.FleetRoleNode,
			BaseURL: "https://node.example",
			Coordinator: &config.FleetCoordinator{
				NodeID: testCoordinatorNodeID, Name: "coordinator", BaseURL: coordinator.URL,
			},
		}},
		WorkspaceSnapshot: func(context.Context) (workspaceapi.FleetSnapshot, error) {
			return workspaceapi.FleetSnapshot{Workspaces: []fleet.RawWorkspace{{
				ID: "ws-fresh-self", Status: "ready",
			}}}, nil
		},
	})

	snapshot, err := server.buildFleetSnapshot(t.Context(), true)
	require.NoError(err)
	assert.False(snapshot.AggregateIncomplete)
	workspaceIDs := make(map[string]bool)
	for _, workspace := range snapshot.Workspaces {
		workspaceIDs[workspace.ID] = true
	}
	assert.True(workspaceIDs["ws-fresh-self"])
	assert.True(workspaceIDs["ws-coordinator"])
	assert.False(workspaceIDs["ws-stale-self"])
	for _, host := range snapshot.Hosts {
		assert.Equal(host.ConfigKey, host.NodeID)
		if host.ConfigKey == testMemberNodeID {
			assert.Equal(fleet.RoleNode, host.FederationRole)
			assert.Equal("https://node.example", host.BaseURL)
		}
		if host.ConfigKey == testCoordinatorNodeID {
			assert.Equal(fleet.RoleCoordinator, host.FederationRole)
			assert.Equal("https://coordinator.example", host.BaseURL)
			availability := host.OperationAvailability[fleet.OpWorkspaceWrite]
			assert.False(availability.Available)
			require.NotNil(availability.UnavailableReason)
			assert.Equal(fleet.ReasonSummaryOnly, *availability.UnavailableReason)
			return
		}
	}
	require.FailNow("coordinator host missing from node projection")
}

func TestNodeAggregateAllowsCoordinatorMemberFanoutToReachItsOwnDeadline(t *testing.T) {
	require := require.New(t)
	aggregate := fleet.NeutralSnapshot{
		ProtocolVersion: federation.ProtocolVersion,
		Hosts: []fleet.NeutralHost{{
			NodeID: fleet.NodeID(testCoordinatorNodeID), Reachable: true,
		}},
	}
	coordinator := httptest.NewTLSServer(http.HandlerFunc(func(
		writer http.ResponseWriter, _ *http.Request,
	) {
		time.Sleep(40 * time.Millisecond)
		assert.NoError(t, json.NewEncoder(writer).Encode(aggregate))
	}))
	defer coordinator.Close()
	credentials, err := federationauth.Open(filepath.Join(t.TempDir(), "credentials.json"))
	require.NoError(err)
	require.NoError(credentials.StoreOutbound(
		testCoordinatorNodeID, "node-calls-coordinator-token",
		federationauth.NodeToCoordinatorScopes(),
	))
	server := New(Deps{
		DB: dbtest.Open(t), NodeID: testMemberNodeID, FederationActive: true,
		Credentials: credentials, FederationHTTPClient: testTLSClient(t, coordinator),
		Config: ConfigSnapshot{Fleet: config.Fleet{
			Enabled: true, Role: config.FleetRoleNode, PeerTimeout: "30ms",
			Coordinator: &config.FleetCoordinator{
				NodeID: testCoordinatorNodeID, BaseURL: coordinator.URL,
			},
		}},
	})

	snapshot, err := server.buildFleetSnapshot(t.Context(), true)
	require.NoError(err)
	require.True(slices.ContainsFunc(snapshot.Hosts, func(host fleet.HostSummary) bool {
		return host.ConfigKey == testCoordinatorNodeID && host.Reachable
	}))
	assert.False(t, snapshot.AggregateIncomplete)
}

func TestNodeSnapshotMarksCoordinatorAggregateFailureIncomplete(t *testing.T) {
	require := require.New(t)
	credentials, err := federationauth.Open(filepath.Join(t.TempDir(), "credentials.json"))
	require.NoError(err)
	require.NoError(credentials.StoreOutbound(
		testCoordinatorNodeID, "node-calls-coordinator-token",
		federationauth.NodeToCoordinatorScopes(),
	))
	server := New(Deps{
		DB: dbtest.Open(t), NodeID: testMemberNodeID, FederationActive: true,
		Credentials: credentials,
		Config: ConfigSnapshot{Fleet: config.Fleet{
			Enabled: true, Role: config.FleetRoleNode, PeerTimeout: "10ms",
			Coordinator: &config.FleetCoordinator{
				NodeID: testCoordinatorNodeID, BaseURL: "https://127.0.0.1:1",
			},
		}},
	})

	snapshot, err := server.buildFleetSnapshot(t.Context(), true)

	require.NoError(err)
	assert.True(t, snapshot.AggregateIncomplete)
}
