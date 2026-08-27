package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.kenn.io/forge/internal/config"
	"go.kenn.io/forge/internal/db"
	"go.kenn.io/forge/internal/federation"
	"go.kenn.io/forge/internal/federationauth"
	"go.kenn.io/forge/internal/providerplane"
	"go.kenn.io/forge/internal/testutil/dbtest"
)

const (
	startupNodeID        = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	startupCoordinatorID = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	startupEnrollmentID  = "cccccccccccccccccccccccccccccccc"
	startupSeal          = "startup-seal"
	startupDigest        = "startup-digest"
)

func TestFederationNodeStartupRequiresLocalSealWithoutCoordinatorTraffic(t *testing.T) {
	var requests atomic.Int32
	coordinator := httptest.NewTLSServer(http.HandlerFunc(func(
		w http.ResponseWriter, _ *http.Request,
	) {
		requests.Add(1)
		http.Error(w, "unexpected", http.StatusInternalServerError)
	}))
	t.Cleanup(coordinator.Close)
	database := dbtest.Open(t)
	enrollments, credentials, cfg := nodeStartupFixture(
		t, database, coordinator.URL, false,
	)

	status := activateFederationNodeAtStartup(
		t.Context(), database, cfg, startupNodeID,
		enrollments, credentials, coordinator.Client(),
	)
	assert.Equal(t, federationStartupActionRequired, status.State)
	assert.Contains(t, status.Reason, "seal")
	assert.Zero(t, requests.Load())
}

func TestFederationNodeStartupActivatesMatchingSealAndRetriesSafely(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	var activations atomic.Int32
	var membershipWrites atomic.Int32
	var coordinatorActive atomic.Bool
	coordinator := httptest.NewTLSServer(http.HandlerFunc(func(
		w http.ResponseWriter, r *http.Request,
	) {
		assert.Equal("Bearer node-to-coordinator", r.Header.Get("Authorization"))
		assert.Equal(startupNodeID, r.Header.Get(federationauth.NodeIDHeader))
		assert.Equal(providerplane.ProtocolVersionHeaderValue(),
			r.Header.Get(providerplane.ProtocolVersionHeader))
		switch r.URL.Path {
		case "/api/v1/federation/identity":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"node_id":          startupCoordinatorID,
				"protocol_version": federation.ProtocolVersion,
			})
		case "/api/v1/federation/enrollments/" + startupEnrollmentID + "/activate":
			activations.Add(1)
			var body map[string]any
			assert.NoError(json.NewDecoder(r.Body).Decode(&body))
			assert.Equal(startupSeal, body["preparation_seal"])
			if coordinatorActive.CompareAndSwap(false, true) {
				membershipWrites.Add(1)
				_, _ = w.Write([]byte("{"))
				return
			}
			_ = json.NewEncoder(w).Encode(federation.Enrollment{
				ID: startupEnrollmentID, NodeID: startupNodeID,
				CoordinatorID:   startupCoordinatorID,
				ProtocolVersion: federation.ProtocolVersion,
				State:           federation.EnrollmentActive,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(coordinator.Close)
	database := dbtest.Open(t)
	enrollments, credentials, cfg := nodeStartupFixture(
		t, database, coordinator.URL, true,
	)

	status := activateFederationNodeAtStartup(
		t.Context(), database, cfg, startupNodeID,
		enrollments, credentials, coordinator.Client(),
	)
	require.Equal(federationStartupActive, status.State, status.Reason)
	assert.Equal(int32(2), activations.Load())
	assert.Equal(int32(1), membershipWrites.Load(),
		"a lost activation response must not duplicate membership")
	local, ok := enrollments.Local()
	require.True(ok)
	assert.Equal(federation.EnrollmentActive, local.State)
	assert.False(local.PreparationRequired)
}

func TestFederationNodeStartupKeepsActiveBindingDormantWhileDisabled(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	var requests atomic.Int32
	coordinator := httptest.NewTLSServer(http.HandlerFunc(func(
		w http.ResponseWriter, _ *http.Request,
	) {
		requests.Add(1)
		http.Error(w, "unexpected", http.StatusInternalServerError)
	}))
	t.Cleanup(coordinator.Close)
	database := dbtest.Open(t)
	enrollments, credentials, cfg := nodeStartupFixture(
		t, database, coordinator.URL, true,
	)
	require.NoError(enrollments.MarkLocalActive(t.Context(), startupEnrollmentID))
	require.NoError(credentials.UpdateInboundScopes(
		startupCoordinatorID, federationauth.CoordinatorToNodeScopes(),
	))
	require.NoError(credentials.UpdateOutboundScopes(
		startupCoordinatorID, federationauth.NodeToCoordinatorScopes(),
	))
	cfg.Fleet.Enabled = false

	status := activateFederationNodeAtStartup(
		t.Context(), database, cfg, startupNodeID,
		enrollments, credentials, coordinator.Client(),
	)

	assert.Equal(federationStartupActive, status.State, status.Reason)
	assert.Zero(requests.Load(), "disabled federation must stay dormant")
}

func TestDisabledFederationNodeStartupRepairsInterruptedCredentialPromotion(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(*testing.T, *federationauth.Store)
	}{
		{name: "active enrollment persisted before either credential"},
		{
			name: "inbound credential persisted before outbound credential",
			prepare: func(t *testing.T, credentials *federationauth.Store) {
				require.NoError(t, credentials.UpdateInboundScopes(
					startupCoordinatorID, federationauth.CoordinatorToNodeScopes(),
				))
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert := assert.New(t)
			require := require.New(t)
			database := dbtest.Open(t)
			enrollments, credentials, cfg := nodeStartupFixture(
				t, database, "https://coordinator.example", true,
			)
			require.NoError(enrollments.MarkLocalActive(t.Context(), startupEnrollmentID))
			if test.prepare != nil {
				test.prepare(t, credentials)
			}
			cfg.Fleet.Enabled = false

			status := activateFederationNodeAtStartup(
				t.Context(), database, cfg, startupNodeID,
				enrollments, credentials, http.DefaultClient,
			)

			require.Equal(federationStartupActive, status.State, status.Reason)
			principal, ok := credentials.Authenticate("coordinator-to-node")
			require.True(ok)
			expectedInbound := make(map[federationauth.Scope]struct{})
			for _, scope := range federationauth.CoordinatorToNodeScopes() {
				expectedInbound[scope] = struct{}{}
			}
			assert.Equal(federationauth.Principal{
				NodeID: startupCoordinatorID,
				Scopes: expectedInbound,
			}, principal)
			outbound, ok := credentials.Outbound(startupCoordinatorID)
			require.True(ok)
			assert.Equal(federationauth.NodeToCoordinatorScopes(), outbound.Scopes)
		})
	}
}

func TestFederationNodeStartupSuppressesIncompatibleCoordinator(t *testing.T) {
	coordinator := httptest.NewTLSServer(http.HandlerFunc(func(
		w http.ResponseWriter, r *http.Request,
	) {
		if r.URL.Path != "/api/v1/federation/identity" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"node_id":          startupCoordinatorID,
			"protocol_version": federation.ProtocolVersion + 1,
		})
	}))
	t.Cleanup(coordinator.Close)
	database := dbtest.Open(t)
	enrollments, credentials, cfg := nodeStartupFixture(
		t, database, coordinator.URL, true,
	)

	status := activateFederationNodeAtStartup(
		t.Context(), database, cfg, startupNodeID,
		enrollments, credentials, coordinator.Client(),
	)
	assert.Equal(t, federationStartupIncompatible, status.State)
}

func TestFederationNodeStartupLoadsOldProtocolAsIncompatibleWithoutTraffic(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	var requests atomic.Int32
	coordinator := httptest.NewTLSServer(http.HandlerFunc(func(
		w http.ResponseWriter, _ *http.Request,
	) {
		requests.Add(1)
		http.Error(w, "unexpected", http.StatusInternalServerError)
	}))
	t.Cleanup(coordinator.Close)
	path := filepath.Join(t.TempDir(), "enrollments.json")
	oldProtocol := federation.ProtocolVersion - 1
	contents, err := json.Marshal(map[string]any{
		"version": 1, "tokens": []any{}, "enrollments": []any{},
		"local": federation.LocalEnrollment{
			EnrollmentID: startupEnrollmentID, NodeID: startupNodeID,
			NodePlatform: "linux", NodeBaseURL: "https://node.example",
			CoordinatorID: startupCoordinatorID, CoordinatorURL: coordinator.URL,
			ProtocolVersion: oldProtocol, State: federation.EnrollmentPending,
			ExpiresAt: time.Now().Add(time.Minute), PreparationStarted: true,
			PreparationRequired: true,
			Preparation: &federation.LocalPreparationSeal{
				EnrollmentID: startupEnrollmentID, NodeID: startupNodeID,
				CoordinatorID: startupCoordinatorID, ProtocolVersion: oldProtocol,
				PreparationDigest: startupDigest, Seal: startupSeal,
			},
		},
	})
	require.NoError(err)
	require.NoError(os.WriteFile(path, contents, 0o600))
	enrollments, err := federation.Open(path, federation.StoreOptions{})
	require.NoError(err)
	cfg := &config.Config{Fleet: config.Fleet{
		Enabled: true, Role: config.FleetRoleNode,
		Coordinator: &config.FleetCoordinator{
			NodeID: startupCoordinatorID, BaseURL: coordinator.URL,
		},
	}}

	status := activateFederationNodeAtStartup(
		t.Context(), nil, cfg, startupNodeID, enrollments, nil,
		coordinator.Client(),
	)
	assert.Equal(federationStartupIncompatible, status.State)
	assert.Zero(requests.Load())
}

func nodeStartupFixture(
	t *testing.T,
	database *db.DB,
	coordinatorURL string,
	withSeal bool,
) (*federation.Store, *federationauth.Store, *config.Config) {
	t.Helper()
	dir := t.TempDir()
	enrollments, err := federation.Open(
		filepath.Join(dir, "enrollments.json"), federation.StoreOptions{},
	)
	require.NoError(t, err)
	credentials, err := federationauth.Open(filepath.Join(dir, "credentials.json"))
	require.NoError(t, err)
	require.NoError(t, credentials.StoreOutbound(
		startupCoordinatorID, "node-to-coordinator",
		federationauth.PendingNodeToCoordinatorScopes(),
	))
	require.NoError(t, credentials.StoreInbound(
		startupCoordinatorID, "coordinator-to-node",
		federationauth.PendingCoordinatorToNodeScopes(),
	))
	require.NoError(t, enrollments.SaveLocal(t.Context(), federation.LocalEnrollment{
		EnrollmentID: startupEnrollmentID, NodeID: startupNodeID,
		NodePlatform: "linux", NodeBaseURL: "https://node.example",
		CoordinatorID: startupCoordinatorID, CoordinatorURL: coordinatorURL,
		ProtocolVersion: federation.ProtocolVersion,
		State:           federation.EnrollmentPending, ExpiresAt: time.Now().Add(time.Minute),
		PreparationStarted: withSeal, PreparationRequired: true,
	}))
	if withSeal {
		require.NoError(t, enrollments.SaveLocalPreparationSeal(
			t.Context(), federation.LocalPreparationSeal{
				EnrollmentID: startupEnrollmentID, NodeID: startupNodeID,
				CoordinatorID:     startupCoordinatorID,
				ProtocolVersion:   federation.ProtocolVersion,
				PreparationDigest: startupDigest, Seal: startupSeal,
			},
		))
		_, err = database.BeginNodePreparation(t.Context(), db.NodePreparationBinding{
			EnrollmentID:      startupEnrollmentID,
			CoordinatorNodeID: startupCoordinatorID,
			LocalNodeID:       startupNodeID,
			ProtocolVersion:   federation.ProtocolVersion,
		})
		require.NoError(t, err)
		require.NoError(t, database.StoreLocalNodePreparationSeal(
			t.Context(), startupDigest, startupSeal,
		))
	}
	return enrollments, credentials, &config.Config{Fleet: config.Fleet{
		Enabled: true, Role: config.FleetRoleNode,
		Coordinator: &config.FleetCoordinator{
			NodeID: startupCoordinatorID, BaseURL: coordinatorURL,
		},
	}}
}
