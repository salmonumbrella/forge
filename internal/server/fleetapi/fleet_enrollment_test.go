package fleetapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.kenn.io/forge/internal/config"
	"go.kenn.io/forge/internal/db"
	"go.kenn.io/forge/internal/federation"
	"go.kenn.io/forge/internal/federationauth"
	"go.kenn.io/forge/internal/server/httpapi"
	"go.kenn.io/forge/internal/testutil/dbtest"
)

const (
	enrollmentCoordinatorID = "0123456789abcdef0123456789abcdef"
	enrollmentNodeID        = "fedcba9876543210fedcba9876543210"
	enrollmentRequestID     = "11111111111111111111111111111111"
)

type enrollmentHandlerFixture struct {
	handler     *Handler
	credentials *federationauth.Store
	enrollments *federation.Store
	database    *db.DB
	mux         http.Handler
}

func TestEnrollmentHTTPExchangeConsumesTokenAndIsDirectional(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	fixture := newEnrollmentHandlerFixture(t, enrollmentCoordinatorID, nil)
	server := httptest.NewTLSServer(fixture.mux)
	t.Cleanup(server.Close)
	now := time.Now().UTC()
	oneTime, err := fixture.enrollments.CreateOneTimeToken(federation.Identity{
		NodeID: enrollmentCoordinatorID, Name: "Studio", BaseURL: server.URL,
	}, now.Add(time.Minute))
	require.NoError(err)

	request := federation.JoinRequest{
		EnrollmentID: enrollmentRequestID,
		NodeID:       enrollmentNodeID, Name: "Build Box", Platform: "linux",
		BaseURL: "https://node.example", ProtocolVersion: federation.ProtocolVersion,
		CoordinatorCredential: "coordinator-calls-node-token",
	}
	first := postEnrollmentRequest(t, server.Client(), server.URL, oneTime.Token, request)
	require.Equal(http.StatusCreated, first.StatusCode)
	var firstResponse federation.JoinResponse
	require.NoError(json.NewDecoder(first.Body).Decode(&firstResponse))
	first.Body.Close()
	assert.Equal(enrollmentRequestID, firstResponse.EnrollmentID)
	assert.Equal(enrollmentCoordinatorID, firstResponse.CoordinatorID)
	assert.True(firstResponse.PreparationRequired)

	outbound, ok := fixture.credentials.Outbound(enrollmentNodeID)
	require.True(ok)
	assert.Equal(request.CoordinatorCredential, outbound.Token)
	assert.Equal(federationauth.PendingCoordinatorToNodeScopes(), outbound.Scopes)
	principal, ok := fixture.credentials.Authenticate(firstResponse.NodeCredential)
	require.True(ok)
	assert.Equal(enrollmentNodeID, principal.NodeID)
	assert.Equal(scopeSetForEnrollmentTest(federationauth.PendingNodeToCoordinatorScopes()), principal.Scopes)

	retry := postEnrollmentRequest(t, server.Client(), server.URL, oneTime.Token, request)
	require.Equal(http.StatusConflict, retry.StatusCode)
	retry.Body.Close()
	_, ok = fixture.credentials.Authenticate(firstResponse.NodeCredential)
	assert.True(ok, "replaying a consumed token must not rotate the credential")

	different := request
	different.EnrollmentID = "22222222222222222222222222222222"
	different.NodeID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	different.BaseURL = "https://other.example"
	rejected := postEnrollmentRequest(t, server.Client(), server.URL, oneTime.Token, different)
	assert.Equal(http.StatusConflict, rejected.StatusCode)
	rejected.Body.Close()
}

func TestFleetJoinPersistsBothCredentialDirectionsAndCoordinatorBinding(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	coordinator := newEnrollmentHandlerFixture(t, enrollmentCoordinatorID, nil)
	coordinatorServer := httptest.NewTLSServer(coordinator.mux)
	t.Cleanup(coordinatorServer.Close)
	oneTime, err := coordinator.enrollments.CreateOneTimeToken(federation.Identity{
		NodeID: enrollmentCoordinatorID, Name: "Studio",
		BaseURL: coordinatorServer.URL,
	}, time.Now().Add(time.Minute))
	require.NoError(err)

	var binding config.FleetCoordinator
	node := newEnrollmentHandlerFixture(t, enrollmentNodeID, func(deps *Deps) {
		deps.FederationHTTPClient = coordinatorServer.Client()
		require.NoError(deps.Enrollments.SaveLocal(t.Context(), federation.LocalEnrollment{
			EnrollmentID: enrollmentRequestID, NodeID: enrollmentNodeID,
			NodePlatform: "linux", NodeBaseURL: "https://node.example",
			CoordinatorURL:  coordinatorServer.URL,
			ProtocolVersion: federation.ProtocolVersion,
			State:           federation.EnrollmentPending, PreparationRequired: true,
		}))
		deps.PersistCoordinatorBinding = func(
			_ context.Context, got config.FleetCoordinator,
		) error {
			binding = got
			return nil
		}
	})
	nodeServer := httptest.NewServer(node.mux)
	t.Cleanup(nodeServer.Close)
	body := map[string]any{
		"coordinator_base_url": coordinatorServer.URL,
		"node_base_url":        "https://node.example",
		"name":                 "Build Box",
		"enrollment_token":     oneTime.Token,
	}
	response := doEnrollmentJSON(
		t, nodeServer.Client(), http.MethodPost,
		nodeServer.URL+"/api/v1/fleet/join", body, "",
	)
	require.Equal(http.StatusOK, response.StatusCode)
	var local federation.LocalEnrollment
	require.NoError(json.NewDecoder(response.Body).Decode(&local))
	response.Body.Close()
	assert.Equal(enrollmentCoordinatorID, local.CoordinatorID)
	assert.NotEqual(enrollmentRequestID, local.EnrollmentID)
	assert.Equal(federation.EnrollmentPending, local.State)
	assert.Equal(oneTime.ExpiresAt, local.ExpiresAt)
	assert.True(local.PreparationRequired)
	assert.Equal(enrollmentCoordinatorID, binding.NodeID)
	assert.Equal(coordinatorServer.URL, binding.BaseURL)

	coordinatorOutbound, ok := coordinator.credentials.Outbound(enrollmentNodeID)
	require.True(ok)
	principal, ok := node.credentials.Authenticate(coordinatorOutbound.Token)
	require.True(ok)
	assert.Equal(enrollmentCoordinatorID, principal.NodeID)
	assert.Equal(
		scopeSetForEnrollmentTest(federationauth.PendingCoordinatorToNodeScopes()),
		principal.Scopes,
	)
	nodeOutbound, ok := node.credentials.Outbound(enrollmentCoordinatorID)
	require.True(ok)
	principal, ok = coordinator.credentials.Authenticate(nodeOutbound.Token)
	require.True(ok)
	assert.Equal(enrollmentNodeID, principal.NodeID)

	persisted, ok := node.enrollments.Local()
	require.True(ok)
	assert.Equal(local, persisted)
}

func TestSendJoinRequestDoesNotRetryAmbiguousTransportFailure(t *testing.T) {
	var attempts int
	handler := &Handler{
		federationHTTPClient: &http.Client{Transport: roundTripFunc(func(
			*http.Request,
		) (*http.Response, error) {
			attempts++
			return nil, errors.New("response unavailable")
		})},
	}

	_, err := handler.sendJoinRequest(
		t.Context(), "https://coordinator.example", "one-time-token",
		federation.JoinRequest{},
	)

	var problem *httpapi.ProblemError
	require.ErrorAs(t, err, &problem)
	assert.Equal(t, http.StatusServiceUnavailable, problem.Status)
	assert.Equal(t, 1, attempts)
}

func TestFleetJoinReplacesUnboundProvisionalAfterLostAcceptedResponse(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	coordinator := newEnrollmentHandlerFixture(t, enrollmentCoordinatorID, nil)
	coordinatorServer := httptest.NewTLSServer(coordinator.mux)
	t.Cleanup(coordinatorServer.Close)
	identity := federation.Identity{
		NodeID: enrollmentCoordinatorID, Name: "Studio",
		BaseURL: coordinatorServer.URL,
	}
	oneTime, err := coordinator.enrollments.CreateOneTimeToken(
		identity, time.Now().Add(time.Minute),
	)
	require.NoError(err)

	coordinatorTransport := coordinatorServer.Client().Transport
	dropResponse := true
	node := newEnrollmentHandlerFixture(t, enrollmentNodeID, func(deps *Deps) {
		deps.PersistCoordinatorBinding = func(context.Context, config.FleetCoordinator) error {
			return nil
		}
		deps.FederationHTTPClient = &http.Client{Transport: roundTripFunc(func(
			request *http.Request,
		) (*http.Response, error) {
			response, err := coordinatorTransport.RoundTrip(request)
			if err != nil || !dropResponse {
				return response, err
			}
			dropResponse = false
			response.Body.Close()
			return nil, errors.New("coordinator response lost")
		})}
	})
	nodeServer := httptest.NewServer(node.mux)
	t.Cleanup(nodeServer.Close)

	failed := doEnrollmentJSON(
		t, nodeServer.Client(), http.MethodPost,
		nodeServer.URL+"/api/v1/fleet/join", map[string]any{
			"coordinator_base_url": coordinatorServer.URL,
			"node_base_url":        "https://node.example",
			"enrollment_token":     oneTime.Token,
		}, "",
	)
	require.Equal(http.StatusServiceUnavailable, failed.StatusCode)
	failed.Body.Close()
	provisional, ok := node.enrollments.Local()
	require.True(ok)
	assert.Empty(provisional.CoordinatorID)
	accepted, err := coordinator.enrollments.Get(t.Context(), provisional.EnrollmentID)
	require.NoError(err)
	assert.Equal(federation.EnrollmentPending, accepted.State)
	require.NoError(coordinator.enrollments.Revoke(t.Context(), provisional.EnrollmentID))
	replacementToken, err := coordinator.enrollments.CreateOneTimeToken(
		identity, time.Now().Add(time.Minute),
	)
	require.NoError(err)

	joined := doEnrollmentJSON(
		t, nodeServer.Client(), http.MethodPost,
		nodeServer.URL+"/api/v1/fleet/join", map[string]any{
			"coordinator_base_url": coordinatorServer.URL,
			"node_base_url":        "https://node.example",
			"enrollment_token":     replacementToken.Token,
		}, "",
	)
	require.Equal(http.StatusOK, joined.StatusCode)
	var local federation.LocalEnrollment
	require.NoError(json.NewDecoder(joined.Body).Decode(&local))
	joined.Body.Close()
	assert.NotEqual(provisional.EnrollmentID, local.EnrollmentID)
	assert.Equal(enrollmentCoordinatorID, local.CoordinatorID)
	assert.Error(node.credentials.BindInbound(
		provisional.EnrollmentID, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	))
}

func TestFleetJoinPreservesCompletedOrPreparedLocalEnrollment(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	tests := []struct {
		name  string
		local federation.LocalEnrollment
	}{
		{
			name: "active",
			local: federation.LocalEnrollment{
				EnrollmentID: enrollmentRequestID, NodeID: enrollmentNodeID,
				NodePlatform: "linux", NodeBaseURL: "https://node.example",
				CoordinatorID:   enrollmentCoordinatorID,
				CoordinatorURL:  "https://coordinator.example",
				ProtocolVersion: federation.ProtocolVersion,
				State:           federation.EnrollmentActive, ExpiresAt: now.Add(time.Hour),
			},
		},
		{
			name: "prepared",
			local: federation.LocalEnrollment{
				EnrollmentID: enrollmentRequestID, NodeID: enrollmentNodeID,
				NodePlatform: "linux", NodeBaseURL: "https://node.example",
				CoordinatorID:   enrollmentCoordinatorID,
				CoordinatorURL:  "https://coordinator.example",
				ProtocolVersion: federation.ProtocolVersion,
				State:           federation.EnrollmentPending, ExpiresAt: now.Add(time.Hour),
				PreparationStarted: true, PreparationRequired: true,
				Preparation: &federation.LocalPreparationSeal{
					EnrollmentID: enrollmentRequestID, NodeID: enrollmentNodeID,
					CoordinatorID:     enrollmentCoordinatorID,
					ProtocolVersion:   federation.ProtocolVersion,
					PreparationDigest: "digest", Seal: "sealed-proof",
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert := assert.New(t)
			require := require.New(t)
			requests := 0
			node := newEnrollmentHandlerFixture(t, enrollmentNodeID, func(deps *Deps) {
				require.NoError(deps.Enrollments.SaveLocal(t.Context(), test.local))
				deps.FederationHTTPClient = &http.Client{Transport: roundTripFunc(func(
					*http.Request,
				) (*http.Response, error) {
					requests++
					return &http.Response{StatusCode: http.StatusInternalServerError, Body: http.NoBody}, nil
				})}
			})
			nodeServer := httptest.NewServer(node.mux)
			t.Cleanup(nodeServer.Close)

			response := doEnrollmentJSON(
				t, nodeServer.Client(), http.MethodPost,
				nodeServer.URL+"/api/v1/fleet/join", map[string]any{
					"coordinator_base_url": "https://coordinator.example",
					"node_base_url":        "https://node.example",
					"enrollment_token":     "replacement-token",
				}, "",
			)
			assert.Equal(http.StatusConflict, response.StatusCode)
			response.Body.Close()
			assert.Zero(requests)
			persisted, ok := node.enrollments.Local()
			require.True(ok)
			assert.Equal(test.local, persisted)
		})
	}
}

func TestFleetJoinRejectsCoordinatorWithMembers(t *testing.T) {
	requests := 0
	node := newEnrollmentHandlerFixture(t, enrollmentNodeID, func(deps *Deps) {
		deps.Config.Fleet.Members = []config.FleetMember{{
			NodeID:  "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			BaseURL: "https://member.example", State: federation.EnrollmentActive,
		}}
		deps.FederationHTTPClient = &http.Client{Transport: roundTripFunc(func(
			*http.Request,
		) (*http.Response, error) {
			requests++
			return &http.Response{StatusCode: http.StatusInternalServerError, Body: http.NoBody}, nil
		})}
	})
	nodeServer := httptest.NewServer(node.mux)
	t.Cleanup(nodeServer.Close)

	response := doEnrollmentJSON(
		t, nodeServer.Client(), http.MethodPost,
		nodeServer.URL+"/api/v1/fleet/join", map[string]any{
			"coordinator_base_url": "https://coordinator.example",
			"node_base_url":        "https://node.example",
			"enrollment_token":     "replacement-token",
		}, "",
	)
	assert.Equal(t, http.StatusConflict, response.StatusCode)
	response.Body.Close()
	assert.Zero(t, requests)
}

func TestEnrollmentActivationRequiresPreparationAndIsIdempotent(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	var persisted []config.FleetMember
	fixture := newEnrollmentHandlerFixture(t, enrollmentCoordinatorID, func(deps *Deps) {
		deps.PersistMember = func(_ context.Context, member config.FleetMember) error {
			persisted = append(persisted, member)
			return nil
		}
	})
	server := httptest.NewTLSServer(authenticatedEnrollmentHandler(fixture))
	t.Cleanup(server.Close)
	oneTime, err := fixture.enrollments.CreateOneTimeToken(federation.Identity{
		NodeID: enrollmentCoordinatorID, BaseURL: server.URL,
	}, time.Now().Add(time.Minute))
	require.NoError(err)
	joinRequest := federation.JoinRequest{
		EnrollmentID: enrollmentRequestID, NodeID: enrollmentNodeID,
		Platform: "linux", BaseURL: "https://node.example",
		ProtocolVersion:       federation.ProtocolVersion,
		CoordinatorCredential: "coordinator-calls-node-token",
	}
	joined := postEnrollmentRequest(t, server.Client(), server.URL, oneTime.Token, joinRequest)
	require.Equal(http.StatusCreated, joined.StatusCode)
	var joinResponse federation.JoinResponse
	require.NoError(json.NewDecoder(joined.Body).Decode(&joinResponse))
	joined.Body.Close()

	preparationSeal := ""
	activate := func() *http.Response {
		return doEnrollmentJSON(
			t, server.Client(), http.MethodPost,
			server.URL+"/api/v1/federation/enrollments/"+enrollmentRequestID+"/activate",
			map[string]any{
				"protocol_version": federation.ProtocolVersion,
				"preparation_seal": preparationSeal,
			},
			joinResponse.NodeCredential,
		)
	}
	blocked := activate()
	assert.Equal(http.StatusConflict, blocked.StatusCode)
	blocked.Body.Close()
	sealRequest := db.NodePreparationSealRequest{
		EnrollmentID: enrollmentRequestID, NodeID: enrollmentNodeID,
		CoordinatorNodeID: enrollmentCoordinatorID,
		ProtocolVersion:   federation.ProtocolVersion,
		MigrationVersion:  db.WorkspaceLaunchSpecMigrationVersion,
		ReceiptsDigest:    "receipts", DrainedAckGeneration: 1,
	}
	sealRequest.PreparationDigest, err = db.NodePreparationSealDigest(sealRequest)
	require.NoError(err)
	seal, err := fixture.database.IssueNodePreparationSeal(t.Context(), sealRequest)
	require.NoError(err)
	preparationSeal = seal.Seal

	active := activate()
	require.Equal(http.StatusOK, active.StatusCode)
	var enrollment federation.Enrollment
	require.NoError(json.NewDecoder(active.Body).Decode(&enrollment))
	active.Body.Close()
	assert.Equal(federation.EnrollmentActive, enrollment.State)
	require.Len(persisted, 1)
	assert.Equal(enrollmentNodeID, persisted[0].NodeID)

	retried := activate()
	assert.Equal(http.StatusOK, retried.StatusCode)
	retried.Body.Close()
	assert.Len(persisted, 1, "an already-active retry does not persist membership twice")

	replayedJoin := postEnrollmentRequest(
		t, server.Client(), server.URL, oneTime.Token, joinRequest,
	)
	assert.Equal(http.StatusConflict, replayedJoin.StatusCode)
	replayedJoin.Body.Close()
	principal, ok := fixture.credentials.Authenticate(joinResponse.NodeCredential)
	require.True(ok, "replaying a consumed token must not rotate the active credential")
	assert.Equal(
		scopeSetForEnrollmentTest(federationauth.NodeToCoordinatorScopes()),
		principal.Scopes,
	)
	outbound, ok := fixture.credentials.Outbound(enrollmentNodeID)
	require.True(ok)
	assert.Equal(federationauth.CoordinatorToNodeScopes(), outbound.Scopes)
}

func TestNodePreparationSealIsEnrollmentBoundAndRetrySafe(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	fixture := newEnrollmentHandlerFixture(t, enrollmentCoordinatorID, nil)
	server := httptest.NewTLSServer(authenticatedEnrollmentHandler(fixture))
	t.Cleanup(server.Close)
	oneTime, err := fixture.enrollments.CreateOneTimeToken(federation.Identity{
		NodeID: enrollmentCoordinatorID, BaseURL: server.URL,
	}, time.Now().Add(time.Minute))
	require.NoError(err)
	joinRequest := federation.JoinRequest{
		EnrollmentID: enrollmentRequestID, NodeID: enrollmentNodeID,
		Platform: "linux", BaseURL: "https://node.example",
		ProtocolVersion:       federation.ProtocolVersion,
		CoordinatorCredential: "coordinator-calls-node-token",
	}
	joined := postEnrollmentRequest(t, server.Client(), server.URL, oneTime.Token, joinRequest)
	require.Equal(http.StatusCreated, joined.StatusCode)
	var joinResponse federation.JoinResponse
	require.NoError(json.NewDecoder(joined.Body).Decode(&joinResponse))
	joined.Body.Close()

	begin := doEnrollmentJSON(
		t, server.Client(), http.MethodPost,
		server.URL+"/api/v1/federation/enrollments/"+enrollmentRequestID+"/preparation/begin",
		map[string]any{}, joinResponse.NodeCredential,
	)
	require.Equal(http.StatusOK, begin.StatusCode)
	begin.Body.Close()
	sealRequest := db.NodePreparationSealRequest{
		EnrollmentID: enrollmentRequestID, NodeID: enrollmentNodeID,
		CoordinatorNodeID: enrollmentCoordinatorID,
		ProtocolVersion:   federation.ProtocolVersion,
		MigrationVersion:  db.WorkspaceLaunchSpecMigrationVersion,
		ReceiptsDigest:    "receipts-digest", DrainedAckGeneration: 4,
	}
	sealRequest.PreparationDigest, err = db.NodePreparationSealDigest(sealRequest)
	require.NoError(err)
	sealBody := func(request db.NodePreparationSealRequest) map[string]any {
		return map[string]any{
			"node_id": request.NodeID, "coordinator_node_id": request.CoordinatorNodeID,
			"protocol_version":       request.ProtocolVersion,
			"migration_version":      request.MigrationVersion,
			"receipts_digest":        request.ReceiptsDigest,
			"drained_ack_generation": request.DrainedAckGeneration,
			"preparation_digest":     request.PreparationDigest,
		}
	}
	seal := func(payload map[string]any) *http.Response {
		return doEnrollmentJSON(
			t, server.Client(), http.MethodPost,
			server.URL+"/api/v1/federation/enrollments/"+enrollmentRequestID+"/preparation/seal",
			payload, joinResponse.NodeCredential,
		)
	}
	first := seal(sealBody(sealRequest))
	require.Equal(http.StatusOK, first.StatusCode)
	var firstSeal db.NodePreparationSeal
	require.NoError(json.NewDecoder(first.Body).Decode(&firstSeal))
	first.Body.Close()
	assert.NotEmpty(firstSeal.Seal)
	assert.Equal(sealRequest.PreparationDigest, firstSeal.PreparationDigest)

	retry := seal(sealBody(sealRequest))
	require.Equal(http.StatusOK, retry.StatusCode)
	var retrySeal db.NodePreparationSeal
	require.NoError(json.NewDecoder(retry.Body).Decode(&retrySeal))
	retry.Body.Close()
	assert.Equal(firstSeal, retrySeal)

	different := sealRequest
	different.ReceiptsDigest = "changed-receipts"
	different.PreparationDigest, err = db.NodePreparationSealDigest(different)
	require.NoError(err)
	conflict := seal(sealBody(different))
	assert.Equal(http.StatusConflict, conflict.StatusCode)
	conflict.Body.Close()

	stored, err := fixture.database.GetNodePreparationSeal(t.Context(), enrollmentRequestID)
	require.NoError(err)
	require.NotNil(stored)
	assert.Equal(firstSeal.Seal, stored.Seal)
}

func TestEnrollmentRevocationRemovesCredentialsBeforeReturning(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	var removedNodeID string
	fixture := newEnrollmentHandlerFixture(t, enrollmentCoordinatorID, func(deps *Deps) {
		deps.RemoveMember = func(_ context.Context, nodeID string) error {
			removedNodeID = nodeID
			return nil
		}
	})
	server := httptest.NewTLSServer(fixture.mux)
	t.Cleanup(server.Close)
	oneTime, err := fixture.enrollments.CreateOneTimeToken(federation.Identity{
		NodeID: enrollmentCoordinatorID, BaseURL: server.URL,
	}, time.Now().Add(time.Minute))
	require.NoError(err)
	joinRequest := federation.JoinRequest{
		EnrollmentID: enrollmentRequestID, NodeID: enrollmentNodeID,
		Platform: "linux", BaseURL: "https://node.example",
		ProtocolVersion:       federation.ProtocolVersion,
		CoordinatorCredential: "coordinator-calls-node-token",
	}
	joined := postEnrollmentRequest(t, server.Client(), server.URL, oneTime.Token, joinRequest)
	require.Equal(http.StatusCreated, joined.StatusCode)
	var joinResponse federation.JoinResponse
	require.NoError(json.NewDecoder(joined.Body).Decode(&joinResponse))
	joined.Body.Close()

	revoked := doEnrollmentJSON(
		t, server.Client(), http.MethodDelete,
		server.URL+"/api/v1/fleet/enrollments/"+enrollmentRequestID,
		nil, "",
	)
	require.Equal(http.StatusNoContent, revoked.StatusCode)
	revoked.Body.Close()

	_, ok := fixture.credentials.Authenticate(joinResponse.NodeCredential)
	assert.False(ok, "the next inbound request must fail authentication")
	_, ok = fixture.credentials.Outbound(enrollmentNodeID)
	assert.False(ok, "the coordinator must stop calling the revoked node")
	assert.Equal(enrollmentNodeID, removedNodeID)
	persisted, err := fixture.enrollments.Get(t.Context(), enrollmentRequestID)
	require.NoError(err)
	assert.Equal(federation.EnrollmentRevoked, persisted.State)
}

func newEnrollmentHandlerFixture(
	t *testing.T, nodeID string, configure func(*Deps),
) enrollmentHandlerFixture {
	t.Helper()
	credentials, err := federationauth.Open(
		filepath.Join(t.TempDir(), "credentials.json"),
	)
	require.NoError(t, err)
	enrollments, err := federation.Open(
		filepath.Join(t.TempDir(), "enrollments.json"), federation.StoreOptions{},
	)
	require.NoError(t, err)
	deps := Deps{
		DB: dbtest.Open(t), NodeID: nodeID,
		Credentials: credentials, Enrollments: enrollments,
		Config: ConfigSnapshot{Fleet: config.Fleet{
			Enabled: true, Role: config.FleetRoleCoordinator,
		}},
	}
	if configure != nil {
		configure(&deps)
	}
	handler := New(deps)
	mux := http.NewServeMux()
	apiConfig := huma.DefaultConfig("fleet enrollment test", "0.0.0")
	apiConfig.OpenAPIPath = ""
	apiConfig.DocsPath = ""
	apiConfig.SchemasPath = ""
	api := humago.NewWithPrefix(mux, "/api/v1", apiConfig)
	handler.Register(api)
	return enrollmentHandlerFixture{
		handler: handler, credentials: credentials, enrollments: enrollments,
		database: deps.DB, mux: mux,
	}
}

func authenticatedEnrollmentHandler(fixture enrollmentHandlerFixture) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/federation/enrollments/") ||
			r.URL.Path == "/api/v1/federation/identity" {
			token, _ := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
			principal, ok := fixture.credentials.Authenticate(token)
			if !ok {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			r = r.WithContext(federationauth.WithPrincipal(r.Context(), principal))
		}
		fixture.mux.ServeHTTP(w, r)
	})
}

func postEnrollmentRequest(
	t *testing.T, client *http.Client, baseURL, token string, request federation.JoinRequest,
) *http.Response {
	t.Helper()
	return doEnrollmentJSON(
		t, client, http.MethodPost, baseURL+"/api/v1/federation/enrollments",
		request, token,
	)
}

func doEnrollmentJSON(
	t *testing.T, client *http.Client, method, target string, body any, token string,
) *http.Response {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	request, err := http.NewRequestWithContext(
		t.Context(), method, target, bytes.NewReader(raw),
	)
	require.NoError(t, err)
	request.Header.Set("Content-Type", "application/json")
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := client.Do(request)
	require.NoError(t, err)
	return response
}

func scopeSetForEnrollmentTest(scopes []federationauth.Scope) map[federationauth.Scope]struct{} {
	result := make(map[federationauth.Scope]struct{}, len(scopes))
	for _, scope := range scopes {
		result[scope] = struct{}{}
	}
	return result
}
