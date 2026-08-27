package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"time"

	"go.kenn.io/forge/internal/config"
	"go.kenn.io/forge/internal/db"
	"go.kenn.io/forge/internal/federation"
	"go.kenn.io/forge/internal/federationauth"
	"go.kenn.io/forge/internal/providerplane"
)

type federationNodeStartupState string

const (
	federationStartupCoordinator    federationNodeStartupState = "coordinator"
	federationStartupActive         federationNodeStartupState = "active"
	federationStartupActionRequired federationNodeStartupState = "action_required"
	federationStartupIncompatible   federationNodeStartupState = "incompatible"

	maxNodeActivationResponseBytes = 1 << 20
	nodeActivationAttempts         = 3
)

var errCoordinatorProtocolMismatch = errors.New("coordinator federation protocol is incompatible")

type federationNodeStartup struct {
	State  federationNodeStartupState
	Reason string
}

func (s federationNodeStartup) Active() bool {
	return s.State == federationStartupActive
}

func activateFederationNodeAtStartup(
	ctx context.Context,
	database *db.DB,
	cfg *config.Config,
	nodeID string,
	enrollments *federation.Store,
	credentials *federationauth.Store,
	httpClient *http.Client,
) federationNodeStartup {
	if cfg == nil || cfg.Fleet.RoleOrDefault() != config.FleetRoleNode {
		return federationNodeStartup{State: federationStartupCoordinator}
	}
	fail := func(state federationNodeStartupState, reason string) federationNodeStartup {
		return federationNodeStartup{State: state, Reason: reason}
	}
	if cfg.Fleet.Coordinator == nil {
		return fail(federationStartupActionRequired, "fleet node coordinator binding is missing")
	}
	local, ok := enrollments.Local()
	if !ok || local.Preparation == nil {
		return fail(federationStartupActionRequired, "fleet node preparation seal is missing")
	}
	if local.ProtocolVersion != federation.ProtocolVersion ||
		local.Preparation.ProtocolVersion != federation.ProtocolVersion {
		return fail(federationStartupIncompatible, "fleet node enrollment protocol is incompatible")
	}
	coordinator := cfg.Fleet.Coordinator
	if local.State == federation.EnrollmentRevoked || local.NodeID != nodeID ||
		local.CoordinatorID != coordinator.NodeID ||
		local.CoordinatorURL != coordinator.BaseURL ||
		local.Preparation.EnrollmentID != local.EnrollmentID ||
		local.Preparation.NodeID != local.NodeID ||
		local.Preparation.CoordinatorID != local.CoordinatorID {
		return fail(federationStartupActionRequired, "fleet node config does not match its sealed enrollment")
	}
	if database == nil {
		return fail(federationStartupActionRequired, "fleet node preparation database is unavailable")
	}
	preparation, err := database.GetNodePreparation(ctx)
	if err != nil || preparation.Phase != db.NodePreparationSealed ||
		preparation.EnrollmentID != local.EnrollmentID ||
		preparation.LocalNodeID != local.NodeID ||
		preparation.CoordinatorNodeID != local.CoordinatorID ||
		preparation.ProtocolVersion != local.ProtocolVersion ||
		preparation.PreparationDigest != local.Preparation.PreparationDigest ||
		preparation.PreparationSeal != local.Preparation.Seal {
		return fail(federationStartupActionRequired, "fleet node preparation state does not match its sealed enrollment")
	}
	credential, ok := credentials.Outbound(local.CoordinatorID)
	if !ok || !slices.Contains(credential.Scopes, federationauth.ScopeEnrollmentActivate) {
		return fail(federationStartupActionRequired, "fleet node coordinator credential is unavailable")
	}
	if !cfg.Fleet.Enabled {
		if local.State != federation.EnrollmentActive {
			return fail(
				federationStartupActionRequired,
				"fleet node activation cannot complete while federation is disabled",
			)
		}
		if err := promoteFederationNodeCredentialScopes(
			credentials, local.CoordinatorID,
		); err != nil {
			return fail(
				federationStartupActionRequired,
				"repair fleet node credential scopes: "+err.Error(),
			)
		}
		return federationNodeStartup{State: federationStartupActive}
	}

	client := nodeActivationHTTPClient(httpClient)
	for attempt := range nodeActivationAttempts {
		activationErr := validateAndActivateFederationNode(
			ctx, client, local, credential,
		)
		if activationErr == nil {
			if err := enrollments.MarkLocalActive(ctx, local.EnrollmentID); err != nil {
				return fail(federationStartupActionRequired, "persist fleet node activation: "+err.Error())
			}
			if err := promoteFederationNodeCredentialScopes(
				credentials, local.CoordinatorID,
			); err != nil {
				return fail(federationStartupActionRequired, "activate federation credentials: "+err.Error())
			}
			return federationNodeStartup{State: federationStartupActive}
		}
		if errors.Is(activationErr, errCoordinatorProtocolMismatch) {
			return fail(federationStartupIncompatible, activationErr.Error())
		}
		if !isRetryableNodeActivationError(activationErr) || attempt+1 == nodeActivationAttempts {
			return fail(
				federationStartupActionRequired,
				"fleet node coordinator activation failed: "+activationErr.Error(),
			)
		}
		delay := time.Duration(1<<attempt) * 100 * time.Millisecond
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			ctxErr := ctx.Err()
			if ctxErr == nil {
				ctxErr = context.Canceled
			}
			return fail(
				federationStartupActionRequired,
				"fleet node coordinator activation failed: "+ctxErr.Error(),
			)
		case <-timer.C:
		}
	}
	return fail(federationStartupActionRequired, "fleet node activation retry loop exhausted")
}

func promoteFederationNodeCredentialScopes(
	credentials *federationauth.Store, coordinatorID string,
) error {
	if err := credentials.UpdateInboundScopes(
		coordinatorID, federationauth.CoordinatorToNodeScopes(),
	); err != nil {
		return fmt.Errorf("promote coordinator credential: %w", err)
	}
	if err := credentials.UpdateOutboundScopes(
		coordinatorID, federationauth.NodeToCoordinatorScopes(),
	); err != nil {
		return fmt.Errorf("promote node credential: %w", err)
	}
	return nil
}

type nodeActivationHTTPError struct {
	status int
}

func (e *nodeActivationHTTPError) Error() string {
	return fmt.Sprintf("coordinator returned HTTP %d", e.status)
}

func isRetryableNodeActivationError(err error) bool {
	if responseErr, ok := errors.AsType[*nodeActivationHTTPError](err); ok {
		return responseErr.status >= http.StatusInternalServerError
	}
	return true
}

func validateAndActivateFederationNode(
	ctx context.Context,
	client *http.Client,
	local federation.LocalEnrollment,
	credential federationauth.Credential,
) error {
	var identity struct {
		NodeID          string `json:"node_id"`
		ProtocolVersion int    `json:"protocol_version"`
	}
	if err := doNodeActivationJSON(
		ctx, client, http.MethodGet,
		local.CoordinatorURL+"/api/v1/federation/identity",
		local.NodeID, credential.Token, nil, &identity,
	); err != nil {
		return err
	}
	if identity.ProtocolVersion != federation.ProtocolVersion {
		return fmt.Errorf(
			"%w: expected %d, got %d",
			errCoordinatorProtocolMismatch,
			federation.ProtocolVersion, identity.ProtocolVersion,
		)
	}
	if identity.NodeID != local.CoordinatorID {
		return errors.New("coordinator identity does not match the sealed enrollment")
	}
	var active federation.Enrollment
	if err := doNodeActivationJSON(
		ctx, client, http.MethodPost,
		local.CoordinatorURL+"/api/v1/federation/enrollments/"+
			local.EnrollmentID+"/activate",
		local.NodeID, credential.Token,
		map[string]any{
			"protocol_version": federation.ProtocolVersion,
			"preparation_seal": local.Preparation.Seal,
		},
		&active,
	); err != nil {
		return err
	}
	if active.ID != local.EnrollmentID || active.NodeID != local.NodeID ||
		active.CoordinatorID != local.CoordinatorID ||
		active.ProtocolVersion != federation.ProtocolVersion ||
		active.State != federation.EnrollmentActive {
		return errors.New("coordinator activation response does not match the sealed enrollment")
	}
	return nil
}

func doNodeActivationJSON(
	ctx context.Context,
	client *http.Client,
	method, endpoint, nodeID, token string,
	body any,
	target any,
) error {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode federation activation request: %w", err)
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, endpoint, reader)
	if err != nil {
		return fmt.Errorf("build federation activation request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set(federationauth.NodeIDHeader, nodeID)
	request.Header.Set(providerplane.ProtocolVersionHeader, providerplane.ProtocolVersionHeaderValue())
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	contents, err := io.ReadAll(io.LimitReader(
		response.Body, maxNodeActivationResponseBytes+1,
	))
	if err != nil {
		return fmt.Errorf("read federation activation response: %w", err)
	}
	if len(contents) > maxNodeActivationResponseBytes {
		return errors.New("federation activation response is too large")
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		if response.StatusCode == http.StatusConflict &&
			strings.Contains(string(contents), "protocolMismatch") {
			return errCoordinatorProtocolMismatch
		}
		return &nodeActivationHTTPError{status: response.StatusCode}
	}
	if err := json.Unmarshal(contents, target); err != nil {
		return fmt.Errorf("decode federation activation response: %w", err)
	}
	return nil
}

func nodeActivationHTTPClient(base *http.Client) *http.Client {
	client := &http.Client{Timeout: 10 * time.Second}
	if base != nil {
		*client = *base
		if client.Timeout <= 0 || client.Timeout > 10*time.Second {
			client.Timeout = 10 * time.Second
		}
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return client
}
