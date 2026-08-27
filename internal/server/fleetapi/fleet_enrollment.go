package fleetapi

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"runtime"
	"strings"
	"time"

	"github.com/danielgtaylor/huma/v2"

	"go.kenn.io/forge/internal/config"
	"go.kenn.io/forge/internal/db"
	"go.kenn.io/forge/internal/federation"
	"go.kenn.io/forge/internal/federationauth"
	"go.kenn.io/forge/internal/server/httpapi"
)

const maxFederationEnrollmentResponseBytes = 1 << 20

type createEnrollmentTokenInput struct {
	Body struct {
		BaseURL          string `json:"base_url"`
		Name             string `json:"name,omitempty"`
		ExpiresInSeconds int    `json:"expires_in_seconds" minimum:"1" maximum:"86400"`
	}
}

type enrollmentTokenOutput = httpapi.BodyOutput[federation.EnrollmentToken]

type beginEnrollmentInput struct {
	Authorization string `header:"Authorization"`
	Body          federation.JoinRequest
}

type beginEnrollmentOutput = httpapi.BodyOutput[federation.JoinResponse]

type localJoinInput struct {
	Body struct {
		CoordinatorURL  string `json:"coordinator_base_url"`
		NodeBaseURL     string `json:"node_base_url"`
		Name            string `json:"name,omitempty"`
		EnrollmentToken string `json:"enrollment_token"`
	}
}

type localJoinOutput = httpapi.BodyOutput[federation.LocalEnrollment]

type activateEnrollmentInput struct {
	EnrollmentID string `path:"enrollment_id"`
	Body         struct {
		ProtocolVersion int    `json:"protocol_version"`
		PreparationSeal string `json:"preparation_seal"`
	}
}

type activateEnrollmentOutput = httpapi.BodyOutput[federation.Enrollment]

type beginNodePreparationInput struct {
	EnrollmentID string `path:"enrollment_id"`
}

type beginNodePreparationOutput = httpapi.BodyOutput[federation.Enrollment]

type sealNodePreparationInput struct {
	EnrollmentID string `path:"enrollment_id"`
	Body         struct {
		NodeID               string `json:"node_id"`
		CoordinatorNodeID    string `json:"coordinator_node_id"`
		ProtocolVersion      int    `json:"protocol_version"`
		MigrationVersion     int    `json:"migration_version"`
		ReceiptsDigest       string `json:"receipts_digest"`
		DrainedAckGeneration int64  `json:"drained_ack_generation"`
		PreparationDigest    string `json:"preparation_digest"`
	}
}

type sealNodePreparationOutput = httpapi.BodyOutput[db.NodePreparationSeal]

type revokeEnrollmentInput struct {
	EnrollmentID string `path:"enrollment_id"`
}

type abortEnrollmentInput struct {
	EnrollmentID string `path:"enrollment_id"`
}

type federationIdentityOutputBody struct {
	NodeID          string `json:"node_id"`
	ProtocolVersion int    `json:"protocol_version"`
}

type federationIdentityOutput = httpapi.BodyOutput[federationIdentityOutputBody]

func (h *Handler) registerEnrollmentRoutes(api huma.API) {
	huma.Register(api, huma.Operation{
		OperationID: "create-fleet-enrollment-token",
		Method:      http.MethodPost, Path: "/fleet/enrollment-tokens",
		DefaultStatus: http.StatusCreated,
		Summary:       "Create a one-time fleet enrollment token", Tags: []string{"Fleet"},
	}, h.createEnrollmentToken)
	huma.Register(api, huma.Operation{
		OperationID: "begin-federation-enrollment",
		Method:      http.MethodPost, Path: "/federation/enrollments",
		DefaultStatus: http.StatusCreated,
		Summary:       "Begin or resume a federation enrollment", Tags: []string{"Fleet"},
	}, h.beginEnrollment)
	huma.Register(api, huma.Operation{
		OperationID: "join-federation",
		Method:      http.MethodPost, Path: "/fleet/join",
		Summary: "Join this daemon to a federation coordinator", Tags: []string{"Fleet"},
	}, h.joinFederation)
	huma.Register(api, huma.Operation{
		OperationID: "activate-federation-enrollment",
		Method:      http.MethodPost, Path: "/federation/enrollments/{enrollment_id}/activate",
		Summary: "Activate a prepared federation member", Tags: []string{"Fleet"},
	}, h.activateEnrollment)
	huma.Register(api, huma.Operation{
		OperationID: "begin-federation-node-preparation",
		Method:      http.MethodPost,
		Path:        "/federation/enrollments/{enrollment_id}/preparation/begin",
		Summary:     "Pin a pending enrollment before node preparation",
		Tags:        []string{"Fleet"},
	}, h.beginNodePreparation)
	huma.Register(api, huma.Operation{
		OperationID: "seal-federation-node-preparation",
		Method:      http.MethodPost,
		Path:        "/federation/enrollments/{enrollment_id}/preparation/seal",
		Summary:     "Seal completed provider-state handoff for a node",
		Tags:        []string{"Fleet"},
	}, h.sealNodePreparation)
	huma.Register(api, huma.Operation{
		OperationID: "get-federation-identity",
		Method:      http.MethodGet, Path: "/federation/identity",
		Summary: "Read the authenticated federation identity", Tags: []string{"Fleet"},
	}, h.getFederationIdentity)
	huma.Register(api, huma.Operation{
		OperationID: "revoke-federation-enrollment",
		Method:      http.MethodDelete, Path: "/fleet/enrollments/{enrollment_id}",
		Summary: "Revoke a federation member", Tags: []string{"Fleet"},
	}, h.revokeEnrollment)
	huma.Register(api, huma.Operation{
		OperationID: "abort-federation-enrollment",
		Method:      http.MethodPost, Path: "/federation/enrollments/{enrollment_id}/abort",
		Summary: "Abandon this node's pending federation enrollment", Tags: []string{"Fleet"},
	}, h.abortEnrollment)
}

func (h *Handler) beginNodePreparation(
	ctx context.Context,
	input *beginNodePreparationInput,
) (*beginNodePreparationOutput, error) {
	if problem := h.requireCoordinatorEnrollmentStore(); problem != nil {
		return nil, problem
	}
	principal, ok := federationauth.PrincipalFromContext(ctx)
	if !ok {
		return nil, httpapi.NewProblem(
			http.StatusUnauthorized, httpapi.CodeUnauthorized,
			"federation principal is required", nil,
		)
	}
	enrollment, err := h.enrollments.Resume(ctx, input.EnrollmentID, principal.NodeID)
	if err != nil {
		return nil, enrollmentProblem(err)
	}
	if err := h.enrollments.MarkPreparationStarted(ctx, enrollment.ID); err != nil {
		return nil, enrollmentProblem(err)
	}
	enrollment, err = h.enrollments.Resume(ctx, enrollment.ID, enrollment.NodeID)
	if err != nil {
		return nil, enrollmentProblem(err)
	}
	return &beginNodePreparationOutput{Body: enrollment}, nil
}

func (h *Handler) sealNodePreparation(
	ctx context.Context,
	input *sealNodePreparationInput,
) (*sealNodePreparationOutput, error) {
	if problem := h.requireCoordinatorEnrollmentStore(); problem != nil {
		return nil, problem
	}
	if h.db == nil {
		return nil, httpapi.ServiceUnavailable("node preparation seal store unavailable")
	}
	principal, ok := federationauth.PrincipalFromContext(ctx)
	if !ok {
		return nil, httpapi.NewProblem(
			http.StatusUnauthorized, httpapi.CodeUnauthorized,
			"federation principal is required", nil,
		)
	}
	enrollment, err := h.enrollments.Resume(ctx, input.EnrollmentID, principal.NodeID)
	if err != nil {
		return nil, enrollmentProblem(err)
	}
	if input.Body.NodeID != principal.NodeID ||
		input.Body.NodeID != enrollment.NodeID ||
		input.Body.CoordinatorNodeID != h.nodeID ||
		input.Body.ProtocolVersion != federation.ProtocolVersion ||
		input.Body.MigrationVersion != db.WorkspaceLaunchSpecMigrationVersion {
		return nil, httpapi.Conflict(
			httpapi.CodeConflict,
			"node preparation seal does not match the pending enrollment",
			map[string]any{"reason": "preparationSealBindingMismatch"},
		)
	}
	if err := h.enrollments.MarkPreparationStarted(ctx, enrollment.ID); err != nil {
		return nil, enrollmentProblem(err)
	}
	seal, err := h.db.IssueNodePreparationSeal(ctx, db.NodePreparationSealRequest{
		EnrollmentID: input.EnrollmentID, NodeID: input.Body.NodeID,
		CoordinatorNodeID:    input.Body.CoordinatorNodeID,
		ProtocolVersion:      input.Body.ProtocolVersion,
		MigrationVersion:     input.Body.MigrationVersion,
		ReceiptsDigest:       input.Body.ReceiptsDigest,
		DrainedAckGeneration: input.Body.DrainedAckGeneration,
		PreparationDigest:    input.Body.PreparationDigest,
	})
	if errors.Is(err, db.ErrNodePreparationConflict) {
		return nil, httpapi.Conflict(httpapi.CodeConflict, err.Error(), map[string]any{
			"reason": "preparationSealConflict",
		})
	}
	if err != nil {
		return nil, httpapi.Internal("issue node preparation seal: " + err.Error())
	}
	return &sealNodePreparationOutput{Body: seal}, nil
}

func (h *Handler) createEnrollmentToken(
	ctx context.Context, input *createEnrollmentTokenInput,
) (*enrollmentTokenOutput, error) {
	if problem := h.requireCoordinatorEnrollmentStore(); problem != nil {
		return nil, problem
	}
	if input.Body.ExpiresInSeconds <= 0 {
		return nil, httpapi.Validation(
			"body.expires_in_seconds", "expiry must be greater than zero",
		)
	}
	if _, err := h.enrollments.CleanupExpired(ctx); err != nil {
		return nil, httpapi.Internal("clean up expired enrollments: " + err.Error())
	}
	token, err := h.enrollments.CreateOneTimeToken(federation.Identity{
		NodeID: h.nodeID, Name: input.Body.Name, BaseURL: input.Body.BaseURL,
	}, h.now().Add(time.Duration(input.Body.ExpiresInSeconds)*time.Second))
	if err != nil {
		return nil, enrollmentProblem(err)
	}
	return &enrollmentTokenOutput{Body: token}, nil
}

func (h *Handler) beginEnrollment(
	ctx context.Context, input *beginEnrollmentInput,
) (*beginEnrollmentOutput, error) {
	if problem := h.requireCoordinatorEnrollmentStore(); problem != nil {
		return nil, problem
	}
	if _, err := h.enrollments.CleanupExpired(ctx); err != nil {
		return nil, httpapi.Internal("clean up expired enrollments: " + err.Error())
	}
	token, ok := enrollmentBearer(input.Authorization)
	if !ok {
		return nil, httpapi.NewProblem(
			http.StatusUnauthorized, httpapi.CodeUnauthorized,
			"missing or invalid one-time enrollment token", nil,
		)
	}
	enrollment, err := h.enrollments.Begin(ctx, token, input.Body)
	if err != nil {
		return nil, enrollmentProblem(err)
	}
	if h.credentials == nil {
		return nil, httpapi.ServiceUnavailable("federation credential store unavailable")
	}
	if err := h.credentials.StoreOutbound(
		enrollment.NodeID, input.Body.CoordinatorCredential,
		federationauth.PendingCoordinatorToNodeScopes(),
	); err != nil {
		return nil, httpapi.Internal("persist coordinator-to-node credential: " + err.Error())
	}
	nodeCredential, err := h.credentials.MintInbound(
		enrollment.NodeID, federationauth.PendingNodeToCoordinatorScopes(),
	)
	if err != nil {
		return nil, httpapi.Internal("persist node-to-coordinator credential: " + err.Error())
	}
	return &beginEnrollmentOutput{Body: federation.JoinResponse{
		EnrollmentID:    enrollment.ID,
		CoordinatorID:   enrollment.CoordinatorID,
		CoordinatorName: enrollment.CoordinatorName,
		CoordinatorURL:  enrollment.CoordinatorURL,
		NodeCredential:  nodeCredential,
		ProtocolVersion: federation.ProtocolVersion,
		State:           enrollment.State, ExpiresAt: enrollment.ExpiresAt,
		PreparationRequired: true,
	}}, nil
}

func (h *Handler) joinFederation(
	ctx context.Context, input *localJoinInput,
) (*localJoinOutput, error) {
	h.enrollmentMu.Lock()
	defer h.enrollmentMu.Unlock()

	if problem := h.requireFederationStores(); problem != nil {
		return nil, problem
	}
	fleet := h.configSnapshot().Fleet
	if !fleet.Enabled {
		return nil, httpapi.NewProblem(
			http.StatusConflict, httpapi.CodeConflict,
			"federation must be enabled before joining a coordinator",
			map[string]any{"reason": "federationDisabled"},
		)
	}
	if len(fleet.Members) != 0 {
		return nil, httpapi.Conflict(
			httpapi.CodeConflict,
			"revoke or migrate this coordinator's members before joining another coordinator",
			map[string]any{"reason": "coordinatorHasMembers"},
		)
	}
	coordinatorURL, err := federation.CanonicalOrigin(input.Body.CoordinatorURL)
	if err != nil {
		return nil, httpapi.Validation("body.coordinator_base_url", err.Error())
	}
	nodeBaseURL, err := federation.CanonicalOrigin(input.Body.NodeBaseURL)
	if err != nil {
		return nil, httpapi.Validation("body.node_base_url", err.Error())
	}
	token := strings.TrimSpace(input.Body.EnrollmentToken)
	if token == "" || strings.ContainsAny(token, " \t\r\n") {
		return nil, httpapi.Validation("body.enrollment_token", "enrollment token is required")
	}

	if existing, ok := h.enrollments.Local(); ok {
		if existing.State != federation.EnrollmentPending ||
			existing.CoordinatorID != "" || existing.PreparationStarted ||
			existing.Preparation != nil {
			return nil, httpapi.Conflict(
				httpapi.CodeConflict,
				"abort or revoke the existing local enrollment before joining again",
				map[string]any{"reason": "localEnrollmentExists"},
			)
		}
		if err := h.credentials.RevokePending(existing.EnrollmentID); err != nil {
			return nil, httpapi.Internal("revoke provisional coordinator credential: " + err.Error())
		}
		if err := h.enrollments.ClearLocal(ctx); err != nil {
			return nil, httpapi.Internal("clear provisional local enrollment: " + err.Error())
		}
	}
	enrollmentID, err := federation.NewID()
	if err != nil {
		return nil, httpapi.Internal(err.Error())
	}
	local := federation.LocalEnrollment{
		EnrollmentID: enrollmentID, NodeID: h.nodeID,
		NodeName: strings.TrimSpace(input.Body.Name), NodePlatform: runtime.GOOS,
		NodeBaseURL: nodeBaseURL, CoordinatorURL: coordinatorURL,
		ProtocolVersion: federation.ProtocolVersion,
		State:           federation.EnrollmentPending, PreparationRequired: true,
	}
	if err := h.enrollments.SaveLocal(ctx, local); err != nil {
		return nil, httpapi.Internal("persist pending local enrollment: " + err.Error())
	}
	coordinatorCredential, err := h.credentials.ReserveInbound(
		enrollmentID, federationauth.PendingCoordinatorToNodeScopes(),
	)
	if err != nil {
		return nil, httpapi.Internal("reserve coordinator credential: " + err.Error())
	}
	response, err := h.sendJoinRequest(ctx, coordinatorURL, token, federation.JoinRequest{
		EnrollmentID: enrollmentID, NodeID: h.nodeID,
		Name: local.NodeName, Platform: local.NodePlatform, BaseURL: nodeBaseURL,
		ProtocolVersion:       federation.ProtocolVersion,
		CoordinatorCredential: coordinatorCredential,
	})
	if err != nil {
		return nil, err
	}
	if err := validateJoinResponse(response, enrollmentID, coordinatorURL); err != nil {
		return nil, httpapi.NewProblem(
			http.StatusBadGateway, httpapi.CodeUpstreamError,
			"coordinator returned an invalid enrollment response",
			map[string]any{"reason": err.Error()},
		)
	}
	if err := h.credentials.BindInbound(enrollmentID, response.CoordinatorID); err != nil {
		return nil, httpapi.Internal("bind coordinator credential: " + err.Error())
	}
	if err := h.credentials.StoreOutbound(
		response.CoordinatorID, response.NodeCredential,
		federationauth.PendingNodeToCoordinatorScopes(),
	); err != nil {
		return nil, httpapi.Internal("persist node credential: " + err.Error())
	}
	local.CoordinatorID = response.CoordinatorID
	local.CoordinatorName = response.CoordinatorName
	local.CoordinatorURL = response.CoordinatorURL
	local.State = response.State
	local.ExpiresAt = response.ExpiresAt
	local.PreparationRequired = response.PreparationRequired
	if h.persistCoordinatorBinding == nil {
		return nil, httpapi.ServiceUnavailable("coordinator binding persistence unavailable")
	}
	if err := h.persistCoordinatorBinding(ctx, config.FleetCoordinator{
		NodeID: response.CoordinatorID,
		Name:   response.CoordinatorName, BaseURL: response.CoordinatorURL,
	}); err != nil {
		return nil, httpapi.Internal("persist coordinator binding: " + err.Error())
	}
	if err := h.enrollments.SaveLocal(ctx, local); err != nil {
		return nil, httpapi.Internal("persist coordinator enrollment: " + err.Error())
	}
	return &localJoinOutput{Body: local}, nil
}

func (h *Handler) activateEnrollment(
	ctx context.Context, input *activateEnrollmentInput,
) (*activateEnrollmentOutput, error) {
	if problem := h.requireCoordinatorEnrollmentStore(); problem != nil {
		return nil, problem
	}
	principal, ok := federationauth.PrincipalFromContext(ctx)
	if !ok {
		return nil, httpapi.NewProblem(
			http.StatusUnauthorized, httpapi.CodeUnauthorized,
			"federation principal is required", nil,
		)
	}
	if input.Body.ProtocolVersion != federation.ProtocolVersion {
		return nil, enrollmentProblem(fmt.Errorf(
			"%w: expected %d, got %d", federation.ErrProtocolMismatch,
			federation.ProtocolVersion, input.Body.ProtocolVersion,
		))
	}
	enrollment, err := h.enrollments.Resume(ctx, input.EnrollmentID, principal.NodeID)
	if err != nil {
		return nil, enrollmentProblem(err)
	}
	if enrollment.State == federation.EnrollmentActive {
		if err := h.credentials.UpdateInboundScopes(
			enrollment.NodeID, federationauth.NodeToCoordinatorScopes(),
		); err != nil {
			return nil, httpapi.Internal("activate node credential: " + err.Error())
		}
		if err := h.credentials.UpdateOutboundScopes(
			enrollment.NodeID, federationauth.CoordinatorToNodeScopes(),
		); err != nil {
			return nil, httpapi.Internal("activate coordinator credential: " + err.Error())
		}
		return &activateEnrollmentOutput{Body: enrollment}, nil
	}
	if enrollment.State == federation.EnrollmentRevoked {
		return nil, enrollmentProblem(federation.ErrEnrollmentRevoked)
	}
	if enrollment.State != federation.EnrollmentPending {
		return nil, enrollmentProblem(federation.ErrEnrollmentConflict)
	}
	if h.db == nil {
		return nil, httpapi.ServiceUnavailable("node preparation seal store unavailable")
	}
	seal, err := h.db.GetNodePreparationSeal(ctx, enrollment.ID)
	if err != nil {
		return nil, httpapi.Internal("read node preparation seal: " + err.Error())
	}
	presented := strings.TrimSpace(input.Body.PreparationSeal)
	if seal == nil || presented == "" ||
		seal.NodeID != principal.NodeID || seal.NodeID != enrollment.NodeID ||
		seal.CoordinatorNodeID != h.nodeID ||
		seal.ProtocolVersion != federation.ProtocolVersion ||
		subtle.ConstantTimeCompare([]byte(seal.Seal), []byte(presented)) != 1 {
		return nil, enrollmentProblem(federation.ErrPreparationRequired)
	}
	if h.persistMember == nil {
		return nil, httpapi.ServiceUnavailable("fleet membership persistence unavailable")
	}
	if err := h.persistMember(ctx, config.FleetMember{
		NodeID: enrollment.NodeID, Name: enrollment.NodeName,
		BaseURL: enrollment.NodeBaseURL, State: federation.EnrollmentActive,
	}); err != nil {
		return nil, httpapi.Internal("persist active fleet member: " + err.Error())
	}
	if err := h.enrollments.Activate(ctx, enrollment.ID); err != nil {
		return nil, enrollmentProblem(err)
	}
	if err := h.credentials.UpdateInboundScopes(
		enrollment.NodeID, federationauth.NodeToCoordinatorScopes(),
	); err != nil {
		return nil, httpapi.Internal("activate node credential: " + err.Error())
	}
	if err := h.credentials.UpdateOutboundScopes(
		enrollment.NodeID, federationauth.CoordinatorToNodeScopes(),
	); err != nil {
		return nil, httpapi.Internal("activate coordinator credential: " + err.Error())
	}
	enrollment, err = h.enrollments.Resume(ctx, enrollment.ID, enrollment.NodeID)
	if err != nil {
		return nil, enrollmentProblem(err)
	}
	return &activateEnrollmentOutput{Body: enrollment}, nil
}

func (h *Handler) getFederationIdentity(
	ctx context.Context, _ *struct{},
) (*federationIdentityOutput, error) {
	if _, ok := federationauth.PrincipalFromContext(ctx); !ok {
		return nil, httpapi.NewProblem(
			http.StatusUnauthorized, httpapi.CodeUnauthorized,
			"federation principal is required", nil,
		)
	}
	if h.nodeID == "" {
		return nil, httpapi.ServiceUnavailable("federation identity unavailable")
	}
	return &federationIdentityOutput{Body: federationIdentityOutputBody{
		NodeID: h.nodeID, ProtocolVersion: federation.ProtocolVersion,
	}}, nil
}

func (h *Handler) revokeEnrollment(
	ctx context.Context, input *revokeEnrollmentInput,
) (*struct{}, error) {
	if problem := h.requireCoordinatorEnrollmentStore(); problem != nil {
		return nil, problem
	}
	enrollment, err := h.enrollments.Get(ctx, input.EnrollmentID)
	if err != nil {
		return nil, enrollmentProblem(err)
	}
	if err := h.revokeEnrollmentState(ctx, enrollment); err != nil {
		return nil, err
	}
	return &struct{}{}, nil
}

func (h *Handler) abortEnrollment(
	ctx context.Context, input *abortEnrollmentInput,
) (*struct{}, error) {
	if problem := h.requireCoordinatorEnrollmentStore(); problem != nil {
		return nil, problem
	}
	principal, ok := federationauth.PrincipalFromContext(ctx)
	if !ok {
		return nil, httpapi.NewProblem(
			http.StatusUnauthorized, httpapi.CodeUnauthorized,
			"federation principal is required", nil,
		)
	}
	enrollment, err := h.enrollments.Resume(ctx, input.EnrollmentID, principal.NodeID)
	if err != nil {
		return nil, enrollmentProblem(err)
	}
	if enrollment.State != federation.EnrollmentPending {
		return nil, httpapi.Conflict(
			httpapi.CodeConflict, "only a pending enrollment can be aborted",
			map[string]any{"reason": "enrollmentNotPending"},
		)
	}
	if err := h.revokeEnrollmentState(ctx, enrollment); err != nil {
		return nil, err
	}
	return &struct{}{}, nil
}

func (h *Handler) revokeEnrollmentState(
	ctx context.Context, enrollment federation.Enrollment,
) error {
	if h.credentials != nil {
		if err := h.credentials.RevokeInboundNode(enrollment.NodeID); err != nil {
			return httpapi.Internal("revoke inbound federation credential: " + err.Error())
		}
		if h.cancelEventStreams != nil {
			h.cancelEventStreams(enrollment.NodeID)
		}
		if err := h.credentials.RevokeOutbound(enrollment.NodeID); err != nil {
			return httpapi.Internal("revoke outbound federation credential: " + err.Error())
		}
	}
	if h.removeMember != nil {
		if err := h.removeMember(ctx, enrollment.NodeID); err != nil {
			return httpapi.Internal("remove fleet member: " + err.Error())
		}
	}
	if err := h.enrollments.Revoke(ctx, enrollment.ID); err != nil {
		return enrollmentProblem(err)
	}
	return nil
}

func (h *Handler) requireFederationStores() error {
	if h == nil {
		return httpapi.ServiceUnavailable("federation enrollment is unavailable")
	}
	if h.enrollments == nil || h.credentials == nil || h.nodeID == "" {
		return httpapi.ServiceUnavailable("federation enrollment is unavailable")
	}
	return nil
}

func (h *Handler) requireCoordinatorEnrollmentStore() error {
	if problem := h.requireFederationStores(); problem != nil {
		return problem
	}
	fleet := h.configSnapshot().Fleet
	if !fleet.Enabled || fleet.RoleOrDefault() != config.FleetRoleCoordinator {
		return httpapi.NewProblem(
			http.StatusConflict, httpapi.CodeConflict,
			"this daemon is not an enabled federation coordinator",
			map[string]any{"reason": "notCoordinator"},
		)
	}
	return nil
}

func (h *Handler) sendJoinRequest(
	ctx context.Context, coordinatorURL, enrollmentToken string,
	request federation.JoinRequest,
) (federation.JoinResponse, error) {
	body, err := json.Marshal(request)
	if err != nil {
		return federation.JoinResponse{}, httpapi.Internal("encode enrollment request: " + err.Error())
	}
	endpoint := coordinatorURL + "/api/v1/federation/enrollments"
	httpRequest, err := http.NewRequestWithContext(
		ctx, http.MethodPost, endpoint, bytes.NewReader(body),
	)
	if err != nil {
		return federation.JoinResponse{}, httpapi.Internal("build enrollment request: " + err.Error())
	}
	httpRequest.Header.Set("Authorization", "Bearer "+enrollmentToken)
	httpRequest.Header.Set("Content-Type", "application/json")
	response, err := h.federationHTTPClient.Do(httpRequest)
	if err != nil {
		return federation.JoinResponse{}, httpapi.NewProblem(
			http.StatusServiceUnavailable, httpapi.CodeServiceUnavailable,
			"the coordinator may have accepted the enrollment, so Forge will not retry with the one-time token; revoke this enrollment and join again with a new token",
			map[string]any{"reason": "coordinatorUnavailable", "error": err.Error()},
		)
	}
	return decodeJoinResponse(response)
}

func decodeJoinResponse(response *http.Response) (federation.JoinResponse, error) {
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(
		response.Body, maxFederationEnrollmentResponseBytes+1,
	))
	if err != nil {
		return federation.JoinResponse{}, httpapi.NewProblem(
			http.StatusBadGateway, httpapi.CodeUpstreamError,
			"read coordinator enrollment response", nil,
		)
	}
	if len(raw) > maxFederationEnrollmentResponseBytes {
		return federation.JoinResponse{}, httpapi.NewProblem(
			http.StatusBadGateway, httpapi.CodeUpstreamError,
			"coordinator enrollment response is too large", nil,
		)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var problem httpapi.ProblemError
		if json.Unmarshal(raw, &problem) == nil && problem.Status != 0 {
			return federation.JoinResponse{}, &problem
		}
		return federation.JoinResponse{}, httpapi.NewProblem(
			http.StatusBadGateway, httpapi.CodeUpstreamError,
			fmt.Sprintf("coordinator enrollment returned HTTP %d", response.StatusCode), nil,
		)
	}
	var result federation.JoinResponse
	if err := json.Unmarshal(raw, &result); err != nil {
		return federation.JoinResponse{}, httpapi.NewProblem(
			http.StatusBadGateway, httpapi.CodeUpstreamError,
			"decode coordinator enrollment response", nil,
		)
	}
	return result, nil
}

func validateJoinResponse(
	response federation.JoinResponse, enrollmentID, coordinatorURL string,
) error {
	if response.EnrollmentID != enrollmentID {
		return errors.New("enrollmentIDMismatch")
	}
	if response.ProtocolVersion != federation.ProtocolVersion {
		return errors.New("protocolVersionMismatch")
	}
	canonicalURL, err := federation.CanonicalOrigin(response.CoordinatorURL)
	if err != nil || canonicalURL != coordinatorURL {
		return errors.New("coordinatorOriginMismatch")
	}
	if !federation.ValidNodeID(response.CoordinatorID) ||
		response.NodeCredential == "" ||
		strings.TrimSpace(response.NodeCredential) != response.NodeCredential ||
		strings.ContainsAny(response.NodeCredential, " \t\r\n") {
		return errors.New("coordinatorIdentityInvalid")
	}
	if response.State != federation.EnrollmentPending &&
		response.State != federation.EnrollmentActive {
		return errors.New("enrollmentStateInvalid")
	}
	if response.ExpiresAt.IsZero() {
		return errors.New("enrollmentExpiryInvalid")
	}
	return nil
}

func enrollmentBearer(authorization string) (string, bool) {
	token, ok := strings.CutPrefix(authorization, "Bearer ")
	if !ok {
		return "", false
	}
	token = strings.TrimSpace(token)
	return token, token != "" && !strings.ContainsAny(token, " \t\r\n")
}

func enrollmentProblem(err error) error {
	switch {
	case errors.Is(err, federation.ErrEnrollmentTokenInvalid):
		return httpapi.NewProblem(http.StatusUnauthorized, httpapi.CodeUnauthorized,
			"invalid one-time enrollment token", map[string]any{"reason": "enrollmentTokenInvalid"})
	case errors.Is(err, federation.ErrEnrollmentTokenExpired):
		return httpapi.NewProblem(http.StatusUnauthorized, httpapi.CodeUnauthorized,
			"expired one-time enrollment token", map[string]any{"reason": "enrollmentTokenExpired"})
	case errors.Is(err, federation.ErrEnrollmentTokenConsumed):
		return httpapi.NewProblem(http.StatusConflict, httpapi.CodeConflict,
			"one-time enrollment token was consumed by another enrollment",
			map[string]any{"reason": "enrollmentTokenConsumed"})
	case errors.Is(err, federation.ErrDuplicateNodeID):
		return httpapi.NewProblem(http.StatusConflict, httpapi.CodeConflict,
			"node ID is already enrolled at another origin",
			map[string]any{"reason": "duplicateNodeID"})
	case errors.Is(err, federation.ErrDuplicateOrigin),
		errors.Is(err, federation.ErrEnrollmentConflict):
		return httpapi.NewProblem(http.StatusConflict, httpapi.CodeConflict,
			err.Error(), map[string]any{"reason": "enrollmentConflict"})
	case errors.Is(err, federation.ErrProtocolMismatch):
		return httpapi.NewProblem(http.StatusConflict, httpapi.CodeConflict,
			err.Error(), map[string]any{
				"reason": "protocolMismatch", "expected": federation.ProtocolVersion,
			})
	case errors.Is(err, federation.ErrPreparationRequired):
		return httpapi.NewProblem(http.StatusConflict, httpapi.CodeConflict,
			err.Error(), map[string]any{"reason": "preparationRequired"})
	case errors.Is(err, federation.ErrEnrollmentNotFound):
		return httpapi.NotFound(httpapi.CodeNotFound, err.Error(), nil)
	case errors.Is(err, federation.ErrEnrollmentRevoked):
		return httpapi.NewProblem(http.StatusForbidden, httpapi.CodeForbidden,
			err.Error(), map[string]any{"reason": "enrollmentRevoked"})
	default:
		return httpapi.BadRequest(httpapi.CodeBadRequest, err.Error(), nil)
	}
}

func newFederationHTTPClient() *http.Client {
	return hardenedFederationHTTPClient(http.DefaultClient, false)
}

func newFederationMemberClients(base *http.Client) federationMemberClients {
	return federationMemberClients{
		rest:      hardenedFederationHTTPClient(base, false),
		proxy:     hardenedFederationProxyHTTPClient(base),
		websocket: hardenedFederationHTTPClient(base, true),
	}
}

func hardenedFederationProxyHTTPClient(base *http.Client) *http.Client {
	client := hardenedFederationHTTPClient(base, true)
	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		return client
	}
	transport = transport.Clone()
	transport.ResponseHeaderTimeout = 0
	client.Transport = transport
	return client
}

func hardenedFederationHTTPClient(base *http.Client, streaming bool) *http.Client {
	if base == nil {
		base = http.DefaultClient
	}
	client := *base
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	if streaming {
		client.Timeout = 0
	} else if client.Timeout <= 0 || client.Timeout > 15*time.Second {
		client.Timeout = 15 * time.Second
	}

	var transport *http.Transport
	switch existing := base.Transport.(type) {
	case nil:
		transport = http.DefaultTransport.(*http.Transport).Clone()
	case *http.Transport:
		transport = existing.Clone()
	}
	if transport == nil {
		return &client
	}
	transport.DialContext = (&net.Dialer{
		Timeout: 5 * time.Second, KeepAlive: 30 * time.Second,
	}).DialContext
	if transport.TLSHandshakeTimeout <= 0 || transport.TLSHandshakeTimeout > 5*time.Second {
		transport.TLSHandshakeTimeout = 5 * time.Second
	}
	if transport.ResponseHeaderTimeout <= 0 || transport.ResponseHeaderTimeout > 10*time.Second {
		transport.ResponseHeaderTimeout = 10 * time.Second
	}
	if transport.ExpectContinueTimeout <= 0 || transport.ExpectContinueTimeout > time.Second {
		transport.ExpectContinueTimeout = time.Second
	}
	if transport.MaxResponseHeaderBytes <= 0 || transport.MaxResponseHeaderBytes > 1<<20 {
		transport.MaxResponseHeaderBytes = 1 << 20
	}
	client.Transport = transport
	return &client
}
