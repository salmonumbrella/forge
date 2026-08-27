package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"go.kenn.io/forge/internal/config"
	"go.kenn.io/forge/internal/db"
	"go.kenn.io/forge/internal/federation"
	"go.kenn.io/forge/internal/federationauth"
	"go.kenn.io/forge/internal/gitclone"
	"go.kenn.io/forge/internal/providerplane"
	"go.kenn.io/forge/internal/server/httpapi"
)

const maxNodePreparationResponseBytes = 1 << 20

type NodePreparationReport struct {
	ReadyLaunchSpecs       int                        `json:"ready_launch_specs"`
	Unprepared             []db.UnpreparedWorkspace   `json:"unprepared" nullable:"false"`
	HandoffConflicts       []db.ProviderStateConflict `json:"handoff_conflicts" nullable:"false"`
	HandoffErrors          []string                   `json:"handoff_errors" nullable:"false"`
	InFlightProviderWrites int                        `json:"in_flight_provider_writes"`
	ActiveDeferredMerges   int                        `json:"active_deferred_merges"`
	UndrainedAcks          int                        `json:"undrained_acks"`
	ReadyToActivate        bool                       `json:"ready_to_activate"`
	PreparationSeal        string                     `json:"preparation_seal,omitempty"`
	RestartRequired        bool                       `json:"restart_required"`
}

type prepareFederationNodeOutput = httpapi.BodyOutput[NodePreparationReport]

type abortFederationNodeInput struct {
	Body struct {
		Force bool `json:"force,omitempty"`
	}
}

type NodePreparationAbortReport struct {
	EnrollmentID       string `json:"enrollment_id"`
	CoordinatorRevoked bool   `json:"coordinator_revoked"`
	ProviderWritesOpen bool   `json:"provider_writes_open"`
	RestartRequired    bool   `json:"restart_required"`
}

type abortFederationNodeOutput = httpapi.BodyOutput[NodePreparationAbortReport]

func (s *Server) registerNodePreparationAPI(api huma.API) {
	huma.Register(api, huma.Operation{
		OperationID: "prepare-federation-node",
		Method:      http.MethodPost,
		Path:        "/fleet/prepare-node",
		Summary:     "Quiesce this daemon and prepare it to become a fleet node",
		Tags:        []string{"Fleet"},
	}, s.prepareFederationNode)
	huma.Register(api, huma.Operation{
		OperationID: "abort-federation-node-preparation",
		Method:      http.MethodPost,
		Path:        "/fleet/prepare-node/abort",
		Summary:     "Abort pending node preparation and restore standalone writes",
		Tags:        []string{"Fleet"},
	}, s.abortFederationNodePreparation)
}

func (s *Server) abortFederationNodePreparation(
	ctx context.Context, input *abortFederationNodeInput,
) (*abortFederationNodeOutput, error) {
	if s.options.FederationEnrollments == nil ||
		s.options.FederationCredentials == nil {
		return nil, httpapi.ServiceUnavailable("federation enrollment is unavailable")
	}
	local, ok := s.options.FederationEnrollments.Local()
	if !ok || local.State != federation.EnrollmentPending || local.CoordinatorID == "" {
		return nil, httpapi.Conflict(
			httpapi.CodeConflict, "a pending coordinator enrollment is required",
			map[string]any{"reason": "pendingEnrollmentRequired"},
		)
	}
	if err := s.providerWriteGate.CanAbortPreparation(); err != nil {
		return nil, httpapi.Conflict(
			httpapi.CodeConflict, "cannot reopen provider writes: "+err.Error(),
			map[string]any{"reason": "providerWritesStillDraining"},
		)
	}
	report := NodePreparationAbortReport{EnrollmentID: local.EnrollmentID}
	if local.PreparationStarted || local.ExpiresAt.After(s.now().UTC()) {
		if err := s.requestCoordinatorEnrollmentAbort(ctx, local); err != nil {
			if !input.Body.Force {
				return nil, nodePreparationCoordinatorProblem(err)
			}
		} else {
			report.CoordinatorRevoked = true
		}
	}
	if err := s.providerWriteGate.AbortPreparation(ctx); err != nil {
		return nil, httpapi.Conflict(
			httpapi.CodeConflict, "cannot reopen provider writes: "+err.Error(),
			map[string]any{"reason": "providerWritesStillDraining"},
		)
	}
	report.RestartRequired = s.providerRouteNode
	report.ProviderWritesOpen = !report.RestartRequired
	if err := s.resetPreparedNodeBinding(ctx); err != nil {
		return nil, httpapi.Internal("restore standalone fleet role: " + err.Error())
	}
	if err := s.options.FederationCredentials.RevokeOutbound(local.CoordinatorID); err != nil {
		return nil, httpapi.Internal("revoke coordinator credential: " + err.Error())
	}
	if err := s.options.FederationCredentials.RevokeInboundNode(local.CoordinatorID); err != nil {
		return nil, httpapi.Internal("revoke inbound coordinator credential: " + err.Error())
	}
	if err := s.options.FederationEnrollments.ClearLocal(ctx); err != nil {
		return nil, httpapi.Internal("clear local enrollment: " + err.Error())
	}
	return &abortFederationNodeOutput{Body: report}, nil
}

func (s *Server) prepareFederationNode(
	ctx context.Context,
	_ *struct{},
) (*prepareFederationNodeOutput, error) {
	if s.options.FederationEnrollments == nil ||
		s.options.FederationCredentials == nil ||
		s.options.FederationNodeID == "" {
		return nil, httpapi.ServiceUnavailable("federation enrollment is unavailable")
	}
	local, ok := s.options.FederationEnrollments.Local()
	if !ok || local.State != federation.EnrollmentPending ||
		local.CoordinatorID == "" {
		return nil, httpapi.Conflict(
			httpapi.CodeConflict,
			"a pending coordinator enrollment is required before node preparation",
			map[string]any{"reason": "pendingEnrollmentRequired"},
		)
	}
	if err := s.pinCoordinatorEnrollment(ctx, local); err != nil {
		return nil, nodePreparationCoordinatorProblem(err)
	}
	if _, err := s.providerWriteGate.BeginQuiesce(ctx, db.NodePreparationBinding{
		EnrollmentID:      local.EnrollmentID,
		CoordinatorNodeID: local.CoordinatorID,
		LocalNodeID:       local.NodeID,
		ProtocolVersion:   local.ProtocolVersion,
	}); err != nil {
		if errors.Is(err, db.ErrNodePreparationConflict) {
			return nil, httpapi.Conflict(httpapi.CodeConflict, err.Error(), map[string]any{
				"reason": "nodePreparationConflict",
			})
		}
		return nil, httpapi.Internal("begin node preparation: " + err.Error())
	}

	report := NodePreparationReport{
		Unprepared:       []db.UnpreparedWorkspace{},
		HandoffConflicts: []db.ProviderStateConflict{},
		HandoffErrors:    []string{},
	}
	status, err := s.providerWriteGate.Status(ctx)
	if err != nil {
		return nil, httpapi.Internal("read node preparation status: " + err.Error())
	}
	report.InFlightProviderWrites = status.InFlightProviderWrites
	report.ActiveDeferredMerges = status.ActiveDeferredMerges
	report.UndrainedAcks = status.UndrainedAcks
	if status.InFlightProviderWrites != 0 || status.ActiveDeferredMerges != 0 ||
		status.DrainAckGeneration == nil {
		return &prepareFederationNodeOutput{Body: report}, nil
	}
	client, err := s.nodePreparationProviderClient(local)
	if err != nil {
		report.HandoffErrors = append(report.HandoffErrors, err.Error())
	} else {
		s.reconcileNodePreparationProjects(ctx, client, &report)
		s.refreshNodePreparationLaunchSpecs(ctx, client, &report)
		s.handoffNodeProviderState(ctx, client, &report)
	}

	report.Unprepared, err = s.db.ListUnpreparedProviderWorkspacesAt(ctx, s.now().UTC())
	if err != nil {
		return nil, httpapi.Internal("list unprepared workspaces: " + err.Error())
	}
	total, err := s.db.CountProviderBackedWorkspaces(ctx)
	if err != nil {
		return nil, httpapi.Internal(err.Error())
	}
	report.ReadyLaunchSpecs = total - len(report.Unprepared)
	status, err = s.providerWriteGate.Status(ctx)
	if err != nil {
		return nil, httpapi.Internal("read node preparation status: " + err.Error())
	}
	report.InFlightProviderWrites = status.InFlightProviderWrites
	report.ActiveDeferredMerges = status.ActiveDeferredMerges
	report.UndrainedAcks = status.UndrainedAcks
	if len(report.Unprepared) != 0 || len(report.HandoffConflicts) != 0 ||
		len(report.HandoffErrors) != 0 || status.InFlightProviderWrites != 0 ||
		status.ActiveDeferredMerges != 0 || status.UndrainedAcks != 0 ||
		status.DrainAckGeneration == nil {
		return &prepareFederationNodeOutput{Body: report}, nil
	}
	receipts, err := s.db.ListNodePreparationReceipts(ctx)
	if err != nil {
		return nil, httpapi.Internal("list provider state receipts: " + err.Error())
	}
	receiptsDigest, err := nodePreparationReceiptsDigest(receipts)
	if err != nil {
		return nil, httpapi.Internal(err.Error())
	}
	sealRequest := db.NodePreparationSealRequest{
		EnrollmentID: local.EnrollmentID, NodeID: local.NodeID,
		CoordinatorNodeID:    local.CoordinatorID,
		ProtocolVersion:      local.ProtocolVersion,
		MigrationVersion:     db.WorkspaceLaunchSpecMigrationVersion,
		ReceiptsDigest:       receiptsDigest,
		DrainedAckGeneration: *status.DrainAckGeneration,
	}
	sealRequest.PreparationDigest, err = db.NodePreparationSealDigest(sealRequest)
	if err != nil {
		return nil, httpapi.Internal(err.Error())
	}
	seal, err := s.requestCoordinatorPreparationSeal(ctx, local, sealRequest)
	if err != nil {
		report.HandoffErrors = append(report.HandoffErrors, err.Error())
		return &prepareFederationNodeOutput{Body: report}, nil
	}
	if err := validateCoordinatorPreparationSeal(sealRequest, seal); err != nil {
		report.HandoffErrors = append(report.HandoffErrors, err.Error())
		return &prepareFederationNodeOutput{Body: report}, nil
	}
	if err := s.db.StoreLocalNodePreparationSeal(
		ctx, sealRequest.PreparationDigest, seal.Seal,
	); err != nil {
		return nil, httpapi.Internal("store node preparation seal: " + err.Error())
	}
	if err := s.options.FederationEnrollments.SaveLocalPreparationSeal(
		ctx, federation.LocalPreparationSeal{
			EnrollmentID: seal.EnrollmentID, NodeID: seal.NodeID,
			CoordinatorID:     seal.CoordinatorNodeID,
			ProtocolVersion:   seal.ProtocolVersion,
			PreparationDigest: seal.PreparationDigest,
			Seal:              seal.Seal,
		},
	); err != nil {
		return nil, httpapi.Internal("persist node preparation seal: " + err.Error())
	}
	if err := s.persistPreparedNodeRole(ctx, local, seal); err != nil {
		return nil, httpapi.Internal("persist fleet node role: " + err.Error())
	}
	report.ReadyToActivate = true
	report.PreparationSeal = seal.Seal
	report.RestartRequired = true
	return &prepareFederationNodeOutput{Body: report}, nil
}

func (s *Server) persistPreparedNodeRole(
	ctx context.Context,
	local federation.LocalEnrollment,
	seal db.NodePreparationSeal,
) error {
	prepared, ok := s.options.FederationEnrollments.Local()
	if !ok || prepared.State != federation.EnrollmentPending || prepared.Preparation == nil {
		return errors.New("a sealed pending local enrollment is required before changing fleet role")
	}
	if prepared.EnrollmentID != local.EnrollmentID || prepared.NodeID != local.NodeID ||
		prepared.CoordinatorID != local.CoordinatorID ||
		prepared.ProtocolVersion != federation.ProtocolVersion ||
		prepared.Preparation.EnrollmentID != seal.EnrollmentID ||
		prepared.Preparation.NodeID != seal.NodeID ||
		prepared.Preparation.CoordinatorID != seal.CoordinatorNodeID ||
		prepared.Preparation.ProtocolVersion != seal.ProtocolVersion ||
		prepared.Preparation.Seal != seal.Seal {
		return federation.ErrPreparationSealMismatch
	}
	return s.mutatePersistedFleetChecked(ctx, func(fleet *config.Fleet) error {
		if !fleet.Enabled || fleet.Coordinator == nil ||
			fleet.Coordinator.NodeID != prepared.CoordinatorID ||
			fleet.Coordinator.BaseURL != prepared.CoordinatorURL {
			return errors.New("fleet coordinator config does not match the sealed local enrollment")
		}
		if len(fleet.Members) != 0 {
			return errors.New("revoke or migrate coordinator members before changing fleet role to node")
		}
		fleet.Role = config.FleetRoleNode
		fleet.Members = nil
		return nil
	})
}

func validateCoordinatorPreparationSeal(
	request db.NodePreparationSealRequest,
	seal db.NodePreparationSeal,
) error {
	if seal.NodePreparationSealRequest != request {
		return errors.New("coordinator returned a preparation seal for a different binding")
	}
	if strings.TrimSpace(seal.Seal) == "" || seal.CreatedAt.IsZero() {
		return errors.New("coordinator returned an incomplete preparation seal")
	}
	return nil
}

func (s *Server) nodePreparationProviderClient(
	local federation.LocalEnrollment,
) (providerplane.Client, error) {
	return providerplane.NewClient(providerplane.Options{
		LocalNodeID: local.NodeID,
		Coordinator: providerplane.Coordinator{
			NodeID: local.CoordinatorID, BaseURL: local.CoordinatorURL,
		},
		Credentials: s.options.FederationCredentials,
		HTTPClient:  s.options.FederationHTTPClient,
	})
}

func (s *Server) refreshNodePreparationLaunchSpecs(
	ctx context.Context,
	client providerplane.Client,
	report *NodePreparationReport,
) {
	unprepared, err := s.db.ListUnpreparedProviderWorkspacesAt(ctx, s.now().UTC())
	if err != nil {
		report.HandoffErrors = append(report.HandoffErrors, "list launch specifications: "+err.Error())
		return
	}
	for _, item := range unprepared {
		workspace := item.Workspace
		current, err := s.db.GetWorkspaceLaunchSpec(ctx, workspace.ID)
		if err != nil {
			report.HandoffErrors = append(report.HandoffErrors,
				fmt.Sprintf("read workspace %s launch specification: %v", workspace.ID, err))
			continue
		}
		body := providerplane.WorkspaceLaunchRequest{
			Repository: providerplane.RepositoryRoute{
				Provider: workspace.Platform, PlatformHost: workspace.PlatformHost,
				Owner: workspace.RepoOwner, Name: workspace.RepoName,
			},
			ItemType: workspace.ItemType, ItemNumber: workspace.ItemNumber,
			ItemKey: workspace.ItemKey, GitHeadRef: workspace.GitHeadRef,
		}
		if current != nil {
			body.PlatformRepoID = current.Repository.PlatformRepoID
		}
		var spec db.WorkspaceLaunchSpec
		if err := nodePreparationProviderJSON(
			ctx, client, federationauth.ScopeProviderRead,
			http.MethodPost, "/api/v1/federation/provider/workspace-launch-spec",
			body, &spec,
		); err != nil {
			report.HandoffErrors = append(report.HandoffErrors,
				fmt.Sprintf("refresh workspace %s: %v", workspace.ID, err))
			continue
		}
		if err := providerplane.ValidateFederationWorkspaceLaunchSpecResponse(body, spec); err != nil {
			report.HandoffErrors = append(report.HandoffErrors,
				fmt.Sprintf("refresh workspace %s: invalid coordinator launch specification: %v", workspace.ID, err))
			continue
		}
		var credentialErr error
		if s.clones == nil {
			credentialErr = gitclone.ErrCredentialUnavailable
		} else {
			credentialErr = s.clones.RequireCredentialRoute(
				ctx, spec.Repository.Provider, spec.Repository.PlatformHost,
				spec.Repository.Owner, spec.Repository.Name,
			)
		}
		if credentialErr != nil {
			report.HandoffErrors = append(report.HandoffErrors,
				fmt.Sprintf("refresh workspace %s: %v", workspace.ID, credentialErr))
			continue
		}
		if _, err := s.db.PutRefreshedWorkspaceLaunchSpec(
			ctx, workspace.ID, spec,
		); err != nil {
			report.HandoffErrors = append(report.HandoffErrors,
				fmt.Sprintf("persist workspace %s launch specification: %v", workspace.ID, err))
		}
	}
}

func (s *Server) reconcileNodePreparationProjects(
	ctx context.Context,
	client providerplane.Client,
	report *NodePreparationReport,
) {
	projects, err := s.db.ListProjects(ctx)
	if err != nil {
		report.HandoffErrors = append(report.HandoffErrors, "list registered projects: "+err.Error())
		return
	}
	seen := make(map[providerplane.RepositoryRoute]struct{}, len(projects))
	for _, project := range projects {
		if project.PlatformIdentity == nil {
			continue
		}
		route, err := providerplane.CanonicalRepositoryRoute(providerplane.RepositoryRoute{
			Provider:     project.PlatformIdentity.Platform,
			PlatformHost: project.PlatformIdentity.Host,
			Owner:        project.PlatformIdentity.Owner,
			Name:         project.PlatformIdentity.Name,
		})
		if err != nil {
			report.HandoffErrors = append(report.HandoffErrors,
				fmt.Sprintf("resolve project %s repository: %v", project.ID, err))
			continue
		}
		if _, ok := seen[route]; ok {
			continue
		}
		seen[route] = struct{}{}
		var descriptor providerplane.RepositoryDescriptor
		if err := nodePreparationProviderJSON(
			ctx, client, federationauth.ScopeProviderRead,
			http.MethodPost, "/api/v1/federation/provider/repository-descriptor",
			route, &descriptor,
		); err != nil {
			report.HandoffErrors = append(report.HandoffErrors,
				fmt.Sprintf("resolve project %s repository: %v", project.ID, err))
			continue
		}
		if err := descriptor.ValidateRoute(route); err != nil {
			report.HandoffErrors = append(report.HandoffErrors,
				fmt.Sprintf("resolve project %s repository: invalid coordinator descriptor: %v", project.ID, err))
			continue
		}
		if err := observeRepositoryDescriptor(ctx, s.db, descriptor); err != nil {
			report.HandoffErrors = append(report.HandoffErrors,
				fmt.Sprintf("persist project %s repository: %v", project.ID, err))
		}
	}
}

func (s *Server) handoffNodeProviderState(
	ctx context.Context,
	client providerplane.Client,
	report *NodePreparationReport,
) {
	records, err := s.db.ListProviderStateForHandoff(ctx)
	if err != nil {
		report.HandoffErrors = append(report.HandoffErrors, "inventory provider state: "+err.Error())
		return
	}
	receipts, err := s.db.ListNodePreparationReceipts(ctx)
	if err != nil {
		report.HandoffErrors = append(report.HandoffErrors, "read provider state receipts: "+err.Error())
		return
	}
	received := make(map[string]db.NodePreparationReceipt, len(receipts))
	for _, receipt := range receipts {
		received[receipt.StateKind+"\x00"+receipt.SourceKey] = receipt
	}
	for _, record := range records {
		if receipt, ok := received[record.Kind+"\x00"+record.SourceKey]; ok &&
			receipt.ContentDigest == record.ContentDigest {
			continue
		}
		path := "/api/v1/federation/provider-state/workflow-states/import"
		var body any = record.WorkflowState
		if record.Kind == db.ProviderStateReviewDraft {
			path = "/api/v1/federation/provider-state/review-drafts/import"
			body = record.ReviewDraft
		}
		var result db.ProviderStateImportResult
		if err := nodePreparationProviderJSON(
			ctx, client, federationauth.ScopeProviderHandoff,
			http.MethodPost, path, body, &result,
		); err != nil {
			report.HandoffErrors = append(report.HandoffErrors,
				fmt.Sprintf("handoff %s %s: %v", record.Kind, record.SourceKey, err))
			continue
		}
		if result.Conflict != nil {
			report.HandoffConflicts = append(report.HandoffConflicts, *result.Conflict)
			continue
		}
		if strings.TrimSpace(result.Receipt) == "" {
			report.HandoffErrors = append(report.HandoffErrors,
				fmt.Sprintf("handoff %s %s returned no receipt", record.Kind, record.SourceKey))
			continue
		}
		if err := s.db.RecordNodePreparationReceipt(ctx, db.NodePreparationReceipt{
			StateKind: record.Kind, SourceKey: record.SourceKey,
			ContentDigest:      record.ContentDigest,
			CoordinatorReceipt: result.Receipt, ImportedAt: s.now().UTC(),
		}); err != nil {
			report.HandoffErrors = append(report.HandoffErrors,
				fmt.Sprintf("record %s %s receipt: %v", record.Kind, record.SourceKey, err))
		}
	}
}

func nodePreparationProviderJSON(
	ctx context.Context,
	client providerplane.Client,
	scope federationauth.Scope,
	method string,
	path string,
	body any,
	target any,
) error {
	encoded, err := json.Marshal(body)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(
		ctx, method, "https://coordinator.invalid"+path, bytes.NewReader(encoded),
	)
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	return providerplane.ReadJSON(ctx, client, scope, request, target)
}

func (s *Server) pinCoordinatorEnrollment(
	ctx context.Context,
	local federation.LocalEnrollment,
) error {
	var response federation.Enrollment
	if err := s.postCoordinatorEnrollmentJSON(
		ctx, local,
		"/api/v1/federation/enrollments/"+local.EnrollmentID+"/preparation/begin",
		map[string]any{}, &response,
	); err != nil {
		return err
	}
	if response.ID != local.EnrollmentID || response.NodeID != local.NodeID ||
		response.CoordinatorID != local.CoordinatorID ||
		response.State != federation.EnrollmentPending || !response.PreparationStarted {
		return errors.New("coordinator pinned a different enrollment")
	}
	return s.options.FederationEnrollments.MarkLocalPreparationStarted(
		ctx, local.EnrollmentID,
	)
}

func (s *Server) requestCoordinatorPreparationSeal(
	ctx context.Context,
	local federation.LocalEnrollment,
	request db.NodePreparationSealRequest,
) (db.NodePreparationSeal, error) {
	body := map[string]any{
		"node_id":                request.NodeID,
		"coordinator_node_id":    request.CoordinatorNodeID,
		"protocol_version":       request.ProtocolVersion,
		"migration_version":      request.MigrationVersion,
		"receipts_digest":        request.ReceiptsDigest,
		"drained_ack_generation": request.DrainedAckGeneration,
		"preparation_digest":     request.PreparationDigest,
	}
	var seal db.NodePreparationSeal
	err := s.postCoordinatorEnrollmentJSON(
		ctx, local,
		"/api/v1/federation/enrollments/"+local.EnrollmentID+"/preparation/seal",
		body, &seal,
	)
	return seal, err
}

func (s *Server) requestCoordinatorEnrollmentAbort(
	ctx context.Context, local federation.LocalEnrollment,
) error {
	var response struct{}
	return s.postCoordinatorEnrollmentJSON(
		ctx, local,
		"/api/v1/federation/enrollments/"+local.EnrollmentID+"/abort",
		struct{}{}, &response,
	)
}

func (s *Server) postCoordinatorEnrollmentJSON(
	ctx context.Context,
	local federation.LocalEnrollment,
	path string,
	body any,
	target any,
) error {
	credential, ok := s.options.FederationCredentials.Outbound(local.CoordinatorID)
	if !ok || !slices.Contains(credential.Scopes, federationauth.ScopeEnrollmentActivate) {
		return providerplane.ErrCredentialUnavailable
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(
		ctx, http.MethodPost, local.CoordinatorURL+path, bytes.NewReader(encoded),
	)
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+credential.Token)
	request.Header.Set(federationauth.NodeIDHeader, local.NodeID)
	request.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 15 * time.Second}
	if s.options.FederationHTTPClient != nil {
		*client = *s.options.FederationHTTPClient
		if client.Timeout <= 0 || client.Timeout > 15*time.Second {
			client.Timeout = 15 * time.Second
		}
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("%w: %v", providerplane.ErrCoordinatorUnavailable, err)
	}
	defer response.Body.Close()
	encodedResponse, err := io.ReadAll(io.LimitReader(
		response.Body, maxNodePreparationResponseBytes+1,
	))
	if err != nil {
		return err
	}
	if len(encodedResponse) > maxNodePreparationResponseBytes {
		return providerplane.ErrResponseBodyTooLarge
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		var problem httpapi.ProblemError
		if json.Unmarshal(encodedResponse, &problem) == nil && problem.Status != 0 {
			return &problem
		}
		return fmt.Errorf("coordinator returned HTTP %d", response.StatusCode)
	}
	if target == nil || len(encodedResponse) == 0 {
		return nil
	}
	return json.Unmarshal(encodedResponse, target)
}

func nodePreparationCoordinatorProblem(err error) error {
	if errors.Is(err, providerplane.ErrCoordinatorUnavailable) ||
		errors.Is(err, providerplane.ErrCredentialUnavailable) {
		return httpapi.CoordinatorUnavailable(
			"the coordinator must be reachable before provider writes can be sealed",
		)
	}
	if problem, ok := errors.AsType[*httpapi.ProblemError](err); ok {
		return problem
	}
	return httpapi.Internal("begin coordinator node preparation: " + err.Error())
}

func nodePreparationReceiptsDigest(
	receipts []db.NodePreparationReceipt,
) (string, error) {
	type semanticReceipt struct {
		StateKind          string `json:"state_kind"`
		SourceKey          string `json:"source_key"`
		ContentDigest      string `json:"content_digest"`
		CoordinatorReceipt string `json:"coordinator_receipt"`
	}
	semantic := make([]semanticReceipt, 0, len(receipts))
	for _, receipt := range receipts {
		semantic = append(semantic, semanticReceipt{
			StateKind: receipt.StateKind, SourceKey: receipt.SourceKey,
			ContentDigest:      receipt.ContentDigest,
			CoordinatorReceipt: receipt.CoordinatorReceipt,
		})
	}
	encoded, err := json.Marshal(semantic)
	if err != nil {
		return "", fmt.Errorf("encode node preparation receipts: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}
