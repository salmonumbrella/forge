package db

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

type NodePreparationPhase string

const (
	NodePreparationOpen      NodePreparationPhase = "open"
	NodePreparationQuiescing NodePreparationPhase = "quiescing"
	NodePreparationSealed    NodePreparationPhase = "sealed"
)

var ErrNodePreparationConflict = errors.New("node preparation conflicts with durable state")

type NodePreparationBinding struct {
	EnrollmentID      string `json:"enrollment_id"`
	CoordinatorNodeID string `json:"coordinator_node_id"`
	LocalNodeID       string `json:"local_node_id"`
	ProtocolVersion   int    `json:"protocol_version"`
}

type NodePreparationState struct {
	Phase NodePreparationPhase `json:"phase"`
	NodePreparationBinding
	MigrationVersion   int        `json:"migration_version"`
	AckGeneration      int64      `json:"ack_generation"`
	DrainAckGeneration *int64     `json:"drain_ack_generation,omitempty"`
	PreparationDigest  string     `json:"preparation_digest,omitempty"`
	PreparationSeal    string     `json:"preparation_seal,omitempty"`
	StartedAt          *time.Time `json:"started_at,omitempty"`
	SealedAt           *time.Time `json:"sealed_at,omitempty"`
	UpdatedAt          time.Time  `json:"updated_at"`
}

type NodePreparationReceipt struct {
	StateKind          string    `json:"state_kind"`
	SourceKey          string    `json:"source_key"`
	ContentDigest      string    `json:"content_digest"`
	CoordinatorReceipt string    `json:"coordinator_receipt"`
	ImportedAt         time.Time `json:"imported_at"`
}

type NodePreparationSealRequest struct {
	EnrollmentID         string `json:"enrollment_id"`
	NodeID               string `json:"node_id"`
	CoordinatorNodeID    string `json:"coordinator_node_id"`
	ProtocolVersion      int    `json:"protocol_version"`
	MigrationVersion     int    `json:"migration_version"`
	ReceiptsDigest       string `json:"receipts_digest"`
	DrainedAckGeneration int64  `json:"drained_ack_generation"`
	PreparationDigest    string `json:"preparation_digest"`
}

type NodePreparationSeal struct {
	NodePreparationSealRequest
	Seal      string    `json:"preparation_seal"`
	CreatedAt time.Time `json:"created_at"`
}

func validateNodePreparationBinding(binding NodePreparationBinding) error {
	if strings.TrimSpace(binding.EnrollmentID) == "" ||
		strings.TrimSpace(binding.CoordinatorNodeID) == "" ||
		strings.TrimSpace(binding.LocalNodeID) == "" {
		return errors.New("node preparation identities are required")
	}
	if binding.ProtocolVersion <= 0 {
		return errors.New("node preparation protocol version must be positive")
	}
	return nil
}

func (d *DB) GetNodePreparation(
	ctx context.Context,
) (NodePreparationState, error) {
	var state NodePreparationState
	var phase string
	var drain sql.NullInt64
	var started, sealed sql.NullString
	var updated string
	err := d.ro.QueryRowContext(ctx, `
		SELECT phase, enrollment_id, coordinator_node_id, local_node_id,
		       protocol_version, migration_version, ack_generation,
		       drain_ack_generation, preparation_digest, preparation_seal,
		       started_at, sealed_at, updated_at
		FROM forge_node_preparation
		WHERE singleton_id = 1`).Scan(
		&phase, &state.EnrollmentID, &state.CoordinatorNodeID,
		&state.LocalNodeID, &state.ProtocolVersion, &state.MigrationVersion,
		&state.AckGeneration, &drain, &state.PreparationDigest,
		&state.PreparationSeal, &started, &sealed, &updated,
	)
	if err != nil {
		return NodePreparationState{}, fmt.Errorf("read node preparation: %w", err)
	}
	state.Phase = NodePreparationPhase(phase)
	if drain.Valid {
		value := drain.Int64
		state.DrainAckGeneration = &value
	}
	parseOptional := func(name string, raw sql.NullString) (*time.Time, error) {
		if !raw.Valid || raw.String == "" {
			return nil, nil
		}
		value, err := parseDBTime(raw.String)
		if err != nil {
			return nil, fmt.Errorf("parse node preparation %s: %w", name, err)
		}
		value = value.UTC()
		return &value, nil
	}
	state.StartedAt, err = parseOptional("started_at", started)
	if err != nil {
		return NodePreparationState{}, err
	}
	state.SealedAt, err = parseOptional("sealed_at", sealed)
	if err != nil {
		return NodePreparationState{}, err
	}
	state.UpdatedAt, err = parseDBTime(updated)
	if err != nil {
		return NodePreparationState{}, fmt.Errorf("parse node preparation updated_at: %w", err)
	}
	state.UpdatedAt = state.UpdatedAt.UTC()
	return state, nil
}

func (d *DB) BeginNodePreparation(
	ctx context.Context,
	binding NodePreparationBinding,
) (NodePreparationState, error) {
	if err := validateNodePreparationBinding(binding); err != nil {
		return NodePreparationState{}, err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	err := d.Tx(ctx, func(tx *sql.Tx) error {
		var phase, enrollmentID, coordinatorID, localID string
		var protocolVersion int
		if err := tx.QueryRowContext(ctx, `
			SELECT phase, enrollment_id, coordinator_node_id, local_node_id,
			       protocol_version
			FROM forge_node_preparation WHERE singleton_id = 1`).Scan(
			&phase, &enrollmentID, &coordinatorID, &localID, &protocolVersion,
		); err != nil {
			return err
		}
		if phase != string(NodePreparationOpen) {
			if enrollmentID != binding.EnrollmentID ||
				coordinatorID != binding.CoordinatorNodeID ||
				localID != binding.LocalNodeID ||
				protocolVersion != binding.ProtocolVersion {
				return ErrNodePreparationConflict
			}
			return nil
		}
		_, err := tx.ExecContext(ctx, `
			UPDATE forge_node_preparation
			SET phase = 'quiescing', enrollment_id = ?,
			    coordinator_node_id = ?, local_node_id = ?,
			    protocol_version = ?, drain_ack_generation = NULL,
			    preparation_digest = '', preparation_seal = '',
			    started_at = ?, sealed_at = NULL, updated_at = ?
			WHERE singleton_id = 1`,
			binding.EnrollmentID, binding.CoordinatorNodeID,
			binding.LocalNodeID, binding.ProtocolVersion, now, now,
		)
		return err
	})
	if err != nil {
		return NodePreparationState{}, fmt.Errorf("begin node preparation: %w", err)
	}
	return d.GetNodePreparation(ctx)
}

// AbortNodePreparation restores standalone provider writes after an
// enrollment is abandoned. It is idempotent and clears only local handoff
// receipts and the singleton binding; coordinator-issued audit seals remain.
func (d *DB) AbortNodePreparation(ctx context.Context) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	err := d.Tx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `DELETE FROM forge_node_preparation_receipts`); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `
			UPDATE forge_node_preparation
			SET phase = 'open', enrollment_id = '', coordinator_node_id = '',
			    local_node_id = '', protocol_version = 0,
			    drain_ack_generation = NULL, preparation_digest = '',
			    preparation_seal = '', started_at = NULL, sealed_at = NULL,
			    updated_at = ?
			WHERE singleton_id = 1`, now)
		return err
	})
	if err != nil {
		return fmt.Errorf("abort node preparation: %w", err)
	}
	return nil
}

func (d *DB) FreezeNodePreparationAckGeneration(
	ctx context.Context,
) (int64, error) {
	_, err := d.execContext(ctx, `
		UPDATE forge_node_preparation
		SET drain_ack_generation = ack_generation,
		    updated_at = ?
		WHERE singleton_id = 1
		  AND phase = 'quiescing'
		  AND drain_ack_generation IS NULL`,
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return 0, fmt.Errorf("freeze notification acknowledgement generation: %w", err)
	}
	state, err := d.GetNodePreparation(ctx)
	if err != nil {
		return 0, err
	}
	if state.Phase == NodePreparationOpen || state.DrainAckGeneration == nil {
		return 0, errors.New("node preparation acknowledgement generation is not frozen")
	}
	return *state.DrainAckGeneration, nil
}

func (d *DB) CountUndrainedNotificationAcks(
	ctx context.Context,
) (int, error) {
	var count int
	err := d.ro.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM forge_notification_ack_admissions admission
		JOIN forge_notification_items notification
		  ON notification.id = admission.notification_id
		JOIN forge_node_preparation preparation
		  ON preparation.singleton_id = 1
		WHERE notification.source_ack_queued_at IS NOT NULL
		  AND notification.source_ack_synced_at IS NULL
		  AND (
		      preparation.phase = 'open'
		      OR preparation.drain_ack_generation IS NULL
		      OR admission.generation <= preparation.drain_ack_generation
		  )`).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count undrained notification acknowledgements: %w", err)
	}
	return count, nil
}

func (d *DB) RecordNodePreparationReceipt(
	ctx context.Context,
	receipt NodePreparationReceipt,
) error {
	if receipt.StateKind != "review_draft" && receipt.StateKind != "workflow_state" {
		return errors.New("node preparation receipt state kind is invalid")
	}
	if strings.TrimSpace(receipt.SourceKey) == "" ||
		strings.TrimSpace(receipt.ContentDigest) == "" ||
		strings.TrimSpace(receipt.CoordinatorReceipt) == "" {
		return errors.New("node preparation receipt fields are required")
	}
	if receipt.ImportedAt.IsZero() {
		receipt.ImportedAt = time.Now().UTC()
	}
	result, err := d.rw.ExecContext(ctx, `
		INSERT INTO forge_node_preparation_receipts (
			state_kind, source_key, content_digest,
			coordinator_receipt, imported_at
		) VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(state_kind, source_key) DO UPDATE SET
			content_digest = excluded.content_digest,
			coordinator_receipt = excluded.coordinator_receipt,
			imported_at = excluded.imported_at
		WHERE forge_node_preparation_receipts.content_digest = excluded.content_digest
		  AND forge_node_preparation_receipts.coordinator_receipt = excluded.coordinator_receipt`,
		receipt.StateKind, receipt.SourceKey, receipt.ContentDigest,
		receipt.CoordinatorReceipt, receipt.ImportedAt.UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return fmt.Errorf("record node preparation receipt: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("record node preparation receipt result: %w", err)
	}
	if affected == 0 {
		return ErrNodePreparationConflict
	}
	return nil
}

func (d *DB) ListNodePreparationReceipts(
	ctx context.Context,
) ([]NodePreparationReceipt, error) {
	rows, err := d.ro.QueryContext(ctx, `
		SELECT state_kind, source_key, content_digest,
		       coordinator_receipt, imported_at
		FROM forge_node_preparation_receipts
		ORDER BY state_kind, source_key`)
	if err != nil {
		return nil, fmt.Errorf("list node preparation receipts: %w", err)
	}
	defer rows.Close()
	var receipts []NodePreparationReceipt
	for rows.Next() {
		var receipt NodePreparationReceipt
		var importedAt string
		if err := rows.Scan(
			&receipt.StateKind, &receipt.SourceKey, &receipt.ContentDigest,
			&receipt.CoordinatorReceipt, &importedAt,
		); err != nil {
			return nil, err
		}
		receipt.ImportedAt, err = parseDBTime(importedAt)
		if err != nil {
			return nil, fmt.Errorf("parse node preparation receipt time: %w", err)
		}
		receipt.ImportedAt = receipt.ImportedAt.UTC()
		receipts = append(receipts, receipt)
	}
	return receipts, rows.Err()
}

func validateNodePreparationSealRequest(request NodePreparationSealRequest) error {
	if strings.TrimSpace(request.EnrollmentID) == "" ||
		strings.TrimSpace(request.NodeID) == "" ||
		strings.TrimSpace(request.CoordinatorNodeID) == "" ||
		strings.TrimSpace(request.ReceiptsDigest) == "" ||
		strings.TrimSpace(request.PreparationDigest) == "" {
		return errors.New("node preparation seal binding is incomplete")
	}
	if request.ProtocolVersion <= 0 ||
		request.MigrationVersion != WorkspaceLaunchSpecMigrationVersion ||
		request.DrainedAckGeneration < 0 {
		return errors.New("node preparation seal version or generation is invalid")
	}
	expected, err := NodePreparationSealDigest(request)
	if err != nil {
		return err
	}
	if request.PreparationDigest != expected {
		return fmt.Errorf("%w: node preparation digest does not cover the seal binding", ErrNodePreparationConflict)
	}
	return nil
}

// NodePreparationSealDigest binds a seal to every semantic preparation field.
func NodePreparationSealDigest(request NodePreparationSealRequest) (string, error) {
	request.PreparationDigest = ""
	encoded, err := json.Marshal(request)
	if err != nil {
		return "", fmt.Errorf("encode node preparation seal binding: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

// IssueNodePreparationSeal stores one coordinator-owned, retry-safe seal.
func (d *DB) IssueNodePreparationSeal(
	ctx context.Context,
	request NodePreparationSealRequest,
) (NodePreparationSeal, error) {
	if err := validateNodePreparationSealRequest(request); err != nil {
		return NodePreparationSeal{}, err
	}
	existing, err := d.GetNodePreparationSeal(ctx, request.EnrollmentID)
	if err != nil {
		return NodePreparationSeal{}, err
	}
	if existing != nil {
		if existing.NodePreparationSealRequest != request {
			return NodePreparationSeal{}, ErrNodePreparationConflict
		}
		return *existing, nil
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return NodePreparationSeal{}, fmt.Errorf("generate node preparation seal: %w", err)
	}
	seal := NodePreparationSeal{
		NodePreparationSealRequest: request,
		Seal:                       hex.EncodeToString(raw), CreatedAt: time.Now().UTC(),
	}
	_, err = d.rw.ExecContext(ctx, `
		INSERT INTO forge_node_preparation_seals (
			enrollment_id, node_id, coordinator_node_id,
			protocol_version, migration_version, receipts_digest,
			drained_ack_generation, preparation_digest,
			preparation_seal, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		request.EnrollmentID, request.NodeID, request.CoordinatorNodeID,
		request.ProtocolVersion, request.MigrationVersion,
		request.ReceiptsDigest, request.DrainedAckGeneration,
		request.PreparationDigest, seal.Seal,
		seal.CreatedAt.Format(time.RFC3339Nano),
	)
	if err != nil {
		// A concurrent identical request may have won the insert.
		existing, readErr := d.GetNodePreparationSeal(ctx, request.EnrollmentID)
		if readErr == nil && existing != nil && existing.NodePreparationSealRequest == request {
			return *existing, nil
		}
		return NodePreparationSeal{}, fmt.Errorf("issue node preparation seal: %w", err)
	}
	return seal, nil
}

func (d *DB) GetNodePreparationSeal(
	ctx context.Context,
	enrollmentID string,
) (*NodePreparationSeal, error) {
	var seal NodePreparationSeal
	var createdAt string
	err := d.ro.QueryRowContext(ctx, `
		SELECT enrollment_id, node_id, coordinator_node_id,
		       protocol_version, migration_version, receipts_digest,
		       drained_ack_generation, preparation_digest,
		       preparation_seal, created_at
		FROM forge_node_preparation_seals
		WHERE enrollment_id = ?`, enrollmentID,
	).Scan(
		&seal.EnrollmentID, &seal.NodeID, &seal.CoordinatorNodeID,
		&seal.ProtocolVersion, &seal.MigrationVersion, &seal.ReceiptsDigest,
		&seal.DrainedAckGeneration, &seal.PreparationDigest,
		&seal.Seal, &createdAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read node preparation seal: %w", err)
	}
	seal.CreatedAt, err = parseDBTime(createdAt)
	if err != nil {
		return nil, fmt.Errorf("parse node preparation seal time: %w", err)
	}
	seal.CreatedAt = seal.CreatedAt.UTC()
	return &seal, nil
}

func (d *DB) StoreLocalNodePreparationSeal(
	ctx context.Context,
	digest string,
	seal string,
) error {
	if strings.TrimSpace(digest) == "" || strings.TrimSpace(seal) == "" {
		return errors.New("node preparation digest and seal are required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	result, err := d.rw.ExecContext(ctx, `
		UPDATE forge_node_preparation
		SET phase = 'sealed', preparation_digest = ?, preparation_seal = ?,
		    sealed_at = COALESCE(sealed_at, ?), updated_at = ?
		WHERE singleton_id = 1
		  AND phase IN ('quiescing', 'sealed')
		  AND (preparation_digest = '' OR preparation_digest = ?)
		  AND (preparation_seal = '' OR preparation_seal = ?)`,
		digest, seal, now, now, digest, seal,
	)
	if err != nil {
		return fmt.Errorf("store local node preparation seal: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return ErrNodePreparationConflict
	}
	return nil
}
