package db

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func nodePreparationBindingForTest() NodePreparationBinding {
	return NodePreparationBinding{
		EnrollmentID: "enrollment-1", CoordinatorNodeID: "coordinator-1",
		LocalNodeID: "node-1", ProtocolVersion: 3,
	}
}

func nodePreparationSealRequestForTest() NodePreparationSealRequest {
	request := NodePreparationSealRequest{
		EnrollmentID: "enrollment-1", NodeID: "node-1",
		CoordinatorNodeID: "coordinator-1", ProtocolVersion: 3,
		MigrationVersion: WorkspaceLaunchSpecMigrationVersion,
		ReceiptsDigest:   "receipts-digest", DrainedAckGeneration: 4,
	}
	digest, err := NodePreparationSealDigest(request)
	if err != nil {
		panic(err)
	}
	request.PreparationDigest = digest
	return request
}

func TestNodePreparationStateIsDurableBoundAndRetrySafe(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	path := filepath.Join(t.TempDir(), "node-preparation.db")
	database, err := Open(path)
	require.NoError(err)
	state, err := database.GetNodePreparation(t.Context())
	require.NoError(err)
	assert.Equal(NodePreparationOpen, state.Phase)

	binding := nodePreparationBindingForTest()
	started, err := database.BeginNodePreparation(t.Context(), binding)
	require.NoError(err)
	assert.Equal(NodePreparationQuiescing, started.Phase)
	assert.Equal(binding, started.NodePreparationBinding)
	retried, err := database.BeginNodePreparation(t.Context(), binding)
	require.NoError(err)
	assert.Equal(started.StartedAt, retried.StartedAt)

	different := binding
	different.CoordinatorNodeID = "other-coordinator"
	_, err = database.BeginNodePreparation(t.Context(), different)
	require.ErrorIs(err, ErrNodePreparationConflict)
	require.NoError(database.Close())

	database, err = Open(path)
	require.NoError(err)
	t.Cleanup(func() { require.NoError(database.Close()) })
	restored, err := database.GetNodePreparation(t.Context())
	require.NoError(err)
	assert.Equal(NodePreparationQuiescing, restored.Phase)
	assert.Equal(binding, restored.NodePreparationBinding)
}

func TestNodePreparationReceiptsAndSealsRejectSemanticChanges(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	database := openTestDB(t)
	now := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	receipt := NodePreparationReceipt{
		StateKind: ProviderStateReviewDraft, SourceKey: "repo/pr/7",
		ContentDigest: "content-digest", CoordinatorReceipt: "coordinator-receipt",
		ImportedAt: now,
	}
	require.NoError(database.RecordNodePreparationReceipt(t.Context(), receipt))
	require.NoError(database.RecordNodePreparationReceipt(t.Context(), receipt))
	changed := receipt
	changed.ContentDigest = "changed-digest"
	require.ErrorIs(database.RecordNodePreparationReceipt(t.Context(), changed), ErrNodePreparationConflict)
	receipts, err := database.ListNodePreparationReceipts(t.Context())
	require.NoError(err)
	assert.Equal([]NodePreparationReceipt{receipt}, receipts)

	request := nodePreparationSealRequestForTest()
	first, err := database.IssueNodePreparationSeal(t.Context(), request)
	require.NoError(err)
	assert.NotEmpty(first.Seal)
	retry, err := database.IssueNodePreparationSeal(t.Context(), request)
	require.NoError(err)
	assert.Equal(first, retry)
	different := request
	different.ReceiptsDigest = "changed-receipts"
	different.PreparationDigest, err = NodePreparationSealDigest(different)
	require.NoError(err)
	_, err = database.IssueNodePreparationSeal(t.Context(), different)
	require.ErrorIs(err, ErrNodePreparationConflict)
}

func TestNodePreparationSealRejectsDigestThatDoesNotCoverBinding(t *testing.T) {
	database := openTestDB(t)
	request := nodePreparationSealRequestForTest()
	request.ReceiptsDigest = "changed-after-digest"

	_, err := database.IssueNodePreparationSeal(t.Context(), request)
	require.ErrorIs(t, err, ErrNodePreparationConflict)
}

func TestNodePreparationLocalSealCannotBeRebound(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	database := openTestDB(t)
	_, err := database.BeginNodePreparation(t.Context(), nodePreparationBindingForTest())
	require.NoError(err)
	require.NoError(database.StoreLocalNodePreparationSeal(
		t.Context(), "preparation-digest", "opaque-seal",
	))
	require.NoError(database.StoreLocalNodePreparationSeal(
		t.Context(), "preparation-digest", "opaque-seal",
	))
	require.ErrorIs(database.StoreLocalNodePreparationSeal(
		t.Context(), "other-digest", "other-seal",
	), ErrNodePreparationConflict)
	state, err := database.GetNodePreparation(t.Context())
	require.NoError(err)
	assert.Equal(NodePreparationSealed, state.Phase)
	assert.Equal("preparation-digest", state.PreparationDigest)
	assert.Equal("opaque-seal", state.PreparationSeal)
}
