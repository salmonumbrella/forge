package workspaceapi

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/forge/internal/db"
	"go.kenn.io/forge/internal/providerplane"
	"go.kenn.io/forge/internal/testutil/dbtest"
	"go.kenn.io/forge/internal/workspace"
)

func TestRefreshWorkspaceUsesCoordinatorProjectionWithoutLocalSyncer(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	database := dbtest.Open(t)
	issuedAt := time.Now().UTC().Truncate(time.Second)
	ws := &db.Workspace{
		ID: "ws-federated-refresh", Platform: "github", PlatformHost: "github.com",
		RepoOwner: "acme", RepoName: "widget",
		ItemType: db.WorkspaceItemTypePullRequest, ItemNumber: 7, ItemKey: "7",
		GitHeadRef: "feature/seven", WorkspaceBranch: "feature/seven",
		WorktreePath: filepath.Join(t.TempDir(), "worktree"), Status: "ready",
	}
	spec := workspaceLaunchSpecForRequest(providerplane.WorkspaceLaunchRequest{
		Repository: providerplane.RepositoryRoute{
			Provider: ws.Platform, PlatformHost: ws.PlatformHost,
			Owner: ws.RepoOwner, Name: ws.RepoName,
		},
		ItemType: ws.ItemType, ItemNumber: ws.ItemNumber,
		ItemKey: ws.ItemKey, GitHeadRef: ws.GitHeadRef,
	}, issuedAt)
	spec.SourceTitle = "Before refresh"
	require.NoError(database.CreateWorkspaceWithLaunchSpec(t.Context(), ws, spec))

	resolver := stubLaunchSpecResolver{refresh: func(
		_ context.Context, current db.WorkspaceLaunchSpec,
	) (db.WorkspaceLaunchSpec, error) {
		current.SourceTitle = "After refresh"
		current.IssuedAt = issuedAt.Add(time.Minute)
		current.SourceVisibleUntil = current.IssuedAt.Add(db.WorkspaceLaunchSpecVisibilityLease)
		return current, nil
	}}
	manager := workspace.NewManager(database, t.TempDir())
	handler := New(Deps{
		DB: database, Workspaces: manager, LaunchSpecResolver: resolver,
		Now: func() time.Time { return issuedAt.Add(time.Minute) },
	})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		require.NoError(handler.Shutdown(ctx))
	})

	output, err := handler.refreshWorkspace(
		t.Context(), &refreshWorkspaceInput{ID: ws.ID},
	)
	require.NoError(err)
	require.NotNil(output)
	refreshed, err := database.GetWorkspaceLaunchSpec(t.Context(), ws.ID)
	require.NoError(err)
	require.NotNil(refreshed)
	assert.Equal("After refresh", refreshed.SourceTitle)
}
