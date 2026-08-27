package providerplane

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.kenn.io/forge/internal/federationauth"
)

type stubClient func(context.Context, federationauth.Scope, *http.Request) (*http.Response, error)

func (f stubClient) Do(
	ctx context.Context, scope federationauth.Scope, request *http.Request,
) (*http.Response, error) {
	return f(ctx, scope, request)
}

type repeatingReader byte

func (r repeatingReader) Read(buffer []byte) (int, error) {
	for i := range buffer {
		buffer[i] = byte(r)
	}
	return len(buffer), nil
}

const (
	clientTestNodeID        = "11111111111111111111111111111111"
	clientTestCoordinatorID = "22222222222222222222222222222222"
)

func TestClientUsesOnlyCoordinatorCredential(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	t.Parallel()

	var got http.Header
	var gotPath string
	var gotEscapedPath string
	coordinator := httptest.NewTLSServer(http.HandlerFunc(func(
		w http.ResponseWriter, r *http.Request,
	) {
		got = r.Header.Clone()
		gotPath = r.URL.Path
		gotEscapedPath = r.URL.EscapedPath()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{}`)
	}))
	t.Cleanup(coordinator.Close)

	credentials, err := federationauth.Open(t.TempDir() + "/credentials.json")
	require.NoError(err)
	require.NoError(credentials.StoreOutbound(
		clientTestCoordinatorID,
		"coordinator-only-secret",
		federationauth.NodeToCoordinatorScopes(),
	))

	client, err := NewClient(Options{
		LocalNodeID: clientTestNodeID,
		Coordinator: Coordinator{
			NodeID:  clientTestCoordinatorID,
			BaseURL: coordinator.URL,
		},
		Credentials: credentials,
		HTTPClient:  coordinator.Client(),
	})
	require.NoError(err)

	request, err := http.NewRequest(
		http.MethodGet,
		"https://node.invalid/api/v1/repos/acme%2Fwidgets/pulls?q=open",
		nil,
	)
	require.NoError(err)
	request.Header.Set("Authorization", "Bearer browser-secret")
	request.Header.Set("Cookie", "forge_auth=browser-secret")
	request.Header.Set("Origin", "https://browser.invalid")

	response, err := client.Do(
		context.Background(), federationauth.ScopeProviderRead, request,
	)
	require.NoError(err)
	require.NoError(response.Body.Close())

	assert.Equal("Bearer coordinator-only-secret", got.Get("Authorization"))
	assert.Equal(clientTestNodeID, got.Get(federationauth.NodeIDHeader))
	assert.Equal(ProtocolVersionHeaderValue(), got.Get(ProtocolVersionHeader))
	assert.Empty(got.Get("Cookie"))
	assert.Empty(got.Get("Origin"))
	assert.Equal("/api/v1/repos/acme/widgets/pulls", gotPath)
	assert.Equal("/api/v1/repos/acme%2Fwidgets/pulls", gotEscapedPath)
}

func TestClientRejectsScopeOutsideProviderPlane(t *testing.T) {
	t.Parallel()

	client, err := NewClient(Options{
		LocalNodeID: clientTestNodeID,
		Coordinator: Coordinator{
			NodeID:  clientTestCoordinatorID,
			BaseURL: "https://coordinator.example",
		},
	})
	require.NoError(t, err)
	request := httptest.NewRequest(http.MethodGet, "/api/v1/workspaces", nil)

	_, err = client.Do(
		context.Background(), federationauth.ScopeWorkspaceRead, request,
	)
	assert.ErrorIs(t, err, ErrInvalidScope)
}

func TestClientBoundsOrdinaryRequestsWithoutTimingOutEventStreams(t *testing.T) {
	t.Parallel()

	base := &http.Client{Timeout: 125 * time.Millisecond}
	client, err := NewClient(Options{
		LocalNodeID: clientTestNodeID,
		Coordinator: Coordinator{
			NodeID:  clientTestCoordinatorID,
			BaseURL: "https://coordinator.example",
		},
		HTTPClient: base,
	})
	require.NoError(t, err)

	coordinator := client.(*coordinatorClient)
	assert.Equal(t, base.Timeout, coordinator.httpClient.Timeout)
	assert.Zero(t, coordinator.streamClient.Timeout)
}

func TestClientRefusesCoordinatorRedirect(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	t.Parallel()

	var redirectedRequests atomic.Int64
	destination := httptest.NewTLSServer(http.HandlerFunc(func(
		w http.ResponseWriter, _ *http.Request,
	) {
		redirectedRequests.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(destination.Close)
	coordinator := httptest.NewTLSServer(http.HandlerFunc(func(
		w http.ResponseWriter, _ *http.Request,
	) {
		w.Header().Set("Location", destination.URL+"/api/v1/pulls")
		w.WriteHeader(http.StatusFound)
	}))
	t.Cleanup(coordinator.Close)
	credentials, err := federationauth.Open(t.TempDir() + "/credentials.json")
	require.NoError(err)
	require.NoError(credentials.StoreOutbound(
		clientTestCoordinatorID, "redirect-secret",
		federationauth.NodeToCoordinatorScopes(),
	))
	client, err := NewClient(Options{
		LocalNodeID: clientTestNodeID,
		Coordinator: Coordinator{
			NodeID: clientTestCoordinatorID, BaseURL: coordinator.URL,
		},
		Credentials: credentials,
		HTTPClient:  coordinator.Client(),
	})
	require.NoError(err)
	request := httptest.NewRequest(http.MethodGet, "/api/v1/pulls", nil)
	response, err := client.Do(
		context.Background(), federationauth.ScopeProviderRead, request,
	)
	require.NoError(err)
	defer response.Body.Close()
	assert.Equal(http.StatusFound, response.StatusCode)
	assert.Zero(redirectedRequests.Load())
}

func TestClientRejectsOversizedBodyBeforeNetwork(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	t.Parallel()

	var requests atomic.Int64
	coordinator := httptest.NewTLSServer(http.HandlerFunc(func(
		w http.ResponseWriter, _ *http.Request,
	) {
		requests.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(coordinator.Close)
	credentials, err := federationauth.Open(t.TempDir() + "/credentials.json")
	require.NoError(err)
	require.NoError(credentials.StoreOutbound(
		clientTestCoordinatorID, "body-secret",
		federationauth.NodeToCoordinatorScopes(),
	))
	client, err := NewClient(Options{
		LocalNodeID: clientTestNodeID,
		Coordinator: Coordinator{
			NodeID: clientTestCoordinatorID, BaseURL: coordinator.URL,
		},
		Credentials: credentials,
		HTTPClient:  coordinator.Client(), BodyLimit: 3,
	})
	require.NoError(err)
	request := httptest.NewRequest(
		http.MethodPost, "/api/v1/pulls", strings.NewReader("four"),
	)
	_, err = client.Do(
		context.Background(), federationauth.ScopeProviderWrite, request,
	)
	require.ErrorIs(err, ErrRequestBodyTooLarge)
	assert.Zero(requests.Load())
}

func TestReadJSONPreservesCoordinatorProblem(t *testing.T) {
	assert := assert.New(t)
	t.Parallel()

	client := stubClient(func(
		context.Context, federationauth.Scope, *http.Request,
	) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusConflict,
			Header:     http.Header{"Retry-After": []string{"3"}},
			Body: io.NopCloser(strings.NewReader(
				`{"code":"conflict","detail":"refresh first"}`,
			)),
		}, nil
	})
	request := httptest.NewRequest(http.MethodGet, "/api/v1/pulls", nil)

	err := ReadJSON(
		t.Context(), client, federationauth.ScopeProviderRead, request, &struct{}{},
	)
	var responseErr *ResponseError
	require.ErrorAs(t, err, &responseErr)
	assert.Equal(http.StatusConflict, responseErr.Status)
	assert.Equal("3", responseErr.Header.Get("Retry-After"))
	assert.JSONEq(`{"code":"conflict","detail":"refresh first"}`, string(responseErr.Body))
}

func TestReadJSONRejectsOversizedCoordinatorBody(t *testing.T) {
	t.Parallel()

	client := stubClient(func(
		context.Context, federationauth.Scope, *http.Request,
	) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body: io.NopCloser(io.LimitReader(
				repeatingReader('x'),
				defaultResponseBodyLimit+1,
			)),
		}, nil
	})
	request := httptest.NewRequest(http.MethodGet, "/api/v1/pulls", nil)

	err := ReadJSON(
		t.Context(), client, federationauth.ScopeProviderRead, request, &struct{}{},
	)
	assert.ErrorIs(t, err, ErrResponseBodyTooLarge)
}
