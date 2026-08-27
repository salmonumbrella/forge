package fleetsetup

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"os/user"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.kenn.io/forge/internal/config"
)

const testTailscaleStatus = `{
  "BackendState":"Running",
  "Self":{"DNSName":"forge-node.example.ts.net.","UserID":42},
  "User":{"42":{"LoginName":"Operator@Example.com"}},
  "CertDomains":["forge-node.example.ts.net"]
}`

type testLock struct{}

func (testLock) Release() error { return nil }

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

func testRunner(t *testing.T, goos string) (*Runner, string, *[]string) {
	t.Helper()
	root := t.TempDir()
	binary := filepath.Join(root, "kenn-forge")
	require.NoError(t, os.WriteFile(binary, []byte("test"), 0o700))
	commands := []string{}
	deps := defaultDependencies()
	deps.goos = goos
	deps.currentUser = func() (*user.User, error) {
		return &user.User{Username: "operator", Uid: "1000", HomeDir: root}, nil
	}
	deps.executable = func() (string, error) { return binary, nil }
	deps.lookupPath = func(name string) (string, error) { return "/usr/bin/" + name, nil }
	deps.checkPortOwner = func(context.Context, string, int, string) error { return nil }
	deps.run = func(_ context.Context, name string, args ...string) (commandResult, error) {
		commands = append(commands, strings.Join(append([]string{name}, args...), " "))
		switch {
		case name == "tailscale" && slices.Equal(args, []string{"status", "--json"}):
			return commandResult{stdout: []byte(testTailscaleStatus)}, nil
		case name == "tailscale" && slices.Equal(args, []string{"serve", "status", "--json"}):
			return commandResult{stdout: []byte(`{"TCP":{},"Web":{}}`)}, nil
		case name == "gh":
			return commandResult{stdout: []byte("credential\n")}, nil
		case name == "loginctl" && len(args) > 0 && args[0] == "show-user":
			return commandResult{stdout: []byte("yes\n")}, nil
		default:
			return commandResult{}, nil
		}
	}
	deps.acquireConfigLock = func(context.Context, string) (setupLock, error) {
		return testLock{}, nil
	}
	deps.ensureNodeID = func(string) (string, error) {
		return "11111111111111111111111111111111", nil
	}
	deps.readinessTimeout = 20 * time.Millisecond
	return &Runner{deps: deps}, root, &commands
}

func TestPlanDiscoversTailscaleIdentityWithoutMutatingTarget(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	runner, root, _ := testRunner(t, "linux")
	configPath := filepath.Join(root, "forge", "config.toml")

	plan, err := runner.Plan(t.Context(), Options{
		Role: RoleCoordinator, ConfigPath: configPath, Tailscale: true,
	})

	require.NoError(err)
	assert.Equal(RoleCoordinator, plan.Role)
	assert.Equal("operator", plan.User)
	assert.Equal("forge-node.example.ts.net", plan.TailscaleDNS)
	assert.Equal("operator@example.com", plan.TailscaleLogin)
	assert.Equal("https://forge-node.example.ts.net", plan.Origin)
	assert.Equal(8091, plan.Port)
	assert.Equal(filepath.Join(root, ".config/systemd/user/kenn-forge.service"), plan.ServicePath)
	_, statErr := os.Stat(configPath)
	require.ErrorIs(statErr, os.ErrNotExist)
	_, statErr = os.Stat(plan.ServicePath)
	require.ErrorIs(statErr, os.ErrNotExist)
}

func TestResolveTailscaleCommandFindsTheMacAppCLI(t *testing.T) {
	require := require.New(t)
	runner, root, _ := testRunner(t, "darwin")
	appCLI := filepath.Join(root, "Applications", "Tailscale")
	require.NoError(os.MkdirAll(filepath.Dir(appCLI), 0o700))
	require.NoError(os.WriteFile(appCLI, []byte("test"), 0o700))
	runner.deps.tailscaleAppPath = appCLI
	runner.deps.lookupPath = func(name string) (string, error) {
		if name == "tailscale" {
			return "", errors.New("not on PATH")
		}
		return "/usr/bin/" + name, nil
	}

	command, err := runner.resolveTailscaleCommand()

	require.NoError(err)
	assert.Equal(t, appCLI, command)
}

func TestPlanRejectsConflictingTailscaleServeRoot(t *testing.T) {
	runner, root, _ := testRunner(t, "linux")
	runner.deps.run = func(_ context.Context, name string, args ...string) (commandResult, error) {
		switch {
		case name == "tailscale" && slices.Equal(args, []string{"status", "--json"}):
			return commandResult{stdout: []byte(testTailscaleStatus)}, nil
		case name == "tailscale" && slices.Equal(args, []string{"serve", "status", "--json"}):
			return commandResult{stdout: []byte(`{
          "Web":{"forge-node.example.ts.net:443":{"Handlers":{"/":{"Proxy":"http://127.0.0.1:9000"}}}}
        }`)}, nil
		case name == "gh":
			return commandResult{stdout: []byte("credential")}, nil
		case name == "loginctl" && len(args) > 0 && args[0] == "show-user":
			return commandResult{stdout: []byte("yes\n")}, nil
		default:
			return commandResult{}, nil
		}
	}

	_, err := runner.Plan(t.Context(), Options{
		Role: RoleNode, ConfigPath: filepath.Join(root, "config.toml"), Tailscale: true,
	})

	require.ErrorContains(t, err, "already owns")
}

func TestConfigureNodePreservesEnrollmentOwnedRole(t *testing.T) {
	candidate := &config.Config{
		Fleet: config.Fleet{
			Enabled: true, Role: config.FleetRoleNode,
			BaseURL: "https://old.example.ts.net",
			Coordinator: &config.FleetCoordinator{
				NodeID:  "22222222222222222222222222222222",
				BaseURL: "https://coordinator.example.ts.net",
			},
		},
	}
	plan := Plan{
		Role: RoleNode, Host: "127.0.0.1", Port: 8091,
		DataDir: "/tmp/data", TailscaleDNS: "node.example.ts.net",
		TailscaleLogin: "operator@example.com", Origin: "https://node.example.ts.net",
		AllowedHost: "node.example.ts.net", Publication: publicationTailscale,
	}

	require.NoError(t, configureCandidate(candidate, plan))
	assert.Equal(t, config.FleetRoleNode, candidate.Fleet.Role)
	assert.NotNil(t, candidate.Fleet.Coordinator)
}

func TestApplyRollsBackOwnedStateWhenReadinessFails(t *testing.T) {
	require := require.New(t)
	runner, root, commands := testRunner(t, "linux")
	runner.deps.httpClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusServiceUnavailable,
			Status:     "503 Service Unavailable",
			Body:       io.NopCloser(strings.NewReader("unavailable")),
			Header:     make(http.Header),
		}, nil
	})}
	plan := Plan{
		Role:       RoleCoordinator,
		ConfigPath: filepath.Join(root, "forge", "config.toml"),
		DataDir:    filepath.Join(root, "forge"), BinaryPath: filepath.Join(root, "kenn-forge"),
		User: "operator", UID: 1000, HomeDir: root, PathEnv: "/usr/bin",
		TailscaleLogin: "operator@example.com", TailscaleDNS: "forge-node.example.ts.net",
		Origin: "https://forge-node.example.ts.net", AllowedHost: "forge-node.example.ts.net",
		Publication: publicationTailscale, Host: "127.0.0.1", Port: 8091,
		ServicePath: filepath.Join(root, ".config/systemd/user/kenn-forge.service"),
		ServiceKind: "systemd user service", ServiceLabel: "kenn-forge.service",
	}

	_, err := runner.Apply(t.Context(), plan)

	require.ErrorContains(err, "readiness")
	_, statErr := os.Stat(plan.ConfigPath)
	require.ErrorIs(statErr, os.ErrNotExist)
	_, statErr = os.Stat(plan.ServicePath)
	require.ErrorIs(statErr, os.ErrNotExist)
	assert.Contains(t, *commands, "tailscale serve --yes --https=443 --set-path=/ off")
}

func TestApplyPublishesConfigServiceAndCanonicalIdentity(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	runner, root, commands := testRunner(t, "linux")
	var snapshotRequestURI string
	runner.deps.httpClient = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		body := `{"status":"ok"}`
		if request.URL.Path == "/api/v1/snapshot" {
			snapshotRequestURI = request.URL.RequestURI()
			body = `{"hosts":[{"nodeID":"11111111111111111111111111111111","kind":"self","federationRole":"coordinator"}]}`
		}
		return &http.Response{
			StatusCode: http.StatusOK, Status: "200 OK",
			Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header),
		}, nil
	})}
	plan := Plan{
		Role:       RoleCoordinator,
		ConfigPath: filepath.Join(root, "forge", "config.toml"),
		DataDir:    filepath.Join(root, "forge"), BinaryPath: filepath.Join(root, "kenn-forge"),
		User: "operator", UID: 1000, HomeDir: root, PathEnv: "/usr/bin",
		TailscaleLogin: "operator@example.com", TailscaleDNS: "forge-node.example.ts.net",
		Origin: "https://forge-node.example.ts.net", AllowedHost: "forge-node.example.ts.net",
		Publication: publicationTailscale, Host: "127.0.0.1", Port: 8091,
		ServicePath: filepath.Join(root, ".config/systemd/user/kenn-forge.service"),
		ServiceKind: "systemd user service", ServiceLabel: "kenn-forge.service",
	}

	result, err := runner.Apply(t.Context(), plan)

	require.NoError(err)
	assert.Equal(plan.Origin, result.Origin)
	assert.Equal("11111111111111111111111111111111", result.NodeID)
	assert.Equal("/api/v1/snapshot?include_peers=true", snapshotRequestURI)
	cfg, err := config.Load(plan.ConfigPath)
	require.NoError(err)
	assert.Equal([]string{"forge-node.example.ts.net"}, cfg.AllowedHosts)
	assert.True(cfg.API.RequireAuth)
	assert.Equal([]string{"operator@example.com"}, cfg.API.TailscaleServe.AllowedUsers)
	assert.Equal(plan.Origin, cfg.Fleet.BaseURL)
	assert.FileExists(plan.ServicePath)
	assert.Contains(*commands, "tailscale serve --yes --bg --https=443 http://127.0.0.1:8091")
	assert.NotContains(*commands, "tailscale serve --yes --https=443 --set-path=/ off")
}

func TestPlanSupportsExternalHTTPSWithoutTailscale(t *testing.T) {
	assert := assert.New(t)
	runner, root, commands := testRunner(t, "linux")

	plan, err := runner.Plan(t.Context(), Options{
		Role: RoleCoordinator, ConfigPath: filepath.Join(root, "config.toml"),
		Origin: "https://forge.internal.example:8443",
	})

	require.NoError(t, err)
	assert.Equal(publicationExternal, plan.Publication)
	assert.Equal("https://forge.internal.example:8443", plan.Origin)
	assert.Equal("forge.internal.example:8443", plan.AllowedHost)
	for _, command := range *commands {
		assert.NotEqual("tailscale", strings.Fields(command)[0])
	}
}

func TestPlanRequiresExactlyOnePublicationMode(t *testing.T) {
	runner, root, _ := testRunner(t, "linux")
	configPath := filepath.Join(root, "config.toml")

	_, neitherErr := runner.Plan(t.Context(), Options{
		Role: RoleCoordinator, ConfigPath: configPath,
	})
	_, bothErr := runner.Plan(t.Context(), Options{
		Role: RoleCoordinator, ConfigPath: configPath,
		Tailscale: true, Origin: "https://forge.internal.example",
	})

	require.ErrorContains(t, neitherErr, "choose exactly one publication mode")
	require.ErrorContains(t, bothErr, "choose exactly one publication mode")
}

func TestPlanEnablesLingerOnlyWhenNeeded(t *testing.T) {
	runner, root, _ := testRunner(t, "linux")
	runner.deps.run = func(_ context.Context, name string, args ...string) (commandResult, error) {
		switch name {
		case "gh":
			return commandResult{stdout: []byte("credential\n")}, nil
		case "loginctl":
			return commandResult{stdout: []byte("no\n")}, nil
		default:
			return commandResult{}, nil
		}
	}

	plan, err := runner.Plan(t.Context(), Options{
		Role: RoleCoordinator, ConfigPath: filepath.Join(root, "config.toml"),
		Origin: "https://forge.internal.example",
	})

	require.NoError(t, err)
	assert.True(t, plan.EnableLinger)
}

func TestExternalPublicationUsesDaemonBearerAndDisablesTailscaleIdentity(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	runner, root, commands := testRunner(t, "linux")
	dataDir := filepath.Join(root, "forge")
	require.NoError(os.MkdirAll(dataDir, 0o700))
	require.NoError(os.WriteFile(filepath.Join(dataDir, "auth_token"), []byte("daemon-secret\n"), 0o600))
	var snapshotAuthorization string
	runner.deps.httpClient = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		body := `{"status":"ok"}`
		if request.URL.Path == "/api/v1/snapshot" {
			snapshotAuthorization = request.Header.Get("Authorization")
			body = `{"hosts":[{"nodeID":"11111111111111111111111111111111","kind":"self","federationRole":"coordinator"}]}`
		}
		return &http.Response{
			StatusCode: http.StatusOK, Status: "200 OK",
			Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header),
		}, nil
	})}
	plan := Plan{
		Role:       RoleCoordinator,
		ConfigPath: filepath.Join(root, "config.toml"), DataDir: dataDir,
		BinaryPath: filepath.Join(root, "kenn-forge"), User: "operator", UID: 1000,
		HomeDir: root, PathEnv: "/usr/bin", Origin: "https://forge.internal.example:8443",
		AllowedHost: "forge.internal.example:8443", Publication: publicationExternal,
		Host: "127.0.0.1", Port: 8091,
		ServicePath: filepath.Join(root, ".config/systemd/user/kenn-forge.service"),
		ServiceKind: "systemd user service", ServiceLabel: "kenn-forge.service",
	}

	_, err := runner.Apply(t.Context(), plan)

	require.NoError(err)
	assert.Equal("Bearer daemon-secret", snapshotAuthorization)
	cfg, err := config.Load(plan.ConfigPath)
	require.NoError(err)
	assert.False(cfg.API.TailscaleServe.Enabled)
	assert.Empty(cfg.API.TailscaleServe.AllowedUsers)
	for _, command := range *commands {
		assert.NotEqual("tailscale", strings.Fields(command)[0])
	}
}

func TestCheckLocalPortOwnerAuthenticatesTheExistingForge(t *testing.T) {
	const token = "local-daemon-secret"
	server := http.Server{Handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer "+token {
			writer.WriteHeader(http.StatusUnauthorized)
			return
		}
		_, _ = io.WriteString(writer, `{"ok":true,"service":"kenn-forge"}`)
	})}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Close()) })
	go func() { _ = server.Serve(listener) }()
	port := listener.Addr().(*net.TCPAddr).Port
	dataDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dataDir, "auth_token"), []byte(token), 0o600))

	require.NoError(t, checkLocalPortOwner(t.Context(), "127.0.0.1", port, dataDir))
}

func TestTransactionRollsBackInReverseOrder(t *testing.T) {
	var order []string
	transaction := transaction{}
	transaction.record(func(context.Context) error { order = append(order, "first"); return nil })
	transaction.record(func(context.Context) error { order = append(order, "second"); return nil })

	require.NoError(t, transaction.rollback(t.Context()))
	assert.Equal(t, []string{"second", "first"}, order)
}

func TestApplyLingerRecordsItsInverse(t *testing.T) {
	runner, _, commands := testRunner(t, "linux")
	transaction := transaction{}

	require.NoError(t, runner.applyLinger(t.Context(), Plan{User: "operator"}, &transaction))
	require.NoError(t, transaction.rollback(t.Context()))

	assert.Equal(t, []string{
		"loginctl enable-linger operator",
		"loginctl disable-linger operator",
	}, *commands)
}

func TestApplyReturnsRollbackFailure(t *testing.T) {
	transaction := transaction{}
	transaction.record(func(context.Context) error { return errors.New("undo failed") })
	assert.ErrorContains(t, transaction.rollback(t.Context()), "undo failed")
}
