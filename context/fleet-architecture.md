# Fleet Architecture

Use this document for federation settings, snapshot aggregation, node routing,
or remote workspace and session operations.

## Ownership And Topology

- Every data directory has one random 128-bit lowercase-hex node ID; hostnames,
  listener addresses, and mutable display names never establish fleet identity
  (`internal/runtimelock/node_id.go::EnsureNodeID`).
- `fleet.role` defaults to `coordinator`. A `node` requires a validated
  coordinator binding; enabling federation requires API authentication and a canonical HTTPS `fleet.base_url`
  (`internal/config/config.go::Fleet.Validate`, `internal/config/config.go::Config.validate`).
- A fleet is a one-hop execution view over independent kenn-forge daemons, not
  a replicated database. Each host remains authoritative for its local
  repositories, workspaces, runtimes, and execution mutations.
- Federation requires mutually reachable canonical HTTPS origins, not a
  private-network product; Tailscale Serve and operator-managed private ingress
  use the same protocol and peer credentials
  (`internal/fleetsetup/setup.go::Runner.Plan`).
- Provider repository settings come from the coordinator, but each node owns
  and persists its stable-identity `worktree_base_path`; mutable or reused routes
  cannot move the override to another repository (`internal/config/config.go::Repo`,
  `internal/server/settings_handlers.go::Server.updateConfiguredRepoWorktreeBasePath`).
- Settings-carried repository observations use coordinator catalog timestamps;
  node clocks never advance route freshness (`internal/server/federation_provider_settings.go::Server.buildProviderSettingsProjection`).
- Raw snapshots contain only producer-local facts; they never contain fetched
  aggregates or observer permissions (`internal/server/fleetapi/fleet_adapter.go::Handler.buildLocalRaw`).
- Coordinator provider enrichment keys by stable repository identity and item
  number; local numeric repository IDs never cross the federation wire
  (`internal/fleet/provider_enrichment.go::EnrichProviderState`).
- Nodes replace their aggregate entries with fresh local authority. Raw endpoints
  never re-export aggregates, which keeps federation data flow acyclic
  (`internal/server/fleetapi/fleet_hub.go::Handler.buildFleetSnapshot`).
- A node may use coordinator routes only after its config, enrollment, and
  database seal match and remote identity, protocol, and activation validate.
  An active node booted disabled validates its sealed binding and repairs both
  active credential grants before building dormant clients without startup
  egress; it publishes an initial disconnected provider state (`cmd/kenn-forge/node_startup.go::activateFederationNodeAtStartup`,
  `internal/server/server.go::newServer`).
- Fleet consumes detached Workspace-owned summaries and runtime snapshots, not
  Workspace managers or root mutable config
  (`internal/server/workspaceapi/fleet_snapshot.go::FleetSnapshot`,
  `internal/server/fleetapi/handler.go::ConfigSnapshot`).
- Workspace overlays keep the local shell but omit links and metadata for
  removed source or associated items; inaccessible items remain visible
  (`internal/server/fleetapi/fleet_adapter.go::worktreeFromWorkspace`).

## Snapshot And Routing Contracts

- Snapshots use the shared federation protocol version, not a separate schema
  version; protocol version and node ID must match exactly
  (`internal/server/fleetapi/fleet_hub.go::Handler.fetchRawSnapshot`).
- Every projected host publishes `configKey == nodeID`, its topology role, and its canonical HTTPS origin. Self uses validated `fleet.base_url`; remote origins come from enrollment, never self-reporting
  (`internal/fleet/enrich.go::buildHost`, `internal/server/fleetapi/fleet_hub.go::Handler.fetchMemberRaw`).
- A failed, incompatible, or unauthenticated member degrades only that member to
  an unreachable summary; it must not fail local or other member results
  (`internal/server/fleetapi/fleet_hub.go::Handler.fetchMemberRaw`).
- Projection always recomputes operation availability. Coordinators route one
  hop; nodes treat every non-self host as summary-only
  (`internal/fleet/enrich.go::ProjectForObserver`).
- Workspace lists consume inline projected summaries without per-host fan-out;
  remote actions require the owning host's projected operation availability.
  Explicitly incomplete aggregates retain absent-host rows; authoritative views
  retain only explicitly degraded hosts and remove absent membership
  (`frontend/src/lib/components/terminal/workspace-list-schema.ts::retainDegradedHostWorkspaces`).
- Workspaces labels remote rows with host display names and shows fleet chrome
  only for actionable degradation; healthy host navigation belongs to the global
  Forge selector (`frontend/src/lib/components/terminal/WorkspaceListSidebar.svelte`).
- The reserved `self` alias and the daemon's stable node ID address the local
  handler. An active member's node ID routes only while federation is enabled
  on a coordinator and an outbound credential exists
  (`internal/server/fleetapi/fleet_proxy.go::Handler.resolveFleetHostTarget`).
- Proxy operations preserve the owning host's status, problem envelope, and
  safe end-to-end headers; browser-facing responses must not carry peer cookies,
  redirects, authentication challenges, or connection-nominated headers
  (`internal/server/fleetapi/fleet_proxy.go::Handler.serveRemoteFleetRESTProxy`).

## Transport Trust Boundary

- Supported fleets expose every Forge peer only through operator-controlled
  private ingress such as a tailnet. HTTPS and federation bearers still
  authenticate application peers; private reachability does not replace them.
- An activated coordinator and its nodes are one operator-controlled trust
  domain. A compromised active peer is equivalent to compromised fleet
  administration and is outside the federation isolation model. Route scopes
  enforce ownership and lifecycle boundaries; they are not a hostile-peer or
  multi-tenant sandbox (`internal/server/provider_route_policy.go::providerRouteDeclarations`).
- Pending nodes are not active peers. Pending coordinator bearers have only
  enrollment access; pending node bearers have preparation projections,
  handoff, and enrollment access. Generic provider reads require activation
  (`internal/server/api_auth.go::pendingProviderRouteAllowed`).
- A valid federation bearer takes precedence over optional ingress user
  identity so proxied peer requests retain their scoped node principal
  (`internal/server/api_auth.go::Server.authorizeAPIRequest`).
- Credentials require the current enrollment and configured binding.
  Activation upgrades both directions
  (`internal/federationauth/scope.go::CoordinatorToNodeScopes`,
  `internal/server/api_auth.go::Server.federationPrincipalEnrollmentState`).
- An active coordinator bearer is accepted by a node only when that daemon's
  startup activation succeeded. This is a local lifecycle gate inside the
  trusted fleet, not isolation from a malicious activated coordinator.
- The credential store persists inbound bearers only as SHA-256 digests and
  keeps outbound bearers readable for request construction. Atomic 0600 writes
  publish immutable in-memory snapshots only after persistence, so revocation
  takes effect on the next authentication attempt
  (`internal/federationauth/store.go::Store.Authenticate`,
  `internal/federationauth/store.go::Store.mutate`).
- Federation authorization is fail-closed: a valid bearer still receives
  a typed forbidden response when the method/path is not inventoried or its
  principal lacks the route's declared scope. Provider-owned operations join
  the closed inventory through their explicit read/write route rules
  (`internal/federationauth/authenticator.go::authorizedRoutes`,
  `internal/server/api_auth.go::Server.authorizeFederationRequest`).
- Provider calls require the exact federation protocol header after scope
  authorization; mismatch is explicit and never falls through to local state
  (`internal/server/api_auth.go::Server.authorizeFederationRequest`).
- Match authorization against the escaped request path; decoding `%2F` before
  scope lookup splits nested provider owners into extra route segments
  (`internal/server/api_auth.go::Server.canonicalAPIPath`).
- Enrollment reserves a digest-only inbound credential before the remote
  subject is known; it cannot authenticate until atomically bound to that
  subject
  (`internal/federationauth/store.go::Store.ReserveInbound`,
  `internal/federationauth/store.go::Store.BindInbound`).
- Issuing and transferring an enrollment token approves eventual active
  membership; the random preparation seal proves retry-safe handoff, not a
  second approval (`internal/server/fleetapi/fleet_enrollment.go::Handler.sealNodePreparation`).
- Member configuration accepts only canonical HTTPS origins. REST and WebSocket
  requests strip browser credentials and browser/proxy provenance, resolve the
  destination member first, and then add only that member's outbound bearer.
  Per-origin clients refuse redirects and bound connection setup. Ordinary
  reads remain time-bounded; long REST proxies use request cancellation because
  clone and diff-watch handlers can legitimately exceed snapshot deadlines
  (`internal/server/fleetapi/fleet_enrollment.go::hardenedFederationProxyHTTPClient`).
- WebSocket attach tracing ends after bounded connection setup and before the
  long-lived bridge (`internal/server/fleetapi/fleet_proxy.go::startFleetAttachSpan`).
- Remote terminals are WebSocket-to-WebSocket bridges. The coordinator does not
  allocate a proxy PTY or rewrite the owning node's local session backend
  (`internal/server/fleetapi/fleet_proxy.go::Handler.serveFleetWebSocketProxy`).
- Provider Git traffic is a separate transport boundary. HTTP clone URLs are
  supported only over an operator-controlled encrypted route; unencrypted public
  HTTP carrying provider credentials is unsupported (`internal/providerplane/client.go::ValidateFederationWorkspaceLaunchSpecResponse`).
- Activation gives the coordinator workspace mutation and terminal attachment
  authority on a node; pending coordinators have enrollment access only
  (`internal/federationauth/scope.go::CoordinatorToNodeScopes`).
- Coordinator-supplied durable Git facts require node-side federation validation,
  including during preparation before the node role is saved
  (`internal/server/node_preparation.go::Server.refreshNodePreparationLaunchSpecs`).

## Provider Facts For Local Execution

- The coordinator is the only authority for provider-backed workspace launch
  facts. A node resolves PR and issue launch specifications through the
  authenticated provider plane; it does not require local repository, pull, or
  issue replicas (`internal/server/provider_sources.go::coordinatorProviderSource.ResolveWorkspaceLaunchSpec`).
- Manual provider refresh, automatic assignment, and merge-request worktree
  facts cross the same provider plane. The coordinator performs provider API
  work; the node persists validated launch facts and performs only node-local
  Git and workspace work
  (`internal/server/provider_state_handoff.go::Server.federationRefreshWorkspaceLaunchSpec`,
  `internal/server/federation_provider_workspace.go::Server.federationAutoAssignWorkspaceItem`,
  `internal/server/provider_sources.go::coordinatorProviderSource.ResolveMergeRequestWorktreeFacts`).
- Clone-backed GitLab merge-request reads fetch only that merge request's
  provider-owned head ref before reading its descriptor SHA; the managed clone
  has no wildcard MR refspec (`internal/gitclone/clone.go::Manager.FetchMergeRequestHead`).
- Coordinator activity responses exclude its local workspace overlays and carry
  the coordinator-owned workspace-recency policy. Nodes apply that policy while
  rebuilding local activity and authors; provider markdown images remain
  coordinator-owned (`internal/server/huma_routes.go::Server.overlayLocalActivityWorkspaces`,
  `internal/server/provider_route_policy.go::providerRouteDeclarations`).
- A resolved specification is bound to the exact request and carries stable
  repository identity, base clone/default-branch facts, PR head-repository
  semantics, and source visibility. Before admitting the workspace, a node
  validates that binding and hosted clone URL, requires an exact Git credential
  route, rejects a historically occupied mutable route, and records only
  repository routing metadata in its node catalog (`internal/providerplane/client.go::ValidateFederationWorkspaceLaunchSpecResponse`,
  `internal/server/provider_sources.go::coordinatorProviderSource.ResolveWorkspaceLaunchSpec`).
- Federated MCP workspace creation resolves a coordinator repository descriptor
  before capturing the node-local route fence, so repositories discovered after
  activation become locally executable without a prior settings refresh
  (`internal/server/mcp_backend.go::mcpBackend.resolveWorkspaceRepositoryFence`).
- Registered node projects resolve stable coordinator repository identity during
  preparation and future registration; raw fleet snapshots remain read-only
  (`internal/server/node_preparation.go::Server.reconcileNodePreparationProjects`).
- Base and fork clone URLs must resolve to canonical provider repository routes;
  network URL syntax alone is not an authorization boundary
  (`internal/providerplane/client.go::federationRemoteRepositoryRoute`).
- The workspace row and launch specification commit atomically before
  asynchronous setup starts. Setup, retry and recovery, branch synchronization,
  PR monitoring, pushed-head observation, and generated agent context consume
  that persisted specification rather than node provider tables
  (`internal/db/queries_workspace_launch_specs.go::DB.CreateWorkspaceWithLaunchSpec`,
  `internal/workspace/launch_spec.go::Manager.RequireWorkspaceLaunchSpec`).
- Launch-spec refresh follows stable provider identity across renames and
  commits the verified route with the specification; reused routes stay fenced
  (`internal/db/queries_workspace_launch_specs.go::DB.PutRefreshedWorkspaceLaunchSpec`).
- Source visibility is a strict 15-minute coordinator lease. Existing work may
  continue while the lease is valid; once it expires, provider-backed lifecycle
  work must refresh it. Coordinator failure then returns a typed retryable error,
  while an explicitly hidden source remains unavailable. Ad-hoc and Kata
  workspaces stay node-local (`internal/workspace/launch_spec.go::LaunchSpecRefreshError`).

## Provider Event Flow And Availability

- `fleet.enabled` is live. Disabling it stops coordinator-backed provider work
  and cancels the inbound event stream; enabling it opens a fresh stream whose
  replay barrier performs authoritative refresh
  (`internal/server/federation_events.go::coordinatorEventLifecycle`).
- Nodes open one authenticated `events.read` SSE stream to the coordinator.
  The exact federation protocol header is required, redirects are refused, and
  the coordinator emits only its provider-owned event vocabulary
  (`internal/server/federation_events.go::Server.streamFederationEvents`,
  `internal/providerplane/events.go::EventClient`).
- Coordinator event IDs are private to that inbound stream. Each node
  re-stamps accepted events with its own `EventHub.Broadcast`, so browsers keep
  one node-local checkpoint and coordinator restart cannot move that checkpoint
  backward or forward (`internal/server/federation_events.go::Server.receiveCoordinatorEvent`).
- Stream open, replay staleness, and poison-frame recovery refresh provider
  projections and `sync_status` before the node announces the coordinator as
  connected. The frontend reconciles its selected projection before restoring
  provider availability, so stale cached data is not presented as current
  (`internal/server/federation_events.go::Server.resynchronizeCoordinatorProviderState`,
  `frontend/src/lib/app-stores.svelte.ts::createAppStores`).
- The inbound stream worker starts after Workspace and participates in the same
  dependent wait group as other Workspace consumers. Coordinator shutdown
  closes its event hub before draining HTTP so long-lived federation streams
  exit cleanly (`internal/server/server.go::Server.runWorkspaceDependent`,
  `internal/server/server.go::Server.Shutdown`).

## Configuration And Lifecycle

- Federation protocol version 3 requires an exact match; there is no
  translation or compatibility fallback
  (`internal/federation/protocol.go::ProtocolVersion`).
- Enrollment accepts only canonical HTTPS origins. Each token is consumed by
  its first accepted request; retry or rekey requires a new token
  (`internal/federation/enrollment_store.go::Store.Begin`).
- Each join abandons any unbound provisional record, revokes its reserved
  credential, and creates a fresh enrollment ID. Bound, completed, or
  preparation-started enrollments are never replaced, and a coordinator must
  revoke or migrate its members before becoming a node
  (`internal/server/fleetapi/fleet_enrollment.go::Handler.joinFederation`).
- Enrollment secrets are printed once and enter `fleet join` only through a
  hidden prompt, stdin, or `--token-file`. The sole pre-auth route is the exact
  enrollment POST (`cmd/kenn-forge/fleet_cli.go`,
  `internal/server/api_auth.go::Server.isPreEnrollmentRequest`).
- The token deadline governs only starting enrollment. Both peers enforce it for
  unstarted pending principals; preparation pins the enrollment until activation,
  abort, or revocation. Joining never changes `fleet.role` or restarts Forge
  (`internal/federation/enrollment_store.go::Store.CleanupExpired`,
  `internal/server/fleetapi/fleet_enrollment.go::Handler.activateEnrollment`).
- `fleet prepare-node` is resumable and talks only to the authenticated local
  daemon. Before sealing local writes it first proves the pending coordinator is
  reachable and pins that enrollment; coordinator failure before that point
  leaves the standalone provider plane open.
- Once quiescing begins, the provider-write barrier survives restarts. Only
  `fleet abort-preparation` may reopen it before activation, after admitted work
  drains. A node-shaped process requires restart before standalone provider work;
  forced abort still reports the enrollment to revoke when its coordinator is unavailable
  (`internal/server/node_preparation.go::Server.abortFederationNodePreparation`).
- Preparation waits for admitted provider writes and deferred merges to drain,
  then freezes notification acknowledgement admission before reading any state
  for handoff. It persists refreshed launch specifications only after the node's
  exact Git credential route resolves, then hands off review drafts and workflow
  rows. Imports are content-addressed:
  absent state imports, identical state returns the same receipt, and different
  state reports both digests without overwriting either side
  (`internal/server/node_preparation.go::Server.prepareFederationNode`).
- The coordinator issues an opaque, retry-safe preparation seal bound to the
  enrollment, both node IDs, protocol and migration versions, handoff receipt
  digest, and drained acknowledgement generation. A changed binding conflicts.
  The node records it in preparation state and the 0600 enrollment store.
  The daemon then saves node role under its config mutation locks for that
  binding; it reports `restart_required` and never restarts itself
  (`internal/server/node_preparation.go::Server.persistPreparedNodeRole`).
- Activation retries are bounded and reuse the sealed enrollment; the
  coordinator validates its issued seal, and already-active retries never
  duplicate membership. Invalid state is `action_required`, protocol mismatch
  is `incompatible`, and both preserve local execution while suppressing routes
  (`cmd/kenn-forge/node_startup.go::activateFederationNodeAtStartup`,
  `internal/server/fleetapi/fleet_enrollment.go::Handler.activateEnrollment`).
- Coordinator revocation removes both credential directions and terminates the
  node's open provider-event streams before returning, so the revoked bearer
  cannot receive later events or start another request
  (`internal/server/fleetapi/fleet_enrollment.go::Handler.revokeEnrollment`).
- Ordinary fleet settings update only operator preferences; enrollment owns
  role, coordinator binding, and membership, so a stale browser save cannot
  replace them (`internal/server/settings_routes.go::Server.updateFleetSettings`).
- Enrollment-managed membership and ordinary fleet settings can reload live.
  Role, coordinator identity/address, API authentication, and
  session-monitor policy report `restart_required` until the running process
  matches persisted config
  (`internal/server/config_reload.go::startupConfigSnapshot.restartRequiredFor`).
- Fleet workers start after Workspace and shut down before Workspace. Detailed
  shutdown rules live in
  [`workspace-runtime-lifecycle.md`](./workspace-runtime-lifecycle.md).
