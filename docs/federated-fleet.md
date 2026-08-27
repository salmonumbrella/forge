# Federated Forge

A Forge fleet gives several development machines one shared view without
moving their local work to a central server.

- One **coordinator** owns provider synchronization, provider mutations, and
  the fleet-wide view.
- Each **node** owns its local repositories, workspaces, processes, Git
  traffic, and tmux sessions.
- Every machine keeps its own directly accessible Forge UI.
- Forge-to-Forge requests and terminal WebSockets use authenticated HTTPS.

Every machine runs the same `kenn-forge` binary. The coordinator does not use
SSH to start a node or attach to its tmux server. Start and supervise each
daemon on its own machine.

This guide uses the following example topology. Each name is a private HTTPS
origin reachable from every fleet machine:

```text
https://forge-coordinator.example.test  coordinator and local execution
https://build-a.example.test            node and local execution
https://build-b.example.test            node and local execution
```

Tailscale Serve is a first-class way to provide those origins, but it is not a
fleet requirement. A private LAN, UniFi network, VPN, or custom reverse proxy
works through the same federation protocol.

## Understand the trust boundary

Use federation only inside one operator-controlled private network. Keep every
Forge listener behind that private ingress.

Private network access is necessary, but it is not sufficient. Forge also
requires HTTPS, local browser authentication, and separate credentials created
during enrollment. An activated coordinator and its nodes form one
administrative trust domain. Someone who controls an active peer should be
treated as controlling fleet workspace and terminal operations.

Pending nodes have narrower access. They can resolve only the provider facts
needed for preparation, hand state to the coordinator, and use enrollment
routes. They cannot browse general provider data or perform provider writes.

## Before you start

On every machine:

1. Install the same `kenn-forge` build on every machine.
2. Log in to the Git host used for local clone, fetch, and push operations. For
   GitHub, `gh auth login` is the normal route.
3. Choose one stable private HTTPS origin for that machine.
4. Confirm every machine can resolve and reach every origin.

Do not copy a Forge data directory between machines. Each directory receives a
stable random node ID on first start.

A federation origin contains only a scheme and authority, such as
`https://build-a.example.test`. Forge rejects cleartext HTTP origins, URL
paths, query strings, fragments, and embedded credentials. Explicit ports are
allowed; default `:443` is normalized away.

## Set up the coordinator

Run setup as the operating-system user that will run Forge. The normal
Tailscale path discovers the machine's certificate name and current Tailscale
login, publishes Forge with Tailscale Serve, installs a per-user service, and
checks the protected HTTPS API:

```sh
kenn-forge fleet setup coordinator --tailscale
```

On macOS, setup also finds the command-line client inside the standard
Tailscale app when `tailscale` is not on `PATH`.

Review the displayed plan and confirm it. For unattended setup, inspect the
same plan with `--dry-run`, then repeat with `--yes`.

If your LAN, UniFi network, VPN, or reverse proxy already provides private
HTTPS, give Forge its canonical origin instead:

```sh
kenn-forge fleet setup coordinator \
  --origin https://forge-coordinator.example.test
```

The `--origin` path never calls Tailscale. It configures and supervises the
same loopback Forge service, while your ingress owns DNS and TLS. It retains
Forge's normal bearer and browser-cookie authentication.

Setup requires exactly one of `--tailscale` and `--origin`. It does not expose
the Forge listener directly, weaken API authentication, or make the service
depend on a particular network product.

The coordinator also needs the provider credentials and repository
configuration used for synchronization.

## Set up each node

Run the matching setup command on each node:

```sh
# Tailscale Serve
kenn-forge fleet setup node --tailscale

# Operator-managed private HTTPS
kenn-forge fleet setup node --origin https://build-a.example.test
```

Node setup deliberately leaves the daemon in standalone mode. Do not set
`fleet.role = "node"` by hand. Enrollment preparation persists that role only
after provider writes drain and state handoff completes.

On Linux, setup installs a systemd user service and enables lingering so Forge
survives logout. On macOS, it installs a LaunchAgent under the selected user.
Both service definitions execute the installed binary directly and preserve
the user's credential environment.

Tailscale identity mode treats local processes on the Forge host as trusted,
because Tailscale Serve forwards identity headers over loopback. Use it on a
single-user or otherwise trusted machine. On a multi-user host, use an external
origin and Forge's bearer/cookie authentication instead.

## Keep credentials separate

Four credential types serve different purposes:

| Credential | Stored on | Purpose |
| --- | --- | --- |
| Browser/API session | Each daemon | Authorizes a person using that daemon's UI or local API |
| Federation credential | Coordinator and enrolled node | Authorizes one exact Forge-to-Forge direction and route set |
| Provider API credential | Coordinator | Synchronizes provider data and performs provider mutations |
| Git credential | Each execution machine | Clones, fetches, pulls, and pushes directly against the Git host |

Do not copy browser cookies or API tokens between machines. Enrollment creates
a different machine credential in each direction. Forge stores inbound
credentials as digests and keeps credential files private to the daemon
account.

A node needs its own Git credential even though it does not synchronize
provider data. Preparation verifies the exact Git credential route before it
stores a workspace launch specification.

The coordinator may have more than one credential source. User-attributed
provider mutations use a user credential. Forge-managed Git follows normal
credential priority and can use an App credential for repositories covered by
that App. Verify the route that matters to your deployment instead of assuming
all coordinator Git traffic uses one credential.

The coordinator strips browser authorization, cookies, origin, and forwarding
headers before it calls a node. It adds only the federation credential enrolled
for that exact HTTPS origin and does not follow redirects.

## Enroll one node at a time

Finish and verify one node before enrolling another. This keeps state handoff
and rollback bounded to one machine.

### 1. Create a one-time token

On the coordinator:

```sh
umask 077
kenn-forge fleet enrollment-token \
  --base-url https://forge-coordinator.example.test \
  --name "Forge coordinator" \
  --ttl 10m > ./forge-enrollment-token
```

The token is printed once, expires, and can create one enrollment. Creating and
transferring it approves that node to finish preparation and activate. Treat it
as a membership credential.

Transfer the file through an approved secret-sharing channel. Do not put the
token in a command-line argument or config file.

### 2. Join from the node

On `build-a`:

```sh
kenn-forge fleet join https://forge-coordinator.example.test \
  --base-url https://build-a.example.test \
  --name "Build node A" \
  --token-file ./forge-enrollment-token
rm ./forge-enrollment-token
```

For automation, pass the token on standard input instead of creating a node-side
file:

```sh
secret-command | kenn-forge fleet join \
  https://forge-coordinator.example.test \
  --base-url https://build-a.example.test \
  --name "Build node A"
```

Interactive terminals use a hidden token prompt when neither input method is
provided.

Joining records a pending enrollment. It does not change the node role or
restart the daemon.

### 3. Prepare the node

Run on the node:

```sh
kenn-forge fleet prepare-node
```

Preparation stops new provider writes, waits for admitted writes and deferred
merges, drains notification acknowledgements, refreshes workspace launch
information, hands provider state to the coordinator, and seals local provider
writes. If it reports concrete remaining work, resolve that work and run the
command again.

The token's original deadline no longer applies after preparation starts.

### 4. Restart and verify activation

When preparation reports completion, restart Forge through the node's service
manager. Activation happens during startup. Verify the node is active in the
coordinator's Fleet settings and in the node's direct UI.

Do not enroll the next node yet. Complete the checks in [Verify the
fleet](#verify-the-fleet) for this node first.

## Abort or revoke an enrollment

Before activation, restore standalone operation from the node:

```sh
kenn-forge fleet abort-preparation
```

This revokes the pending enrollment and reopens the durable provider-write
gate. On a standalone process, provider writes resume immediately. If the
daemon already restarted in node mode, the command reports that another
restart is required.

If the coordinator is unavailable, `--force` performs local recovery and
prints the enrollment ID that must be revoked later.

To remove a pending or active enrollment, run on the coordinator:

```sh
kenn-forge fleet revoke ENROLLMENT_ID
```

Revoke before removing a node from configuration. After activation, restoring
a former standalone node requires its complete pre-enrollment unit: config,
database, federation store, credential store, and binary. Do not restore only
one file from that set.

## Verify the fleet

Configuration checks are not enough. Exercise the protected paths and the
behavior an operator depends on.

### Transport and identity

- Open every direct HTTPS UI and establish its own authenticated browser
  session.
- Confirm every HTTPS certificate validates normally.
- Confirm the HTTPS ingress is reachable only through the intended private
  network.
- Confirm every daemon reports a unique node ID.
- Restart each daemon and confirm its node ID and enrollment persist.

### Fleet view and local ownership

- Confirm every UI shows the same fleet host set and provider data.
- Create a disposable workspace on each execution host.
- Confirm each node UI uses fresh local state for its own workspace.
- Confirm the coordinator shows each workspace under its owning node.
- Start a tmux-backed session and attach through the node's direct UI.
- Attach to the same session through the coordinator's WebSocket proxy.
- Remove the disposable node workspace from the coordinator and confirm the
  node performs the deletion.

A node UI treats other hosts as summary-only. Send remote mutations from the
coordinator instead of trying to route them through another node.

### Provider and Git ownership

- Observe provider synchronization on the coordinator.
- Confirm nodes run no provider synchronization worker.
- Start one safe provider mutation from a node UI and observe it complete
  through the coordinator.
- Clone or fetch a private repository on every node and confirm that machine's
  Git credential route is used.
- Confirm provider API request budget is not independently consumed by every
  node.

### Failure behavior

- Stop one node. Healthy machines and their workspaces should remain visible;
  only the stopped node should become unavailable.
- Stop the coordinator. Each node should retain local workspace authority and
  mark provider and aggregate data unavailable or incomplete.
- Restart the coordinator. Nodes should reconnect, replay events, reconcile
  provider state, and restore the fleet view without a browser reload.
- Restart each node. Its direct UI, local workspaces, enrollment, and remote
  terminal attachment should recover.

## Use the fleet

Use the Forge selector in the top bar to open any fleet member directly. It is
an ordinary link, so browser modifiers open another tab or window. Each origin
keeps its own browser state, filters, searches, terminal connections, and event
cursors; changing one Forge tab does not retarget another.

Open the coordinator UI for the aggregate workspace view and fleet-wide
operations. Supported actions call the owning node's HTTPS API:

- create, inspect, refresh, or remove a workspace;
- launch or stop a runtime session;
- inspect local Git state;
- open a terminal through the WebSocket bridge.

The coordinator never creates a local proxy process for a remote terminal.
Input, output, resize messages, and close events pass between the two WebSocket
connections.

Open a node UI when you want that machine's local execution context while still
seeing the aggregate fleet and global provider data. If the coordinator is
down, the node remains useful for local workspace operations but cannot provide
current coordinator-owned provider data.

## Advanced: publish with your own reverse proxy

The setup command always binds Forge to loopback and validates the public
authority with `trust_reverse_proxy = false`. Your proxy must preserve the
incoming `Host` header and forward to the configured loopback port. It must not
rewrite `Host` to the upstream address.

For example:

```caddyfile
https://build-a.example.test {
    bind 192.0.2.20
    reverse_proxy 127.0.0.1:8091
}
```

Use your real private address and a certificate trusted by every fleet
machine. Verify the HTTPS URL with the normal operating-system trust store. Do
not use `curl -k`; it hides certificate and hostname mistakes that also break
federation.

`--origin` does not manage this proxy, its certificate, DNS, or browser login.
That separation is intentional: the federation contract is canonical HTTPS,
not Tailscale or Caddy.

## Coordinator replacement is a separate migration

Do not replace the coordinator by changing DNS or copying one credential. An
enrollment is bound to the coordinator's stable node ID and canonical origin.
The coordinator also stores provider state alongside its own local execution
state.

A safe replacement must transfer coordinator-owned provider state without
reassigning local workspaces, enroll every node against the new coordinator,
revoke both old credential directions, and support rollback during the
transition. Forge does not currently provide that complete workflow.

## Replace an older fleet

Older key-based and shell-relay entries are not accepted. Before starting the
new binary:

1. Remove the old fleet entries from every config file.
2. Enable API authentication and select the coordinator.
3. Start each daemon with a reachable HTTPS origin.
4. Enroll every node again with a new one-time token.

Do not copy old tokens, node IDs, relay sockets, or daemon data directories.
There is no automatic translation because the older entries did not establish
the identity and credential pair required by federation.

## Troubleshooting

### A node is unreachable

From the coordinator machine, verify the node's HTTPS origin and certificate.
Confirm the URL exactly matches the enrolled origin. A changed hostname, port,
or certificate route requires deliberate re-enrollment rather than a redirect.

If the daemon is down, restart it through its local service manager. The
coordinator does not log in to start it.

### A request is forbidden

Check that the enrollment is active and has not been revoked. A valid
credential can still be forbidden when its direction does not grant the
requested operation. Re-enroll instead of widening or copying a token by hand.

### A terminal does not open

Confirm the node is reachable and the local session still exists. The
coordinator attaches to the node's terminal WebSocket; it does not attach to
the node's tmux server directly.

### A config no longer loads

Remove old fleet keys and relay entries, then use the enrollment workflow.
Forge fails closed instead of guessing how an old host label maps to a stable
node identity.
