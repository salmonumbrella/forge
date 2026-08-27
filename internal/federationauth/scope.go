// Package federationauth owns credentials used only for Forge-to-Forge
// federation traffic. These credentials are deliberately separate from the
// daemon token that grants a local user full access to one Forge instance.
package federationauth

import (
	"fmt"
	"slices"
)

// Scope is one capability in the closed federation authorization vocabulary.
type Scope string

const (
	ScopeSnapshotRead       Scope = "snapshot.read"
	ScopeWorkspaceRead      Scope = "workspace.read"
	ScopeWorkspaceWrite     Scope = "workspace.write"
	ScopeTerminalAttach     Scope = "terminal.attach"
	ScopeProviderRead       Scope = "provider.read"
	ScopeProviderWrite      Scope = "provider.write"
	ScopeProviderHandoff    Scope = "provider.handoff"
	ScopeEventsRead         Scope = "events.read"
	ScopeEnrollmentActivate Scope = "enrollment.activate"
)

var scopesInCanonicalOrder = []Scope{
	ScopeSnapshotRead,
	ScopeWorkspaceRead,
	ScopeWorkspaceWrite,
	ScopeTerminalAttach,
	ScopeProviderRead,
	ScopeProviderWrite,
	ScopeProviderHandoff,
	ScopeEventsRead,
	ScopeEnrollmentActivate,
}

var knownScopes = func() map[Scope]int {
	result := make(map[Scope]int, len(scopesInCanonicalOrder))
	for index, scope := range scopesInCanonicalOrder {
		result[scope] = index
	}
	return result
}()

// CoordinatorToNodeScopes returns the fixed grant used when the coordinator
// calls an execution node.
func CoordinatorToNodeScopes() []Scope {
	return []Scope{
		ScopeSnapshotRead,
		ScopeWorkspaceRead,
		ScopeWorkspaceWrite,
		ScopeTerminalAttach,
		ScopeEnrollmentActivate,
	}
}

// PendingCoordinatorToNodeScopes is the narrow grant held by a coordinator
// until the node confirms activation. Pending enrollment must not grant access
// to workspace mutations, shells, or terminals on a still-standalone daemon.
func PendingCoordinatorToNodeScopes() []Scope {
	return []Scope{ScopeEnrollmentActivate}
}

// NodeToCoordinatorScopes returns the fixed grant used when an execution node
// calls the coordinator.
func NodeToCoordinatorScopes() []Scope {
	return []Scope{
		ScopeSnapshotRead,
		ScopeProviderRead,
		ScopeProviderWrite,
		ScopeEventsRead,
		ScopeEnrollmentActivate,
	}
}

// PendingNodeToCoordinatorScopes is the deliberately narrow grant used while
// a node is enrolled but has not completed preparation and activation.
func PendingNodeToCoordinatorScopes() []Scope {
	return []Scope{
		ScopeProviderRead,
		ScopeProviderHandoff,
		ScopeEnrollmentActivate,
	}
}

func normalizeScopes(scopes []Scope) ([]Scope, error) {
	seen := make(map[Scope]struct{}, len(scopes))
	result := make([]Scope, 0, len(scopes))
	for _, scope := range scopes {
		if _, ok := knownScopes[scope]; !ok {
			return nil, fmt.Errorf("unknown federation scope %q", scope)
		}
		if _, duplicate := seen[scope]; duplicate {
			continue
		}
		seen[scope] = struct{}{}
		result = append(result, scope)
	}
	slices.SortFunc(result, func(left, right Scope) int {
		return knownScopes[left] - knownScopes[right]
	})
	return result, nil
}

// Principal is the authenticated identity established by an inbound bearer.
// Callers receive their own scope map so they cannot mutate a store snapshot.
type Principal struct {
	NodeID string
	Scopes map[Scope]struct{}
}

// Has reports whether the principal carries scope.
func (p Principal) Has(scope Scope) bool {
	_, ok := p.Scopes[scope]
	return ok
}

func validNodeID(nodeID string) bool {
	if len(nodeID) != 32 {
		return false
	}
	for _, char := range nodeID {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}
