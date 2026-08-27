package providerplane

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go.kenn.io/forge/internal/db"
)

var ErrNodePreparationInProgress = errors.New("node preparation is in progress")

type ProviderWriteStatus struct {
	Phase                  db.NodePreparationPhase `json:"phase"`
	InFlightProviderWrites int                     `json:"in_flight_provider_writes"`
	ActiveDeferredMerges   int                     `json:"active_deferred_merges"`
	UndrainedAcks          int                     `json:"undrained_acks"`
	DrainAckGeneration     *int64                  `json:"drain_ack_generation,omitempty"`
}

// ProviderWriteGate is the single admission boundary for provider mutations
// while a standalone daemon is being converted into a federation node.
type ProviderWriteGate struct {
	database *db.DB

	mu             sync.Mutex
	phase          db.NodePreparationPhase
	inFlight       int
	deferredMerges int
	loadErr        error
}

func NewProviderWriteGate(database *db.DB) *ProviderWriteGate {
	gate := &ProviderWriteGate{database: database, phase: db.NodePreparationOpen}
	if database == nil {
		gate.phase = db.NodePreparationQuiescing
		gate.loadErr = errors.New("provider write gate database is unavailable")
		return gate
	}
	state, err := database.GetNodePreparation(context.Background())
	if err != nil {
		gate.phase = db.NodePreparationQuiescing
		gate.loadErr = err
		return gate
	}
	gate.phase = state.Phase
	return gate
}

func (g *ProviderWriteGate) Admit(ctx context.Context) (func(), error) {
	if g == nil {
		return nil, errors.New("provider write gate is unavailable")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.loadErr != nil {
		return nil, fmt.Errorf("load durable node preparation state: %w", g.loadErr)
	}
	if g.phase != db.NodePreparationOpen {
		return nil, ErrNodePreparationInProgress
	}
	g.inFlight++
	var once sync.Once
	return func() {
		once.Do(func() {
			g.mu.Lock()
			g.inFlight--
			g.mu.Unlock()
		})
	}, nil
}

// BeginDeferredMerge admits a durable background mutation separately from the
// HTTP request that asked to queue it. Its release remains live until the
// deferred operation reaches a known terminal outcome.
func (g *ProviderWriteGate) BeginDeferredMerge(ctx context.Context) (func(), error) {
	if g == nil {
		return nil, errors.New("provider write gate is unavailable")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.loadErr != nil {
		return nil, fmt.Errorf("load durable node preparation state: %w", g.loadErr)
	}
	if g.phase != db.NodePreparationOpen {
		return nil, ErrNodePreparationInProgress
	}
	g.deferredMerges++
	var once sync.Once
	return func() {
		once.Do(func() {
			g.mu.Lock()
			g.deferredMerges--
			g.mu.Unlock()
		})
	}, nil
}

func (g *ProviderWriteGate) BeginQuiesce(
	ctx context.Context,
	binding db.NodePreparationBinding,
) (db.NodePreparationState, error) {
	if g == nil || g.database == nil {
		return db.NodePreparationState{}, errors.New("provider write gate is unavailable")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	state, err := g.database.BeginNodePreparation(ctx, binding)
	if err != nil {
		return db.NodePreparationState{}, err
	}
	g.loadErr = nil
	g.phase = state.Phase
	return state, nil
}

// AbortPreparation reopens provider writes after a pending enrollment is
// abandoned. The durable reset happens while the admission lock is held, so
// no write can enter between the database and in-memory phase changes.
func (g *ProviderWriteGate) CanAbortPreparation() error {
	if g == nil || g.database == nil {
		return errors.New("provider write gate is unavailable")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.inFlight != 0 || g.deferredMerges != 0 {
		return errors.New("provider writes are still draining")
	}
	return nil
}

func (g *ProviderWriteGate) AbortPreparation(ctx context.Context) error {
	if g == nil || g.database == nil {
		return errors.New("provider write gate is unavailable")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.inFlight != 0 || g.deferredMerges != 0 {
		return errors.New("provider writes are still draining")
	}
	if err := g.database.AbortNodePreparation(ctx); err != nil {
		return err
	}
	g.loadErr = nil
	g.phase = db.NodePreparationOpen
	return nil
}

func (g *ProviderWriteGate) Status(
	ctx context.Context,
) (ProviderWriteStatus, error) {
	if g == nil || g.database == nil {
		return ProviderWriteStatus{}, errors.New("provider write gate is unavailable")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.loadErr != nil {
		return ProviderWriteStatus{}, g.loadErr
	}
	state, err := g.database.GetNodePreparation(ctx)
	if err != nil {
		return ProviderWriteStatus{}, err
	}
	g.phase = state.Phase
	if state.Phase == db.NodePreparationQuiescing &&
		g.inFlight == 0 && g.deferredMerges == 0 &&
		state.DrainAckGeneration == nil {
		if _, err := g.database.FreezeNodePreparationAckGeneration(ctx); err != nil {
			return ProviderWriteStatus{}, err
		}
		state, err = g.database.GetNodePreparation(ctx)
		if err != nil {
			return ProviderWriteStatus{}, err
		}
	}
	undrained, err := g.database.CountUndrainedNotificationAcks(ctx)
	if err != nil {
		return ProviderWriteStatus{}, err
	}
	return ProviderWriteStatus{
		Phase: state.Phase, InFlightProviderWrites: g.inFlight,
		ActiveDeferredMerges: g.deferredMerges, UndrainedAcks: undrained,
		DrainAckGeneration: state.DrainAckGeneration,
	}, nil
}

func (g *ProviderWriteGate) Phase() db.NodePreparationPhase {
	if g == nil {
		return db.NodePreparationQuiescing
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.phase
}
