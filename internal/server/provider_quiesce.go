package server

import (
	"context"
	"errors"
	"net/http"

	"go.kenn.io/forge/internal/federationauth"
	"go.kenn.io/forge/internal/providerplane"
	"go.kenn.io/forge/internal/server/httpapi"
)

func nodePreparationProblem() *httpapi.ProblemError {
	return httpapi.NewProblem(
		http.StatusConflict,
		httpapi.CodeNodePreparationInProgress,
		"provider writes are sealed while this daemon is being prepared as a federation node",
		map[string]any{"reason": "nodePreparationInProgress"},
	)
}

func (s *Server) admitProviderWrite(
	w http.ResponseWriter,
	r *http.Request,
) (func(), bool) {
	if s.providerRouteNode || s.providerWriteGate == nil {
		return nil, false
	}
	rule, ok := providerRouteRuleForRequest(r.Method, s.canonicalAPIPath(r))
	if !ok || rule.PeerScope != federationauth.ScopeProviderWrite {
		return nil, false
	}
	// Several provider mutations deliberately detach durable work from the
	// request lifetime. Admission is an in-memory phase check, so preserve that
	// existing contract even when the client disconnects before dispatch.
	release, err := s.providerWriteGate.Admit(context.WithoutCancel(r.Context()))
	if err == nil {
		return release, false
	}
	if errors.Is(err, providerplane.ErrNodePreparationInProgress) {
		writeProblemResponse(w, nodePreparationProblem())
	} else {
		writeProblemResponse(w, httpapi.NewProblem(
			http.StatusInternalServerError, httpapi.CodeInternalError,
			"admit provider write: "+err.Error(), nil,
		))
	}
	return nil, true
}
