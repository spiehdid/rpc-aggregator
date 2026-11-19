package admin

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestAdminAPI_NodesExposeLearnedLogsLimits(t *testing.T) {
	api := newTestAdmin()
	node := api.chains["ethereum"].Pool.GetNode("rpc1")
	node.RecordLogsSpanSuccess(1_000)
	node.RecordLogsSpanFailure(10_000, time.Now())

	for _, path := range []string{"/admin/status", "/admin/chains/ethereum/nodes"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		w := httptest.NewRecorder()
		api.Handler().ServeHTTP(w, req)
		var nodes []NodeStatus
		if path == "/admin/status" {
			var st StatusResponse
			json.NewDecoder(w.Body).Decode(&st)
			nodes = st.Chains["ethereum"].Nodes
		} else {
			json.NewDecoder(w.Body).Decode(&nodes)
		}
		if len(nodes) != 1 || nodes[0].LogsMaxOKSpan != 1_000 || nodes[0].LogsFailSpan != 10_000 {
			t.Errorf("%s: want logs spans ok=1000 fail=10000, got %+v", path, nodes)
		}
	}
}
