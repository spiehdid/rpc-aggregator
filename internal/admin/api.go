package admin

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/spiehdid/rpc-aggregator/internal/cache"
	"github.com/spiehdid/rpc-aggregator/internal/pool"
)

//go:embed dashboard.html
var dashboardHTML []byte

type ChainInfo struct {
	Pool   *pool.NodePool
	Cache  *cache.Cache
	Scorer *pool.Scorer
}

type API struct {
	chains    map[string]ChainInfo
	startedAt time.Time
	token     string
}

func New(chains map[string]ChainInfo, startedAt time.Time, token string) *API {
	return &API{chains: chains, startedAt: startedAt, token: token}
}

type StatusResponse struct {
	Chains map[string]ChainStatus `json:"chains"`
	Uptime string                 `json:"uptime"`
}

type ChainStatus struct {
	HealthyNodes    int            `json:"healthy_nodes"`
	TotalNodes      int            `json:"total_nodes"`
	BestBlockHeight uint64         `json:"best_block_height"`
	Nodes           []NodeStatus   `json:"nodes"`
	CacheStats      CacheStatsResp `json:"cache_stats"`
}

type NodeStatus struct {
	ID          string  `json:"id"`
	URL         string  `json:"url"`
	BlockHeight uint64  `json:"block_height"`
	LatencyMs   int64   `json:"latency_ms"`
	ErrorRate   float64 `json:"error_rate"`
	Score       float64 `json:"score"`
	Circuit     string  `json:"circuit"`
}

type CacheStatsResp struct {
	Entries int     `json:"entries"`
	HitRate float64 `json:"hit_rate"`
}

type AddNodeRequest struct {
	ID     string  `json:"id"`
	URL    string  `json:"url"`
	Weight float64 `json:"weight"`
}

type UpdateWeightRequest struct {
	Weight float64 `json:"weight"`
}

func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/status", a.requireAuth(a.handleStatus))
	mux.HandleFunc("/admin/config", a.requireAuth(a.handleConfig))
	mux.HandleFunc("/admin/chains/", a.requireAuth(a.handleChains))
	mux.HandleFunc("/dashboard", a.requireAuth(a.handleDashboard))
	mux.HandleFunc("/health", a.handleHealth)
	return mux
}

func (a *API) requireAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if a.token == "" {
			next(w, r)
			return
		}
		auth := r.Header.Get("Authorization")
		if auth != "Bearer "+a.token {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

func (a *API) handleDashboard(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(dashboardHTML)
}

func (a *API) handleConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	type chainSummary struct {
		NodeCount int `json:"node_count"`
	}
	resp := make(map[string]chainSummary)
	for name, info := range a.chains {
		resp[name] = chainSummary{NodeCount: len(info.Pool.Nodes())}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (a *API) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	resp := StatusResponse{Chains: make(map[string]ChainStatus), Uptime: time.Since(a.startedAt).Round(time.Second).String()}
	for name, info := range a.chains {
		nodes := info.Pool.Nodes()
		var healthy int
		var bestHeight uint64
		var minLat, maxLat time.Duration
		var minH, maxH uint64
		first := true
		for _, n := range nodes {
			if n.CircuitState() == pool.CircuitOpen {
				continue
			}
			if first {
				minLat, maxLat = n.GetLatency(), n.GetLatency()
				minH, maxH = n.GetBlockHeight(), n.GetBlockHeight()
				first = false
			} else {
				if n.GetLatency() < minLat {
					minLat = n.GetLatency()
				}
				if n.GetLatency() > maxLat {
					maxLat = n.GetLatency()
				}
				if n.GetBlockHeight() < minH {
					minH = n.GetBlockHeight()
				}
				if n.GetBlockHeight() > maxH {
					maxH = n.GetBlockHeight()
				}
			}
		}
		nodeStatuses := make([]NodeStatus, 0, len(nodes))
		for _, n := range nodes {
			if n.CircuitState() != pool.CircuitOpen {
				healthy++
			}
			if n.GetBlockHeight() > bestHeight {
				bestHeight = n.GetBlockHeight()
			}
			var score float64
			if n.CircuitState() != pool.CircuitOpen && info.Scorer != nil {
				score = math.Round(info.Scorer.Score(n, minLat, maxLat, minH, maxH)*100) / 100
			}
			nodeStatuses = append(nodeStatuses, NodeStatus{
				ID: n.ID, URL: n.URL, BlockHeight: n.GetBlockHeight(),
				LatencyMs: n.GetLatency().Milliseconds(), ErrorRate: n.ErrorRate(),
				Score: score, Circuit: n.CircuitState().String(),
			})
		}
		stats := info.Cache.Stats()
		var hitRate float64
		if total := stats.Hits + stats.Misses; total > 0 {
			hitRate = float64(stats.Hits) / float64(total)
		}
		resp.Chains[name] = ChainStatus{
			HealthyNodes: healthy, TotalNodes: len(nodes), BestBlockHeight: bestHeight,
			Nodes: nodeStatuses, CacheStats: CacheStatsResp{Entries: stats.Entries, HitRate: hitRate},
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (a *API) handleChains(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/admin/chains/")
	parts := strings.Split(path, "/")
	if len(parts) < 2 {
		http.NotFound(w, r)
		return
	}
	chainName := parts[0]
	info, ok := a.chains[chainName]
	if !ok {
		http.NotFound(w, r)
		return
	}
	if parts[1] != "nodes" {
		http.NotFound(w, r)
		return
	}
	switch {
	case len(parts) == 2:
		switch r.Method {
		case http.MethodGet:
			a.listNodes(w, info)
		case http.MethodPost:
			a.addNode(w, r, info)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	case len(parts) == 3:
		nodeID := parts[2]
		switch r.Method {
		case http.MethodDelete:
			a.deleteNode(w, info, nodeID)
		case http.MethodPatch:
			a.updateNode(w, r, info, nodeID)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	case len(parts) == 4 && parts[3] == "reset":
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		a.resetCircuit(w, info, parts[2])
	default:
		http.NotFound(w, r)
	}
}

func (a *API) listNodes(w http.ResponseWriter, info ChainInfo) {
	nodes := info.Pool.Nodes()
	statuses := make([]NodeStatus, 0, len(nodes))
	for _, n := range nodes {
		statuses = append(statuses, NodeStatus{
			ID: n.ID, URL: n.URL, BlockHeight: n.GetBlockHeight(),
			LatencyMs: n.GetLatency().Milliseconds(), ErrorRate: n.ErrorRate(), Circuit: n.CircuitState().String(),
		})
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(statuses)
}

func sanitizeID(id string) string {
	return strings.ReplaceAll(id, "/", "_")
}

// validateNodeURL rejects URLs that could be used for SSRF attacks.
// Only http and https schemes are allowed, and internal/loopback addresses are blocked.
func validateNodeURL(rawURL string) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("URL scheme must be http or https, got %q", u.Scheme)
	}
	host := u.Hostname()
	if host == "" {
		return fmt.Errorf("URL must have a hostname")
	}
	if host == "localhost" {
		return fmt.Errorf("localhost not allowed")
	}
	ip := net.ParseIP(host)
	if ip != nil {
		if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() {
			return fmt.Errorf("internal IP addresses not allowed: %s", host)
		}
	}
	// Resolve hostname and check resolved IPs (DNS rebinding protection).
	// If the hostname cannot be resolved (e.g. DNS unavailable), we skip
	// the check — the node simply won't work at runtime.
	if ip == nil {
		ips, err := net.LookupIP(host)
		if err == nil {
			for _, resolved := range ips {
				if resolved.IsLoopback() || resolved.IsPrivate() || resolved.IsLinkLocalUnicast() {
					return fmt.Errorf("hostname %q resolves to internal IP %s", host, resolved)
				}
			}
		}
	}
	return nil
}

func (a *API) addNode(w http.ResponseWriter, r *http.Request, info ChainInfo) {
	r.Body = http.MaxBytesReader(w, r.Body, 1024*1024) // 1MB
	var req AddNodeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	req.ID = sanitizeID(req.ID)
	if err := validateNodeURL(req.URL); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := info.Pool.AddNode(req.ID, req.URL, req.Weight); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (a *API) deleteNode(w http.ResponseWriter, info ChainInfo, nodeID string) {
	if err := info.Pool.RemoveNode(nodeID); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *API) updateNode(w http.ResponseWriter, r *http.Request, info ChainInfo, nodeID string) {
	r.Body = http.MaxBytesReader(w, r.Body, 1024*1024) // 1MB
	var req UpdateWeightRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	if err := info.Pool.UpdateWeight(nodeID, req.Weight); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (a *API) resetCircuit(w http.ResponseWriter, info ChainInfo, nodeID string) {
	if err := info.Pool.ResetCircuitBreaker(nodeID); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (a *API) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"ok"}`))
}
