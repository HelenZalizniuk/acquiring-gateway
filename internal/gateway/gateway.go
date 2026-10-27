package gateway

import (
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"
)

const (
	HeaderMerchantID  = "X-Merchant-ID"
	HeaderRouteTarget = "X-Route-Target" // "new" | "legacy"

	defaultProxyMaxIdleConns   = 100
	defaultProxyIdleTimeout    = 90 * time.Second
	defaultProxyTLSHandshake   = 10 * time.Second
	defaultProxyExpectContinue = 1 * time.Second
	defaultProxyResponseHeader = 30 * time.Second
)

type Config struct {
	ListenAddr        string
	LegacyURL         string
	NewPaymentsURL    string // gRPC address of the payment service (host:port or http://host:port)
	MigratedMerchants map[string]struct{}
	GRPC              GRPCClientConfig
}

type Gateway struct {
	legacyProxy *httputil.ReverseProxy
	paymentGRPC *PaymentGRPCClient
	migrated    map[string]struct{}
	logger      *slog.Logger
}

func New(cfg Config, logger *slog.Logger) (*Gateway, error) {
	if logger == nil {
		logger = slog.Default()
	}

	legacyURL, err := url.Parse(cfg.LegacyURL)
	if err != nil {
		return nil, fmt.Errorf("parse legacy url: %w", err)
	}

	paymentGRPC, err := NewPaymentGRPCClientWithConfig(cfg.NewPaymentsURL, cfg.GRPC, logger)
	if err != nil {
		return nil, fmt.Errorf("create payment grpc client: %w", err)
	}

	migrated := cfg.MigratedMerchants
	if migrated == nil {
		migrated = map[string]struct{}{}
	}

	g := &Gateway{
		legacyProxy: newReverseProxy(legacyURL, "legacy", logger),
		paymentGRPC: paymentGRPC,
		migrated:    migrated,
		logger:      logger,
	}
	return g, nil
}

func (g *Gateway) Close() error {
	if g == nil {
		return nil
	}
	return g.paymentGRPC.Close()
}

func newReverseProxy(target *url.URL, name string, logger *slog.Logger) *httputil.ReverseProxy {
	proxy := httputil.NewSingleHostReverseProxy(target)

	originalDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		originalDirector(req)
		req.Host = target.Host
		req.Header.Set("X-Forwarded-Host", req.Header.Get("Host"))
		req.Header.Set("X-Gateway-Target", name)
	}

	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		logger.Error("upstream proxy error",
			"target", name,
			"upstream", target.String(),
			"method", r.Method,
			"path", r.URL.Path,
			"err", err,
		)
		http.Error(w, "bad gateway", http.StatusBadGateway)
	}

	proxy.Transport = &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		MaxIdleConns:          defaultProxyMaxIdleConns,
		IdleConnTimeout:       defaultProxyIdleTimeout,
		TLSHandshakeTimeout:   defaultProxyTLSHandshake,
		ExpectContinueTimeout: defaultProxyExpectContinue,
		ResponseHeaderTimeout: defaultProxyResponseHeader,
	}

	return proxy
}

func (g *Gateway) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", g.route)
	return mux
}

func (g *Gateway) route(w http.ResponseWriter, r *http.Request) {
	target := g.resolveTarget(r)

	g.logger.Info("route request",
		"method", r.Method,
		"path", r.URL.Path,
		"merchant_id", r.Header.Get(HeaderMerchantID),
		"route_target", target,
	)

	switch target {
	case "new":
		if !isPaymentsPath(r.URL.Path) {
			writeJSON(w, http.StatusNotFound, map[string]string{
				"error": "path is not available on the new payment service",
			})
			return
		}
		g.paymentGRPC.HandleProcessPayment(w, r)
	default:
		g.legacyProxy.ServeHTTP(w, r)
	}
}

// resolveTarget implements Strangler Fig routing:
//   - X-Route-Target overrides routing (canary / debug)
//   - /v1/payments goes to the new service only for migrated merchants
//   - everything else is forwarded to the legacy stack
func (g *Gateway) resolveTarget(r *http.Request) string {
	switch strings.ToLower(strings.TrimSpace(r.Header.Get(HeaderRouteTarget))) {
	case "new":
		return "new"
	case "legacy":
		return "legacy"
	}

	if isPaymentsPath(r.URL.Path) && g.isMigratedMerchant(r.Header.Get(HeaderMerchantID)) {
		return "new"
	}

	return "legacy"
}

func isPaymentsPath(path string) bool {
	path = strings.TrimSuffix(path, "/")
	return path == "/v1/payments" || strings.HasPrefix(path, "/v1/payments/")
}

func (g *Gateway) isMigratedMerchant(merchantID string) bool {
	merchantID = strings.TrimSpace(merchantID)
	if merchantID == "" {
		return false
	}
	_, ok := g.migrated[merchantID]
	return ok
}
