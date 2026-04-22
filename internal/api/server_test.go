package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gin "github.com/gin-gonic/gin"
	proxyconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	internallogging "github.com/router-for-me/CLIProxyAPI/v6/internal/logging"
	sdkaccess "github.com/router-for-me/CLIProxyAPI/v6/sdk/access"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

func newTestServer(t *testing.T) *Server {
	t.Helper()

	gin.SetMode(gin.TestMode)

	tmpDir := t.TempDir()
	authDir := filepath.Join(tmpDir, "auth")
	if err := os.MkdirAll(authDir, 0o700); err != nil {
		t.Fatalf("failed to create auth dir: %v", err)
	}

	cfg := &proxyconfig.Config{
		SDKConfig: sdkconfig.SDKConfig{
			APIKeys: []string{"test-key"},
		},
		Port:                   0,
		AuthDir:                authDir,
		Debug:                  true,
		LoggingToFile:          false,
		UsageStatisticsEnabled: false,
	}

	authManager := auth.NewManager(nil, nil, nil)
	accessManager := sdkaccess.NewManager()

	configPath := filepath.Join(tmpDir, "config.yaml")
	return NewServer(cfg, authManager, accessManager, configPath)
}

func TestHealthz(t *testing.T) {
	server := newTestServer(t)

	t.Run("GET", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		rr := httptest.NewRecorder()
		server.engine.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("unexpected status code: got %d want %d; body=%s", rr.Code, http.StatusOK, rr.Body.String())
		}

		var resp struct {
			Status string `json:"status"`
		}
		if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to parse response JSON: %v; body=%s", err, rr.Body.String())
		}
		if resp.Status != "ok" {
			t.Fatalf("unexpected response status: got %q want %q", resp.Status, "ok")
		}
	})

	t.Run("HEAD", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodHead, "/healthz", nil)
		rr := httptest.NewRecorder()
		server.engine.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("unexpected status code: got %d want %d; body=%s", rr.Code, http.StatusOK, rr.Body.String())
		}
		if rr.Body.Len() != 0 {
			t.Fatalf("expected empty body for HEAD request, got %q", rr.Body.String())
		}
	})
}

func TestBackendAPIWhamUsage_LocalLoopbackReturnsComparableQuotaPayload(t *testing.T) {
	server := newTestServer(t)

	now := time.Now().UTC()
	registerComparable := func(id string, primaryUsed float64, secondaryUsed float64) {
		t.Helper()
		_, err := server.handlers.AuthManager.Register(context.Background(), &auth.Auth{
			ID:       id,
			Provider: "codex",
			Quota: auth.QuotaState{
				Comparable: &auth.ComparableQuotaSnapshot{
					Provider:  "codex",
					AccountID: id,
					PlanType:  "pro",
					UpdatedAt: now,
					Windows: map[string]auth.ComparableQuotaWindow{
						auth.ComparableQuotaWindowFiveHour: {
							ID:            auth.ComparableQuotaWindowFiveHour,
							UsedPercent:   primaryUsed,
							HasValue:      true,
							Available:     true,
							ResetAt:       now.Add(10 * time.Minute),
							WindowSeconds: 18_000,
						},
						auth.ComparableQuotaWindowWeekly: {
							ID:            auth.ComparableQuotaWindowWeekly,
							UsedPercent:   secondaryUsed,
							HasValue:      true,
							Available:     true,
							ResetAt:       now.Add(24 * time.Hour),
							WindowSeconds: 604_800,
						},
					},
				},
			},
		})
		if err != nil {
			t.Fatalf("Register(%s) error = %v", id, err)
		}
	}

	registerComparable("codex-a", 20, 50)
	registerComparable("codex-b", 60, 70)

	req := httptest.NewRequest(http.MethodGet, "/backend-api/wham/usage", nil)
	req.RemoteAddr = "127.0.0.1:4321"
	rr := httptest.NewRecorder()

	server.engine.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("unexpected status code: got %d want %d; body=%s", rr.Code, http.StatusOK, rr.Body.String())
	}

	var resp struct {
		PlanType  string `json:"plan_type"`
		RateLimit struct {
			Allowed       bool `json:"allowed"`
			LimitReached  bool `json:"limit_reached"`
			PrimaryWindow struct {
				UsedPercent        int `json:"used_percent"`
				LimitWindowSeconds int `json:"limit_window_seconds"`
			} `json:"primary_window"`
			SecondaryWindow struct {
				UsedPercent        int `json:"used_percent"`
				LimitWindowSeconds int `json:"limit_window_seconds"`
			} `json:"secondary_window"`
		} `json:"rate_limit"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response JSON: %v; body=%s", err, rr.Body.String())
	}

	if resp.PlanType != "pro" {
		t.Fatalf("plan_type = %q, want %q", resp.PlanType, "pro")
	}
	if !resp.RateLimit.Allowed {
		t.Fatalf("rate_limit.allowed = false, want true")
	}
	if resp.RateLimit.LimitReached {
		t.Fatalf("rate_limit.limit_reached = true, want false")
	}
	if resp.RateLimit.PrimaryWindow.UsedPercent != 40 {
		t.Fatalf("primary used_percent = %d, want %d", resp.RateLimit.PrimaryWindow.UsedPercent, 40)
	}
	if resp.RateLimit.PrimaryWindow.LimitWindowSeconds != 18_000 {
		t.Fatalf("primary limit_window_seconds = %d, want %d", resp.RateLimit.PrimaryWindow.LimitWindowSeconds, 18_000)
	}
	if resp.RateLimit.SecondaryWindow.UsedPercent != 60 {
		t.Fatalf("secondary used_percent = %d, want %d", resp.RateLimit.SecondaryWindow.UsedPercent, 60)
	}
	if resp.RateLimit.SecondaryWindow.LimitWindowSeconds != 604_800 {
		t.Fatalf("secondary limit_window_seconds = %d, want %d", resp.RateLimit.SecondaryWindow.LimitWindowSeconds, 604_800)
	}
}

func TestBackendAPIWhamUsage_RejectsNonLoopback(t *testing.T) {
	server := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/backend-api/wham/usage", nil)
	req.RemoteAddr = "192.0.2.10:4321"
	rr := httptest.NewRecorder()

	server.engine.ServeHTTP(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("unexpected status code: got %d want %d; body=%s", rr.Code, http.StatusForbidden, rr.Body.String())
	}
}

func TestAmpProviderModelRoutes(t *testing.T) {
	testCases := []struct {
		name         string
		path         string
		wantStatus   int
		wantContains string
	}{
		{
			name:         "openai root models",
			path:         "/api/provider/openai/models",
			wantStatus:   http.StatusOK,
			wantContains: `"object":"list"`,
		},
		{
			name:         "groq root models",
			path:         "/api/provider/groq/models",
			wantStatus:   http.StatusOK,
			wantContains: `"object":"list"`,
		},
		{
			name:         "openai models",
			path:         "/api/provider/openai/v1/models",
			wantStatus:   http.StatusOK,
			wantContains: `"object":"list"`,
		},
		{
			name:         "anthropic models",
			path:         "/api/provider/anthropic/v1/models",
			wantStatus:   http.StatusOK,
			wantContains: `"data"`,
		},
		{
			name:         "google models v1",
			path:         "/api/provider/google/v1/models",
			wantStatus:   http.StatusOK,
			wantContains: `"models"`,
		},
		{
			name:         "google models v1beta",
			path:         "/api/provider/google/v1beta/models",
			wantStatus:   http.StatusOK,
			wantContains: `"models"`,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			server := newTestServer(t)

			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			req.Header.Set("Authorization", "Bearer test-key")

			rr := httptest.NewRecorder()
			server.engine.ServeHTTP(rr, req)

			if rr.Code != tc.wantStatus {
				t.Fatalf("unexpected status code for %s: got %d want %d; body=%s", tc.path, rr.Code, tc.wantStatus, rr.Body.String())
			}
			if body := rr.Body.String(); !strings.Contains(body, tc.wantContains) {
				t.Fatalf("response body for %s missing %q: %s", tc.path, tc.wantContains, body)
			}
		})
	}
}

func TestDefaultRequestLoggerFactory_UsesResolvedLogDirectory(t *testing.T) {
	t.Setenv("WRITABLE_PATH", "")
	t.Setenv("writable_path", "")

	originalWD, errGetwd := os.Getwd()
	if errGetwd != nil {
		t.Fatalf("failed to get current working directory: %v", errGetwd)
	}

	tmpDir := t.TempDir()
	if errChdir := os.Chdir(tmpDir); errChdir != nil {
		t.Fatalf("failed to switch working directory: %v", errChdir)
	}
	defer func() {
		if errChdirBack := os.Chdir(originalWD); errChdirBack != nil {
			t.Fatalf("failed to restore working directory: %v", errChdirBack)
		}
	}()

	// Force ResolveLogDirectory to fallback to auth-dir/logs by making ./logs not a writable directory.
	if errWriteFile := os.WriteFile(filepath.Join(tmpDir, "logs"), []byte("not-a-directory"), 0o644); errWriteFile != nil {
		t.Fatalf("failed to create blocking logs file: %v", errWriteFile)
	}

	configDir := filepath.Join(tmpDir, "config")
	if errMkdirConfig := os.MkdirAll(configDir, 0o755); errMkdirConfig != nil {
		t.Fatalf("failed to create config dir: %v", errMkdirConfig)
	}
	configPath := filepath.Join(configDir, "config.yaml")

	authDir := filepath.Join(tmpDir, "auth")
	if errMkdirAuth := os.MkdirAll(authDir, 0o700); errMkdirAuth != nil {
		t.Fatalf("failed to create auth dir: %v", errMkdirAuth)
	}

	cfg := &proxyconfig.Config{
		SDKConfig: proxyconfig.SDKConfig{
			RequestLog: false,
		},
		AuthDir:           authDir,
		ErrorLogsMaxFiles: 10,
	}

	logger := defaultRequestLoggerFactory(cfg, configPath)
	fileLogger, ok := logger.(*internallogging.FileRequestLogger)
	if !ok {
		t.Fatalf("expected *FileRequestLogger, got %T", logger)
	}

	errLog := fileLogger.LogRequestWithOptions(
		"/v1/chat/completions",
		http.MethodPost,
		map[string][]string{"Content-Type": []string{"application/json"}},
		[]byte(`{"input":"hello"}`),
		http.StatusBadGateway,
		map[string][]string{"Content-Type": []string{"application/json"}},
		[]byte(`{"error":"upstream failure"}`),
		nil,
		nil,
		nil,
		nil,
		nil,
		true,
		"issue-1711",
		time.Now(),
		time.Now(),
	)
	if errLog != nil {
		t.Fatalf("failed to write forced error request log: %v", errLog)
	}

	authLogsDir := filepath.Join(authDir, "logs")
	authEntries, errReadAuthDir := os.ReadDir(authLogsDir)
	if errReadAuthDir != nil {
		t.Fatalf("failed to read auth logs dir %s: %v", authLogsDir, errReadAuthDir)
	}
	foundErrorLogInAuthDir := false
	for _, entry := range authEntries {
		if strings.HasPrefix(entry.Name(), "error-") && strings.HasSuffix(entry.Name(), ".log") {
			foundErrorLogInAuthDir = true
			break
		}
	}
	if !foundErrorLogInAuthDir {
		t.Fatalf("expected forced error log in auth fallback dir %s, got entries: %+v", authLogsDir, authEntries)
	}

	configLogsDir := filepath.Join(configDir, "logs")
	configEntries, errReadConfigDir := os.ReadDir(configLogsDir)
	if errReadConfigDir != nil && !os.IsNotExist(errReadConfigDir) {
		t.Fatalf("failed to inspect config logs dir %s: %v", configLogsDir, errReadConfigDir)
	}
	for _, entry := range configEntries {
		if strings.HasPrefix(entry.Name(), "error-") && strings.HasSuffix(entry.Name(), ".log") {
			t.Fatalf("unexpected forced error log in config dir %s", configLogsDir)
		}
	}
}
