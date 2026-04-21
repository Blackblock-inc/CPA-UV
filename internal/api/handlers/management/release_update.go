package management

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/branding"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/buildinfo"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/managementasset"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/util"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	log "github.com/sirupsen/logrus"
)

const (
	updateReleaseUserAgent   = "CPA-UV-updater"
	maxReleaseDownloadSize   = 128 << 20
	selfExitDelay            = 1500 * time.Millisecond
	updateInstallScriptName  = "install-update"
	updateArchiveWindowsExt  = ".zip"
	updateArchiveUnixExt     = ".tar.gz"
	updateExecutableBaseName = "cli-proxy-api"
)

type releaseAsset struct {
	Name               string `json:"name"`
	BrowserDownloadURL string `json:"browser_download_url"`
	Digest             string `json:"digest"`
}

type githubReleaseInfo struct {
	TagName string         `json:"tag_name"`
	Name    string         `json:"name"`
	HTMLURL string         `json:"html_url"`
	Assets  []releaseAsset `json:"assets"`
}

type versionSnapshot struct {
	RawVersion      string `json:"raw-version"`
	DisplayVersion  string `json:"display-version"`
	BaselineVersion string `json:"baseline-version,omitempty"`
	UVVersion       string `json:"uv-version,omitempty"`
}

type latestVersionResponse struct {
	Repository       string          `json:"repository"`
	ReleasePage      string          `json:"release-page"`
	ManagementSource string          `json:"management-source"`
	InstallSupported bool            `json:"install-supported"`
	UpdateAvailable  bool            `json:"update-available"`
	InstallNote      string          `json:"install-note,omitempty"`
	AssetName        string          `json:"asset-name,omitempty"`
	Current          versionSnapshot `json:"current"`
	Latest           versionSnapshot `json:"latest"`
	CurrentVersion   string          `json:"current-version"`
	LatestVersion    string          `json:"latest-version"`
}

type preparedUpdate struct {
	workDir              string
	executablePath       string
	replacementExecPath  string
	replacementPanelPath string
	panelTargetPath      string
	argsFilePath         string
	workingDirectory     string
}

func (h *Handler) GetLatestVersion(c *gin.Context) {
	response, err := h.buildLatestVersionResponse(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": "version_check_failed", "message": err.Error()})
		return
	}
	c.JSON(http.StatusOK, response)
}

func (h *Handler) PostInstallUpdate(c *gin.Context) {
	if !h.beginUpdateInstall() {
		c.JSON(http.StatusConflict, gin.H{"error": "update_in_progress", "message": "an update installation is already running"})
		return
	}

	response, release, archiveAsset, err := h.buildLatestVersionResponseForInstall(c.Request.Context())
	if err != nil {
		h.finishUpdateInstall()
		c.JSON(http.StatusBadGateway, gin.H{"error": "version_check_failed", "message": err.Error()})
		return
	}
	if !response.UpdateAvailable {
		h.finishUpdateInstall()
		c.JSON(http.StatusOK, gin.H{
			"status":          "already-latest",
			"current-version": response.CurrentVersion,
			"latest-version":  response.LatestVersion,
		})
		return
	}
	if archiveAsset == nil {
		h.finishUpdateInstall()
		c.JSON(http.StatusNotImplemented, gin.H{
			"error":          "install_unsupported",
			"message":        response.InstallNote,
			"latest-version": response.LatestVersion,
		})
		return
	}

	prepared, err := h.prepareUpdate(c.Request.Context(), release, archiveAsset)
	if err != nil {
		h.finishUpdateInstall()
		c.JSON(http.StatusBadGateway, gin.H{"error": "prepare_update_failed", "message": err.Error()})
		return
	}

	if err = launchInstaller(prepared); err != nil {
		h.finishUpdateInstall()
		c.JSON(http.StatusInternalServerError, gin.H{"error": "launch_install_failed", "message": err.Error()})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{
		"status":           "installing",
		"latest-version":   response.LatestVersion,
		"current-version":  response.CurrentVersion,
		"release-page":     response.ReleasePage,
		"repository":       response.Repository,
		"restart-required": true,
	})

	go func() {
		time.Sleep(selfExitDelay)
		os.Exit(0)
	}()
}

func (h *Handler) buildLatestVersionResponse(ctx context.Context) (*latestVersionResponse, error) {
	response, _, _, err := h.buildLatestVersionResponseForInstall(ctx)
	return response, err
}

func (h *Handler) buildLatestVersionResponseForInstall(ctx context.Context) (*latestVersionResponse, *githubReleaseInfo, *releaseAsset, error) {
	release, repoURL, releasePage, err := h.fetchLatestReleaseInfo(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	current := buildVersionSnapshot(buildinfo.RawVersion)
	latestRaw := strings.TrimSpace(release.TagName)
	if latestRaw == "" {
		latestRaw = strings.TrimSpace(release.Name)
	}
	if latestRaw == "" {
		return nil, nil, nil, fmt.Errorf("missing release version")
	}

	latest := buildVersionSnapshot(latestRaw)
	archiveAsset, installNote := selectReleaseArchive(release.Assets)
	if archiveAsset == nil && installNote == "" {
		installNote = fmt.Sprintf("no update package found for %s/%s", runtime.GOOS, runtime.GOARCH)
	}
	response := &latestVersionResponse{
		Repository:       repoURL,
		ReleasePage:      firstNonEmpty(strings.TrimSpace(release.HTMLURL), releasePage),
		ManagementSource: managementasset.ResolveManagementSourceURL(repoURL),
		InstallSupported: archiveAsset != nil,
		UpdateAvailable:  branding.CompareVersions(latest.RawVersion, current.RawVersion) > 0,
		InstallNote:      installNote,
		AssetName:        assetName(archiveAsset),
		Current:          current,
		Latest:           latest,
		CurrentVersion:   current.DisplayVersion,
		LatestVersion:    latest.DisplayVersion,
	}
	return response, release, archiveAsset, nil
}

func buildVersionSnapshot(raw string) versionSnapshot {
	info := branding.NormalizeVersion(raw)
	return versionSnapshot{
		RawVersion:      strings.TrimSpace(raw),
		DisplayVersion:  info.Display,
		BaselineVersion: info.BaselineVersion,
		UVVersion:       info.UVVersion,
	}
}

func (h *Handler) fetchLatestReleaseInfo(ctx context.Context) (*githubReleaseInfo, string, string, error) {
	repoURL := h.panelRepositoryURL()
	releaseURL := managementasset.ResolveReleaseURL(repoURL)
	releasePage := managementasset.ResolveLatestReleasePageURL(repoURL)
	client := newUpdateHTTPClient(h.proxyURL())

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, releaseURL, nil)
	if err != nil {
		return nil, "", "", fmt.Errorf("create release request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", updateReleaseUserAgent)

	resp, err := client.Do(req)
	if err != nil {
		return nil, "", "", fmt.Errorf("request latest release: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, "", "", fmt.Errorf("unexpected release status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var release githubReleaseInfo
	if err = json.NewDecoder(resp.Body).Decode(&release); err != nil {
		return nil, "", "", fmt.Errorf("decode release response: %w", err)
	}

	return &release, repoURL, releasePage, nil
}

func (h *Handler) prepareUpdate(ctx context.Context, release *githubReleaseInfo, archiveAsset *releaseAsset) (*preparedUpdate, error) {
	if archiveAsset == nil {
		return nil, fmt.Errorf("missing release archive")
	}

	executablePath, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("resolve current executable: %w", err)
	}
	executablePath, err = filepath.Abs(executablePath)
	if err != nil {
		return nil, fmt.Errorf("resolve executable absolute path: %w", err)
	}

	workingDirectory := filepath.Dir(executablePath)
	if wd, errWD := os.Getwd(); errWD == nil && strings.TrimSpace(wd) != "" {
		workingDirectory = wd
	}

	workDir, err := os.MkdirTemp("", "cpa-uv-update-*")
	if err != nil {
		return nil, fmt.Errorf("create update workspace: %w", err)
	}

	client := newUpdateHTTPClient(h.proxyURL())
	archiveData, archiveHash, err := downloadReleaseAsset(ctx, client, *archiveAsset)
	if err != nil {
		return nil, err
	}
	replacementExecPath := filepath.Join(workDir, expectedExecutableFileName())
	if err = extractExecutableFromArchive(archiveData, replacementExecPath); err != nil {
		return nil, err
	}
	if err = os.Chmod(replacementExecPath, 0o755); err != nil && runtime.GOOS != "windows" {
		return nil, fmt.Errorf("mark replacement executable executable: %w", err)
	}

	var replacementPanelPath string
	panelAsset := selectManagementAsset(release.Assets)
	if panelAsset != nil {
		panelData, _, errDownload := downloadReleaseAsset(ctx, client, *panelAsset)
		if errDownload != nil {
			log.WithError(errDownload).Warn("failed to download management panel asset for update install")
		} else {
			replacementPanelPath = filepath.Join(workDir, managementasset.ManagementFileName)
			if errWrite := os.WriteFile(replacementPanelPath, panelData, 0o644); errWrite != nil {
				log.WithError(errWrite).Warn("failed to stage management panel asset for update install")
				replacementPanelPath = ""
			}
		}
	}

	argsFilePath := filepath.Join(workDir, "args.txt")
	if err = writeArgsFile(argsFilePath, os.Args[1:]); err != nil {
		return nil, err
	}

	log.Infof(
		"prepared CPA-UV update installation for %s (release=%s asset=%s hash=%s)",
		executablePath,
		release.TagName,
		archiveAsset.Name,
		archiveHash,
	)

	return &preparedUpdate{
		workDir:              workDir,
		executablePath:       executablePath,
		replacementExecPath:  replacementExecPath,
		replacementPanelPath: replacementPanelPath,
		panelTargetPath:      managementasset.FilePath(h.configFilePath),
		argsFilePath:         argsFilePath,
		workingDirectory:     workingDirectory,
	}, nil
}

func launchInstaller(prepared *preparedUpdate) error {
	if prepared == nil {
		return fmt.Errorf("missing prepared update")
	}
	if runtime.GOOS == "windows" {
		return launchWindowsInstaller(prepared)
	}
	return launchUnixInstaller(prepared)
}

func launchWindowsInstaller(prepared *preparedUpdate) error {
	scriptPath := filepath.Join(prepared.workDir, updateInstallScriptName+".ps1")
	script := `$ErrorActionPreference = 'Stop'
param(
  [int]$ParentPid,
  [string]$ExecutablePath,
  [string]$ReplacementPath,
  [string]$PanelPath,
  [string]$PanelTarget,
  [string]$ArgsPath,
  [string]$WorkingDirectory
)

for ($i = 0; $i -lt 240; $i++) {
  if (-not (Get-Process -Id $ParentPid -ErrorAction SilentlyContinue)) {
    break
  }
  Start-Sleep -Milliseconds 500
}

$backupPath = "$ExecutablePath.old"
if (Test-Path -LiteralPath $backupPath) {
  Remove-Item -LiteralPath $backupPath -Force -ErrorAction SilentlyContinue
}
if (Test-Path -LiteralPath $ExecutablePath) {
  Move-Item -LiteralPath $ExecutablePath -Destination $backupPath -Force
}
Copy-Item -LiteralPath $ReplacementPath -Destination $ExecutablePath -Force

if ($PanelPath -and $PanelTarget) {
  $panelDir = Split-Path -Parent $PanelTarget
  if ($panelDir) {
    New-Item -ItemType Directory -Force -Path $panelDir | Out-Null
  }
  Copy-Item -LiteralPath $PanelPath -Destination $PanelTarget -Force
}

$argList = @()
if ($ArgsPath -and (Test-Path -LiteralPath $ArgsPath)) {
  $argList = @(Get-Content -LiteralPath $ArgsPath)
}

Start-Process -FilePath $ExecutablePath -WorkingDirectory $WorkingDirectory -ArgumentList $argList -WindowStyle Hidden | Out-Null

if (Test-Path -LiteralPath $backupPath) {
  Remove-Item -LiteralPath $backupPath -Force -ErrorAction SilentlyContinue
}
`
	if err := os.WriteFile(scriptPath, []byte(script), 0o600); err != nil {
		return fmt.Errorf("write windows install script: %w", err)
	}

	cmd := exec.Command(
		"powershell",
		"-NoProfile",
		"-ExecutionPolicy",
		"Bypass",
		"-WindowStyle",
		"Hidden",
		"-File",
		scriptPath,
		"-ParentPid", fmt.Sprintf("%d", os.Getpid()),
		"-ExecutablePath", prepared.executablePath,
		"-ReplacementPath", prepared.replacementExecPath,
		"-PanelPath", prepared.replacementPanelPath,
		"-PanelTarget", prepared.panelTargetPath,
		"-ArgsPath", prepared.argsFilePath,
		"-WorkingDirectory", prepared.workingDirectory,
	)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	return cmd.Start()
}

func launchUnixInstaller(prepared *preparedUpdate) error {
	scriptPath := filepath.Join(prepared.workDir, updateInstallScriptName+".sh")
	script := `#!/bin/sh
set -eu

parent_pid="$1"
executable_path="$2"
replacement_path="$3"
panel_path="$4"
panel_target="$5"
args_path="$6"
working_directory="$7"

while kill -0 "$parent_pid" 2>/dev/null; do
  sleep 1
done

cp "$replacement_path" "$executable_path"
chmod +x "$executable_path"

if [ -n "$panel_path" ] && [ -n "$panel_target" ]; then
  mkdir -p "$(dirname "$panel_target")"
  cp "$panel_path" "$panel_target"
fi

set --
if [ -f "$args_path" ]; then
  while IFS= read -r line; do
    set -- "$@" "$line"
  done < "$args_path"
fi

cd "$working_directory"
nohup "$executable_path" "$@" >/dev/null 2>&1 &
`
	if err := os.WriteFile(scriptPath, []byte(script), 0o700); err != nil {
		return fmt.Errorf("write unix install script: %w", err)
	}

	cmd := exec.Command(
		"/bin/sh",
		scriptPath,
		fmt.Sprintf("%d", os.Getpid()),
		prepared.executablePath,
		prepared.replacementExecPath,
		prepared.replacementPanelPath,
		prepared.panelTargetPath,
		prepared.argsFilePath,
		prepared.workingDirectory,
	)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	return cmd.Start()
}

func newUpdateHTTPClient(proxyURL string) *http.Client {
	client := &http.Client{Timeout: 30 * time.Second}
	sdkCfg := &sdkconfig.SDKConfig{ProxyURL: strings.TrimSpace(proxyURL)}
	util.SetProxy(sdkCfg, client)
	return client
}

func downloadReleaseAsset(ctx context.Context, client *http.Client, asset releaseAsset) ([]byte, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, asset.BrowserDownloadURL, nil)
	if err != nil {
		return nil, "", fmt.Errorf("create download request for %s: %w", asset.Name, err)
	}
	req.Header.Set("User-Agent", updateReleaseUserAgent)

	resp, err := client.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("download %s: %w", asset.Name, err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, "", fmt.Errorf("unexpected download status %d for %s: %s", resp.StatusCode, asset.Name, strings.TrimSpace(string(body)))
	}

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxReleaseDownloadSize+1))
	if err != nil {
		return nil, "", fmt.Errorf("read %s: %w", asset.Name, err)
	}
	if int64(len(data)) > maxReleaseDownloadSize {
		return nil, "", fmt.Errorf("asset %s exceeds maximum size of %d bytes", asset.Name, maxReleaseDownloadSize)
	}

	sum := sha256.Sum256(data)
	hash := hex.EncodeToString(sum[:])
	expectedHash := normalizeDigest(asset.Digest)
	if expectedHash != "" && !strings.EqualFold(expectedHash, hash) {
		return nil, "", fmt.Errorf("digest mismatch for %s: expected %s got %s", asset.Name, expectedHash, hash)
	}

	return data, hash, nil
}

func extractExecutableFromArchive(data []byte, destinationPath string) error {
	if runtime.GOOS == "windows" {
		return extractExecutableFromZip(data, destinationPath)
	}
	return extractExecutableFromTarGz(data, destinationPath)
}

func extractExecutableFromZip(data []byte, destinationPath string) error {
	reader, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return fmt.Errorf("open zip archive: %w", err)
	}

	expectedName := strings.ToLower(expectedExecutableFileName())
	for _, file := range reader.File {
		if strings.ToLower(filepath.Base(file.Name)) != expectedName {
			continue
		}
		rc, errOpen := file.Open()
		if errOpen != nil {
			return fmt.Errorf("open %s in zip: %w", file.Name, errOpen)
		}
		defer func() {
			_ = rc.Close()
		}()
		content, errRead := io.ReadAll(io.LimitReader(rc, maxReleaseDownloadSize+1))
		if errRead != nil {
			return fmt.Errorf("read %s in zip: %w", file.Name, errRead)
		}
		if int64(len(content)) > maxReleaseDownloadSize {
			return fmt.Errorf("archive entry %s exceeds maximum size", file.Name)
		}
		if errWrite := os.WriteFile(destinationPath, content, 0o755); errWrite != nil {
			return fmt.Errorf("write extracted executable: %w", errWrite)
		}
		return nil
	}

	return fmt.Errorf("executable %s not found in release archive", expectedExecutableFileName())
}

func extractExecutableFromTarGz(data []byte, destinationPath string) error {
	gzipReader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("open tar.gz archive: %w", err)
	}
	defer func() {
		_ = gzipReader.Close()
	}()

	tarReader := tar.NewReader(gzipReader)
	expectedName := expectedExecutableFileName()

	for {
		header, errNext := tarReader.Next()
		if errNext == io.EOF {
			break
		}
		if errNext != nil {
			return fmt.Errorf("read tar.gz archive: %w", errNext)
		}
		if header == nil || header.Typeflag != tar.TypeReg {
			continue
		}
		if filepath.Base(header.Name) != expectedName {
			continue
		}
		content, errRead := io.ReadAll(io.LimitReader(tarReader, maxReleaseDownloadSize+1))
		if errRead != nil {
			return fmt.Errorf("read %s in tar.gz: %w", header.Name, errRead)
		}
		if int64(len(content)) > maxReleaseDownloadSize {
			return fmt.Errorf("archive entry %s exceeds maximum size", header.Name)
		}
		if errWrite := os.WriteFile(destinationPath, content, 0o755); errWrite != nil {
			return fmt.Errorf("write extracted executable: %w", errWrite)
		}
		return nil
	}

	return fmt.Errorf("executable %s not found in release archive", expectedName)
}

func selectReleaseArchive(assets []releaseAsset) (*releaseAsset, string) {
	needle := "_" + strings.ToLower(runtime.GOOS) + "_" + strings.ToLower(runtime.GOARCH)
	expectedExt := updateArchiveUnixExt
	if runtime.GOOS == "windows" {
		expectedExt = updateArchiveWindowsExt
	}

	for i := range assets {
		name := strings.ToLower(strings.TrimSpace(assets[i].Name))
		if strings.Contains(name, needle) && strings.HasSuffix(name, expectedExt) {
			return &assets[i], ""
		}
	}

	return nil, fmt.Sprintf("no release package found for %s/%s", runtime.GOOS, runtime.GOARCH)
}

func selectManagementAsset(assets []releaseAsset) *releaseAsset {
	for i := range assets {
		if strings.EqualFold(strings.TrimSpace(assets[i].Name), managementasset.ManagementFileName) {
			return &assets[i]
		}
	}
	return nil
}

func writeArgsFile(path string, args []string) error {
	content := strings.Join(args, "\n")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		return fmt.Errorf("write update args file: %w", err)
	}
	return nil
}

func normalizeDigest(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if idx := strings.Index(value, ":"); idx >= 0 {
		value = value[idx+1:]
	}
	return strings.ToLower(strings.TrimSpace(value))
}

func expectedExecutableFileName() string {
	if runtime.GOOS == "windows" {
		return updateExecutableBaseName + ".exe"
	}
	return updateExecutableBaseName
}

func (h *Handler) proxyURL() string {
	if h == nil || h.cfg == nil {
		return ""
	}
	return strings.TrimSpace(h.cfg.ProxyURL)
}

func (h *Handler) panelRepositoryURL() string {
	if h == nil || h.cfg == nil {
		return branding.RepoURL
	}
	repo := strings.TrimSpace(h.cfg.RemoteManagement.PanelGitHubRepository)
	if repo == "" {
		return branding.RepoURL
	}
	return managementasset.ResolveRepositoryURL(repo)
}

func (h *Handler) beginUpdateInstall() bool {
	h.updateMu.Lock()
	defer h.updateMu.Unlock()
	if h.updateInProgress {
		return false
	}
	h.updateInProgress = true
	return true
}

func (h *Handler) finishUpdateInstall() {
	h.updateMu.Lock()
	h.updateInProgress = false
	h.updateMu.Unlock()
}

func assetName(asset *releaseAsset) string {
	if asset == nil {
		return ""
	}
	return asset.Name
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
