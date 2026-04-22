package management

import "testing"

func TestExtractManagementVersionFromHTML(t *testing.T) {
	t.Parallel()

	html := []byte(`<html><body><script>const VERSION="1.7.41-UV (2.0.0)"</script></body></html>`)
	if got := extractManagementVersionFromHTML(html); got != "1.7.41-UV (2.0.0)" {
		t.Fatalf("extractManagementVersionFromHTML() = %q, want %q", got, "1.7.41-UV (2.0.0)")
	}
}

func TestBuildManagementVersionSnapshot(t *testing.T) {
	t.Parallel()

	got := buildManagementVersionSnapshot("1.7.41-UV (2.0.0)")
	if got.DisplayVersion != "1.7.41-UV (2.0.0)" {
		t.Fatalf("DisplayVersion = %q, want %q", got.DisplayVersion, "1.7.41-UV (2.0.0)")
	}
	if got.BaselineVersion != "1.7.41" {
		t.Fatalf("BaselineVersion = %q, want %q", got.BaselineVersion, "1.7.41")
	}
	if got.UVVersion != "2.0.0" {
		t.Fatalf("UVVersion = %q, want %q", got.UVVersion, "2.0.0")
	}
}
