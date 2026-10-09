package internal

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// fakeS3 is a tiny path-style S3 endpoint: /<bucket>/<key>.
type fakeS3 struct {
	mu      sync.Mutex
	objects map[string]bool // "bucket/key"
	deleted []string
	// headStatus, if non-zero, overrides every HEAD response.
	headStatus int
}

func newFakeS3(t *testing.T) (*fakeS3, *StorageChecker) {
	t.Helper()
	f := &fakeS3{objects: map[string]bool{}}
	srv := httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(srv.Close)

	t.Setenv("MINIO_ENDPOINT", srv.URL)
	sc, err := NewStorageChecker(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return f, sc
}

func (f *fakeS3) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	path := r.URL.Path[1:]
	switch r.Method {
	case http.MethodHead:
		if f.headStatus != 0 {
			w.WriteHeader(f.headStatus)
			return
		}
		if f.objects[path] {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	case http.MethodDelete:
		f.deleted = append(f.deleted, path)
		delete(f.objects, path)
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func TestMarkerExists(t *testing.T) {
	f, sc := newFakeS3(t)
	f.objects["test-bucket/stage1/_SUCCESS"] = true

	got, err := sc.MarkerExists(context.Background(), "s3://test-bucket/stage1/_SUCCESS")
	if err != nil || !got {
		t.Errorf("existing marker: got (%v, %v), want (true, nil)", got, err)
	}

	got, err = sc.MarkerExists(context.Background(), "s3://test-bucket/stage2/_SUCCESS")
	if err != nil || got {
		t.Errorf("missing marker: got (%v, %v), want (false, nil)", got, err)
	}
}

func TestMarkerExists_ErrorsAreNotTreatedAsMissing(t *testing.T) {
	f, sc := newFakeS3(t)
	f.headStatus = http.StatusForbidden // e.g. wrong credentials

	got, err := sc.MarkerExists(context.Background(), "s3://test-bucket/stage1/_SUCCESS")
	if err == nil {
		t.Fatalf("403 from storage: got (%v, nil), want an error", got)
	}
}

func TestClearMarker(t *testing.T) {
	f, sc := newFakeS3(t)
	f.objects["test-bucket/old/_SUCCESS"] = true

	if err := sc.ClearMarker(context.Background(), "s3://test-bucket/old/_SUCCESS"); err != nil {
		t.Fatal(err)
	}
	if len(f.deleted) != 1 || f.deleted[0] != "test-bucket/old/_SUCCESS" {
		t.Errorf("deleted = %v, want [test-bucket/old/_SUCCESS]", f.deleted)
	}
}

func TestEndpointFromEnv(t *testing.T) {
	// Covered implicitly by newFakeS3 (MINIO_ENDPOINT points at the test server);
	// here we just check the default is still localhost:9000.
	t.Setenv("MINIO_ENDPOINT", "")
	if got := getEnv("MINIO_ENDPOINT", "http://localhost:9000"); got != "http://localhost:9000" {
		t.Errorf("default endpoint = %q", got)
	}
}

func TestParseS3Path(t *testing.T) {
	tests := []struct {
		in      string
		bucket  string
		key     string
		wantErr bool
	}{
		{"s3://test-bucket/stage1/_SUCCESS", "test-bucket", "stage1/_SUCCESS", false},
		{"s3://other/a", "other", "a", false},
		{"s3://bucket-only", "", "", true},
		{"s3://bucket/", "", "", true},
		{"s3:///key", "", "", true},
		{"test-bucket/key", "", "", true},
		{"", "", "", true},
	}
	for _, tt := range tests {
		b, k, err := parseS3Path(tt.in)
		if (err != nil) != tt.wantErr || b != tt.bucket || k != tt.key {
			t.Errorf("parseS3Path(%q) = (%q, %q, %v), want (%q, %q, err=%v)", tt.in, b, k, err, tt.bucket, tt.key, tt.wantErr)
		}
	}
}
