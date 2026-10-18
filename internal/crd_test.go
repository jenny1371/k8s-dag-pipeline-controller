package internal

import (
	"os"
	"regexp"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/yaml"
)

// crdPattern reads spec.<field>.pattern out of the CRD manifest.
func crdPattern(t *testing.T, field string) *regexp.Regexp {
	t.Helper()
	raw, err := os.ReadFile("../config/crd.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var crd map[string]interface{}
	if err := yaml.Unmarshal(raw, &crd); err != nil {
		t.Fatal(err)
	}

	node := crd
	path := []string{"spec", "versions", "#0", "schema", "openAPIV3Schema", "properties", "spec", "properties", field}
	var cur interface{} = node
	for _, p := range path {
		if p == "#0" {
			cur = cur.([]interface{})[0]
			continue
		}
		cur = cur.(map[string]interface{})[p]
	}
	pattern, ok := cur.(map[string]interface{})["pattern"].(string)
	if !ok {
		t.Fatalf("no pattern for %s in config/crd.yaml", field)
	}
	return regexp.MustCompile(pattern)
}

// The CRD pattern is a first line of defence; it must agree with what Kubernetes
// (and therefore validateSpec) actually accepts.
func TestCRDMemoryPatternMatchesKubernetesQuantities(t *testing.T) {
	re := crdPattern(t, "requestMemory")

	mustAccept := []string{"256Mi", "1Gi", "1.5Gi", "512Ki", "100", "128974848", "500k", "1M", "2G", "1T", "1P", "1E", "1Ei", "4Pi", "0.5Gi"}
	for _, v := range mustAccept {
		if !re.MatchString(v) {
			t.Errorf("CRD pattern rejects %q, which Kubernetes accepts", v)
		}
		if _, err := resource.ParseQuantity(v); err != nil {
			t.Errorf("test data bug: Kubernetes rejects %q: %v", v, err)
		}
	}

	// Anything the pattern lets through must really parse, otherwise the
	// controller fails the job later instead of the API rejecting it.
	candidates := []string{"1K", "1ki", "1mi", "1gi", "1g", "1m", "1Mib", "Gi", "", "-1Gi", "1 Gi", "1.Gi", ".5Gi", "1Gi\n", "1KI", "abc", "1e3", "1MB"}
	for _, v := range candidates {
		if re.MatchString(v) {
			if _, err := resource.ParseQuantity(v); err != nil {
				t.Errorf("CRD pattern accepts %q but Kubernetes rejects it: %v", v, err)
			}
		}
	}

	for _, v := range []string{"1K", "1ki", "Gi", "-1Gi"} {
		if re.MatchString(v) {
			t.Errorf("CRD pattern should reject %q", v)
		}
	}
}

func TestCRDCPUPatternMatchesKubernetesQuantities(t *testing.T) {
	re := crdPattern(t, "requestCPU")

	for _, v := range []string{"200m", "1", "0.5", "1500m", "4"} {
		if !re.MatchString(v) {
			t.Errorf("CRD pattern rejects %q, which Kubernetes accepts", v)
		}
	}
	for _, v := range []string{"lots", "1Gi", "-1", "m", "", "1 m"} {
		if re.MatchString(v) {
			if _, err := resource.ParseQuantity(v); err != nil {
				t.Errorf("CRD pattern accepts %q but Kubernetes rejects it", v)
			}
		}
	}
}
