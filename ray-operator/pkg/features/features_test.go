package features

import "testing"

func TestFederationRequiresStatusConditions(t *testing.T) {
	for _, federation := range []bool{false, true} {
		for _, status := range []bool{false, true} {
			SetFeatureGateDuringTest(t, RayFederation, federation)
			SetFeatureGateDuringTest(t, RayClusterStatusConditions, status)
			if err := ValidateDependencies(); (err != nil) != (federation && !status) {
				t.Fatalf("federation=%v status=%v: %v", federation, status, err)
			}
		}
	}
}
