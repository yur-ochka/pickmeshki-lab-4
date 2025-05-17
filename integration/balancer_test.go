package integration

import (
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"
)

const baseAddress = "http://balancer:8090"

var client = http.Client{
	Timeout: 3 * time.Second,
}

func TestBalancer(t *testing.T) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		t.Skip("Integration test is not enabled")
	}

	const requestsCount = 10
	seen := make(map[string]bool)

	for i := 0; i < requestsCount; i++ {
		resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
		if err != nil {
			t.Fatalf("Request %d failed: %v", i+1, err)
		}
		defer resp.Body.Close()

		server := resp.Header.Get("lb-from")
		if server == "" {
			t.Errorf("Missing lb-from header in response %d", i+1)
		} else {
			t.Logf("Response %d from [%s]", i+1, server)
			seen[server] = true
		}
	}

	if len(seen) < 2 {
		t.Errorf("Expected responses from at least 2 different servers, but got: %v", seen)
	}
}

func BenchmarkBalancer(b *testing.B) {
	for i := 0; i < b.N; i++ {
		resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
		if err != nil {
			b.Fatalf("Request failed: %v", err)
		}
		resp.Body.Close()
	}
}
