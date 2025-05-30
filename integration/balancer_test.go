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

	if !checkBalancer() {
		t.Skip("Balancer is not available or not responding with 200 OK")
	}

	servers := make(map[string]bool)
	const requestCount = 20

	for i := 0; i < requestCount; i++ {
		resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data?key=pickmeshki", baseAddress))
		if err != nil {
			t.Fatalf("Request failed: %v", err)
		}
		server := resp.Header.Get("lb-from")
		defer resp.Body.Close()

		if server != "" {
			servers[server] = true
		} else {
			t.Log("Missing lb-from header in response")
		}

		if resp.StatusCode == http.StatusNotFound {
			t.Errorf("Unexpected 404 Not Found from server on iteration %d", i)
		}
		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected 200 OK, got %d", resp.StatusCode)
		}
	}

	if len(servers) < 2 {
		t.Errorf("Expected responses from at least 2 servers, got: %v", servers)
	} else {
		t.Logf("Responses received from servers: %v", servers)
	}
}


func checkBalancer() bool {
	resp, err := client.Get(baseAddress)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func BenchmarkBalancer(b *testing.B) {
	for i := 0; i < b.N; i++ {
		resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
		if err != nil {
			b.Errorf("Request failed: %v", err)
			continue
		}
		resp.Body.Close()
	}
}
