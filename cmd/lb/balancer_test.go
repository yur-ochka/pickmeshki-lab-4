package main

import (
	"fmt"
	"testing"
)

func TestChooseServer_ConsistentHashing(t *testing.T) {
	addr := "192.168.1.100:12345"
	server1 := chooseServer(addr)
	server2 := chooseServer(addr)

	if server1 != server2 {
		t.Errorf("Expected consistent server choice, got %s and %s", server1, server2)
	}
}

func TestChooseServer_Distribution(t *testing.T) {
	hits := make(map[string]int)
	for i := 0; i < 1000; i++ {
		addr := "10.0.0." + fmt.Sprintf("%d", i%255)
		server := chooseServer(addr)
		hits[server]++
	}
	if len(hits) < 2 {
		t.Errorf("Expected distribution across servers, got only one server")
	}
}
