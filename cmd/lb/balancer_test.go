package main

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestChooseServer_ConsistentHashing(t *testing.T) {
	addr := "192.168.1.100:12345"
	server1 := chooseServer(addr)
	server2 := chooseServer(addr)

	assert.Equal(t, server1, server2, "Same address should map to the same server")
}

func TestChooseServer_Distribution(t *testing.T) {
	hits := make(map[string]int)
	for i := 0; i < 1000; i++ {
		addr := fmt.Sprintf("10.0.0.%d", i%255)
		server := chooseServer(addr)
		hits[server]++
	}

	assert.GreaterOrEqual(t, len(hits), 2, "Expected requests to be distributed across at least 2 servers")
}