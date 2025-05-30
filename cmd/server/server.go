package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/roman-mazur/architecture-practice-4-template/httptools"
	"github.com/roman-mazur/architecture-practice-4-template/signal"
)

const dbURL = "http://db:8081/db/"
const teamName = "pickmeshki"

var port = flag.Int("port", 8080, "server port")
const confResponseDelaySec = "CONF_RESPONSE_DELAY_SEC"
const confHealthFailure = "CONF_HEALTH_FAILURE"

func main() {
	h := new(http.ServeMux)

	h.HandleFunc("/health", func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("content-type", "text/plain")
		if failConfig := os.Getenv(confHealthFailure); failConfig == "true" {
			rw.WriteHeader(http.StatusInternalServerError)
			_, _ = rw.Write([]byte("FAILURE"))
		} else {
			rw.WriteHeader(http.StatusOK)
			_, _ = rw.Write([]byte("OK"))
		}
	})

	// Надсилання POST до db з поточною датою
	value := map[string]string{"value": time.Now().Format("2006-01-02")}
	jsonBody, _ := json.Marshal(value)
	resp, err := http.Post(dbURL+teamName, "application/json", strings.NewReader(string(jsonBody)))
	if err != nil || (resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK) {
		log.Printf("Failed to init value in DB: %v", err)
	} else {
		log.Printf("Initial value for '%s' saved.", teamName)
	}

	h.HandleFunc("/api/v1/some-data", func(rw http.ResponseWriter, r *http.Request) {
		respDelayString := os.Getenv(confResponseDelaySec)
		if delaySec, parseErr := strconv.Atoi(respDelayString); parseErr == nil && delaySec > 0 && delaySec < 300 {
			time.Sleep(time.Duration(delaySec) * time.Second)
		}

		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(rw, "missing key parameter", http.StatusBadRequest)
			return
		}

		resp, err := http.Get(dbURL + url.PathEscape(key))
		if err != nil {
			log.Printf("Error fetching from DB: %v", err)
			rw.WriteHeader(http.StatusInternalServerError)
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusNotFound {
			rw.WriteHeader(http.StatusNotFound)
			return
		}

		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(http.StatusOK)
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Printf("Error reading response body: %v", err)
			rw.WriteHeader(http.StatusInternalServerError)
			return
		}
		rw.Write(body)
	})

	server := httptools.CreateServer(*port, h)
	server.Start()
	signal.WaitForTerminationSignal()
}