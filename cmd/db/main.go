package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

	"architecture-practice-4-template/datastore"
)

var db *datastore.Db

func main() {
	db = datastore.NewDb()

	http.HandleFunc("/db/", handleDbRequest)
	fmt.Println("DB server running on :8081")
	log.Fatal(http.ListenAndServe(":8081", nil))
}

func handleDbRequest(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/db/")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}
	switch r.Method {
	case http.MethodGet:
		val, ok := db.Get(key)
		if !ok {
			http.NotFound(w, r)
			return
		}
		resp := map[string]interface{}{"key": key, "value": val}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	case http.MethodPost:
		var body map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		val, ok := body["value"]
		if !ok {
			http.Error(w, "missing value", http.StatusBadRequest)
			return
		}
		db.Put(key, val)
		w.WriteHeader(http.StatusCreated)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}
