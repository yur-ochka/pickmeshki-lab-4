package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync" // Потрібен для синхронізації доступу до map
)

type KeyValueStore interface {
	Get(key string) (string, error)
	Put(key string, value string) error
}

type InMemoryDb struct {
	mu   sync.RWMutex 
	data map[string]string
}

func NewInMemoryDb() *InMemoryDb {
	return &InMemoryDb{
		data: make(map[string]string),
	}
}

func (imdb *InMemoryDb) Get(key string) (string, error) {
	imdb.mu.RLock()
	defer imdb.mu.RUnlock()
	value, ok := imdb.data[key]
	if !ok {
		return "", fmt.Errorf("key '%s' not found", key)
	}
	return value, nil
}


func (imdb *InMemoryDb) Put(key string, value string) error {
	imdb.mu.Lock() 
	defer imdb.mu.Unlock()
	imdb.data[key] = value
	return nil
}

var db KeyValueStore 

func main() {

	db = NewInMemoryDb()
	log.Println("Initialized in-memory DB successfully.")

	http.HandleFunc("/db/", handleDbRequest)
	fmt.Println("DB server (in-memory) running on :8081")
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
		val, err := db.Get(key)
		if err != nil {
			http.NotFound(w, r)
			log.Printf("Failed to get key '%s': %v", key, err)
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
		strVal, ok := val.(string)
		if !ok {
			http.Error(w, "value must be a string", http.StatusBadRequest)
			return
		}
		if err := db.Put(key, strVal); err != nil {
			http.Error(w, "failed to write value", http.StatusInternalServerError)
			log.Printf("Failed to put key '%s': %v", key, err) 
			return
		}
		w.WriteHeader(http.StatusCreated)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}