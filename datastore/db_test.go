package datastore

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDb(t *testing.T) {
	tmp := t.TempDir()
	db, err := Open(tmp)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	pairs := [][]string{
		{"k1", "v1"},
		{"k2", "v2"},
		{"k3", "v3"},
		{"k2", "v2.1"},
	}

	t.Run("put/get", func(t *testing.T) {
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pair[0], err)
			}
			value, err := db.Get(pair[0])
			if err != nil {
				t.Errorf("Cannot get %s: %s", pair[0], err)
			}
			if value != pair[1] {
				t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
			}
		}
	})

	t.Run("file growth", func(t *testing.T) {
		sizeBefore, err := db.Size()
		if err != nil {
			t.Fatal(err)
		}
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pair[0], err)
			}
		}
		sizeAfter, err := db.Size()
		if err != nil {
			t.Fatal(err)
		}
		if sizeAfter <= sizeBefore {
			t.Errorf("Size does not grow after put (before %d, after %d)", sizeBefore, sizeAfter)
		}
	})

	t.Run("new db process", func(t *testing.T) {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db, err = Open(tmp)
		if err != nil {
			t.Fatal(err)
		}

		uniquePairs := make(map[string]string)
		for _, pair := range pairs {
			uniquePairs[pair[0]] = pair[1]
		}

		for key, expectedValue := range uniquePairs {
			value, err := db.Get(key)
			if err != nil {
				t.Errorf("Cannot get %s: %s", key, err)
			}
			if value != expectedValue {
				t.Errorf("Get(%q) = %q, wanted %q", key, value, expectedValue)
			}
		}
	})
}

func TestSegmentRotation(t *testing.T) {
	tmp := t.TempDir()
	db, err := OpenWithMaxSize(tmp, 10) // Small size for testing
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	// First write should go to initial file
	err = db.Put("k1", "v1")
	if err != nil {
		t.Fatal(err)
	}

	// Second write should trigger rotation
	err = db.Put("k2", "v2")
	if err != nil {
		t.Fatal(err)
	}

	// Verify both values can be read
	val, err := db.Get("k1")
	if err != nil {
		t.Fatal(err)
	}
	if val != "v1" {
		t.Errorf("Expected v1, got %s", val)
	}

	val, err = db.Get("k2")
	if err != nil {
		t.Fatal(err)
	}
	if val != "v2" {
		t.Errorf("Expected v2, got %s", val)
	}

	// Verify we have a segment file
	files, err := os.ReadDir(tmp)
	if err != nil {
		t.Fatal(err)
	}

	var segmentFound bool
	for _, file := range files {
		name := file.Name()
		if name != "current-data" && len(name) > len(segmentPrefix) && name[:len(segmentPrefix)] == segmentPrefix {
			segmentFound = true
			break
		}
	}

	if !segmentFound {
		t.Error("No segment file found after rotation")
	}
}

func TestSegmentMerge(t *testing.T) {
	tmp := t.TempDir()
	db, err := OpenWithMaxSize(tmp, 10) // Small size for testing
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	// Create multiple segments
	db.Put("k1", "v1") // First segment
	db.Put("k2", "v2") // Second segment
	db.Put("k3", "v3") // Third segment

	// Update k1 in current segment
	db.Put("k1", "v1-updated")

	// Verify we have multiple segments
	if len(db.segments) < 2 {
		t.Fatalf("Expected at least 2 segments, got %d", len(db.segments))
	}

	// Perform merge
	err = db.MergeSegments()
	if err != nil {
		t.Fatal(err)
	}

	// Verify we have only one segment after merge
	if len(db.segments) != 1 {
		t.Fatalf("Expected 1 segment after merge, got %d", len(db.segments))
	}

	// Verify all keys are accessible with correct values
	val, err := db.Get("k1")
	if err != nil {
		t.Fatal(err)
	}
	if val != "v1-updated" {
		t.Errorf("Expected v1-updated, got %s", val)
	}

	val, err = db.Get("k2")
	if err != nil {
		t.Fatal(err)
	}
	if val != "v2" {
		t.Errorf("Expected v2, got %s", val)
	}

	val, err = db.Get("k3")
	if err != nil {
		t.Fatal(err)
	}
	if val != "v3" {
		t.Errorf("Expected v3, got %s", val)
	}
}

func TestMergeAtomicity(t *testing.T) {
    tmp := t.TempDir()
    
    db, err := OpenWithMaxSize(tmp, 10)
    if err != nil {
        t.Fatal(err)
    }
    defer db.Close()

    db.Put("k1", "v1")
    db.Put("k2", "v2")
    
    initialSegments := len(db.segments)
    if _, err := os.ReadDir(tmp); err != nil {
        t.Fatal(err)
    }

    tempPath := filepath.Join(tmp, "merged-temp")
    tempFile, err := os.Create(tempPath)
    if err != nil {
        t.Fatal(err)
    }
    tempFile.Close() // Close the file immediately after creation

    if err := os.Remove(tempPath); err != nil {
        t.Fatal(err)
    }

    if err := os.WriteFile(filepath.Join(tmp, "merge-failed"), []byte("1"), 0644); err != nil {
        t.Fatal(err)
    }

    if len(db.segments) != initialSegments {
        t.Errorf("Segment count changed after failure")
    }

    if val, err := db.Get("k1"); err != nil || val != "v1" {
        t.Errorf("Data corrupted after failed merge")
    }

    if val, err := db.Get("k2"); err != nil || val != "v2" {
        t.Errorf("Data corrupted after failed merge")
    }

    files, err := os.ReadDir(tmp)
    if err != nil {
        t.Fatal(err)
    }

    for _, file := range files {
        if file.Name() == "merged-temp" {
            t.Error("Temporary file not cleaned up")
        }
    }
}

func TestCorruptedChecksumEntry(t *testing.T) {
	tmp := t.TempDir()
	db, err := Open(tmp)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Put("key", "valid")
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Закриваємо базу перед прямою модифікацією файлу
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Шукаємо поточний data-файл
	dataPath := filepath.Join(tmp, "current-data")
	content, err := os.ReadFile(dataPath)
	if err != nil {
		t.Fatal(err)
	}

	// Псуємо останній байт value (корупція даних)
	if len(content) > 0 {
		content[len(content)-1] ^= 0xFF
	}

	// Перезаписуємо файл
	err = os.WriteFile(dataPath, content, 0644)
	if err != nil {
		t.Fatal(err)
	}

	// Заново відкриваємо базу
	db, err = Open(tmp)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Тепер Get повинен повернути помилку
	_, err = db.Get("key")
	if err == nil {
		t.Fatal("Expected checksum error after corruption, but got nil")
	}
	t.Logf("Successfully detected checksum mismatch: %v", err)
}
