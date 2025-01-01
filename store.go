package sqlite3kv

import (
	"context"
	"database/sql"
	"errors"
	"github.com/kvtools/valkeyrie"
	"github.com/kvtools/valkeyrie/store"
	_ "github.com/mattn/go-sqlite3"
	"path"
	"strings"
	"sync"
	"time"
)

const StoreName = "sqlite3kv"

func init() {
	valkeyrie.Register(StoreName, newStore)
}

type SQLiteStore struct {
	db      *sql.DB
	mu      sync.RWMutex
	watches map[string][]chan *store.KVPair
}

type Config struct{}

func newStore(ctx context.Context, endpoints []string, options valkeyrie.Config) (store.Store, error) {
	cfg, ok := options.(*Config)
	if !ok && options != nil {
		return nil, &store.InvalidConfigurationError{Store: StoreName, Config: options}
	}

	return New(ctx, endpoints, cfg)
}

func New(ctx context.Context, endpoints []string, options *Config) (*SQLiteStore, error) {
	if endpoints == nil || len(endpoints) == 0 {
		return nil, errors.New("database file path is required")
	}

	db, err := sql.Open("sqlite3", endpoints[0])
	if err != nil {
		return nil, err
	}

	// Create tables for directories and key-value store
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS directories (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			path TEXT UNIQUE NOT NULL
		);

		CREATE TABLE IF NOT EXISTS kv_store (
			key TEXT PRIMARY KEY,
			value BLOB,
			last_index INTEGER,
			directory_id INTEGER NOT NULL,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			expires_at DATETIME,
			FOREIGN KEY (directory_id) REFERENCES directories(id) ON DELETE CASCADE
		);

		CREATE INDEX IF NOT EXISTS idx_directory_id ON kv_store(directory_id);
	`)
	if err != nil {
		return nil, err
	}

	s := &SQLiteStore{
		db:      db,
		watches: make(map[string][]chan *store.KVPair),
	}
	s.startCleanupTask(ctx, 1*time.Minute)

	return s, nil
}

// extractDirectory splits a full key path into the directory and the key name
func extractDirectory(key string) (string, string) {
	dir, file := path.Split(key)
	return strings.TrimSuffix(dir, "/"), file
}

// ensureDirectory ensures the directory exists and returns its ID
func (s *SQLiteStore) ensureDirectory(ctx context.Context, dir string) (int64, error) {
	if dir != "" && !strings.HasSuffix(dir, "/") {
		dir += "/"
	}
	// Insert the directory if it doesn't exist and return its ID
	_, err := s.db.ExecContext(ctx, `
        INSERT OR IGNORE INTO directories (path)
        VALUES (?)
    `, dir)
	if err != nil {
		return 0, err
	}

	// Fetch the directory ID
	var id int64
	err = s.db.QueryRowContext(ctx, `
        SELECT id FROM directories WHERE path = ?
    `, dir).Scan(&id)
	return id, err
}

func (s *SQLiteStore) Put(ctx context.Context, key string, value []byte, opts *store.WriteOptions) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	dir, _ := extractDirectory(key)

	// Ensure the directory exists and get its ID
	dirID, err := s.ensureDirectory(ctx, dir)
	if err != nil {
		return err
	}

	// If opts.IsDir is set, ensure the directory exists
	if opts != nil && opts.IsDir {
		_, err := s.ensureDirectory(ctx, key)
		if err != nil {
			return err
		}
	}

	// Calculate expiration time if TTL is provided
	var expiresAt sql.NullTime
	if opts != nil && opts.TTL > 0 {
		expiresAt = sql.NullTime{
			Time:  time.Now().Add(opts.TTL),
			Valid: true,
		}
	}

	// Insert or replace the key with expiration time
	_, err = s.db.ExecContext(ctx, `
		INSERT OR REPLACE INTO kv_store (key, value, last_index, directory_id, expires_at)
		VALUES (?, ?, COALESCE((SELECT last_index FROM kv_store WHERE key = ?) + 1, 1), ?, ?)
	`, key, value, key, dirID, expiresAt)

	if err == nil {
		// Notify watchers of the change
		s.notify(key, value)
	}

	return err
}

func (s *SQLiteStore) Get(ctx context.Context, key string, opts *store.ReadOptions) (*store.KVPair, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var pair store.KVPair
	var expiresAt sql.NullTime

	// Fetch key and check expiration time
	err := s.db.QueryRowContext(ctx, `
        SELECT key, value, last_index, expires_at FROM kv_store WHERE key = ?
    `, key).Scan(&pair.Key, &pair.Value, &pair.LastIndex, &expiresAt)
	if err == sql.ErrNoRows {
		return nil, store.ErrKeyNotFound
	} else if err != nil {
		return nil, err
	}

	// Check if the key has expired
	if expiresAt.Valid && time.Now().After(expiresAt.Time) {
		// Delete the expired key
		_, _ = s.db.ExecContext(ctx, `DELETE FROM kv_store WHERE key = ?`, key)
		return nil, store.ErrKeyNotFound
	}

	return &pair, nil
}

func (s *SQLiteStore) List(ctx context.Context, directory string, opts *store.ReadOptions) ([]*store.KVPair, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Ensure the directory ends with "/"
	if directory != "" && !strings.HasSuffix(directory, "/") {
		directory += "/"
	}

	var dirID int64
	err := s.db.QueryRowContext(ctx, `
		SELECT id FROM directories WHERE path = ?
	`, directory).Scan(&dirID)
	if err == sql.ErrNoRows {
		ok, _ := s.Exists(ctx, directory[0:len(directory)-1], nil)
		if ok {
			return []*store.KVPair{}, nil
		}
		return nil, store.ErrKeyNotFound
	} else if err != nil {
		return nil, err
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT kv.key, kv.value, kv.last_index
		FROM kv_store kv
		INNER JOIN directories d ON kv.directory_id = d.id
		WHERE d.path LIKE ? || '%'
		ORDER BY kv.key
	`, directory)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var pairs []*store.KVPair
	for rows.Next() {
		pair := &store.KVPair{}
		if err := rows.Scan(&pair.Key, &pair.Value, &pair.LastIndex); err != nil {
			return nil, err
		}
		pairs = append(pairs, pair)
	}
	return pairs, rows.Err()
}

func (s *SQLiteStore) DeleteTree(ctx context.Context, directory string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if directory != "" && !strings.HasSuffix(directory, "/") {
		directory += "/"
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, `
		DELETE FROM kv_store 
		WHERE directory_id IN (
			SELECT id FROM directories WHERE path = ?
		)
	`, directory)
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.ExecContext(ctx, `
		DELETE FROM directories 
		WHERE path = ?
	`, directory)
	if err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

func (s *SQLiteStore) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, err := s.db.ExecContext(ctx, "DELETE FROM kv_store WHERE key = ?", key)
	if err == nil {
		s.notify(key, nil)
	}
	return err
}

func (s *SQLiteStore) notify(key string, value []byte) {
	pair := &store.KVPair{Key: key, Value: value}
	for _, ch := range s.watches[key] {
		select {
		case ch <- pair:
		default:
		}
	}
}

// Exists checks if a key exists in the key-value store
func (s *SQLiteStore) Exists(ctx context.Context, key string, opts *store.ReadOptions) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var exists bool
	err := s.db.QueryRowContext(ctx, `
        SELECT EXISTS(SELECT 1 FROM kv_store WHERE key = ?)
    `, key).Scan(&exists)
	return exists, err
}

// Watch creates a watch channel for a specific key
func (s *SQLiteStore) Watch(ctx context.Context, key string, opts *store.ReadOptions) (<-chan *store.KVPair, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ch := make(chan *store.KVPair, 1)
	if s.watches[key] == nil {
		s.watches[key] = []chan *store.KVPair{}
	}
	s.watches[key] = append(s.watches[key], ch)

	go func() {
		<-ctx.Done()
		s.mu.Lock()
		defer s.mu.Unlock()

		watchers := s.watches[key]
		for i, w := range watchers {
			if w == ch {
				s.watches[key] = append(watchers[:i], watchers[i+1:]...)
				break
			}
		}
		close(ch)
	}()

	// Fetch the current value of the key and send it to the watcher
	existingPair := &store.KVPair{}
	err := s.db.QueryRowContext(ctx, `
	SELECT value, last_index FROM kv_store WHERE key = ?
`, key).Scan(&existingPair.Value, &existingPair.LastIndex)
	if err == nil {
		// Notify watchers with the current value
		s.notify(key, existingPair.Value)
	}

	return ch, nil
}

// WatchTree creates a watch channel for all keys under a directory
func (s *SQLiteStore) WatchTree(ctx context.Context, directory string, opts *store.ReadOptions) (<-chan []*store.KVPair, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if directory != "" && !strings.HasSuffix(directory, "/") {
		directory += "/"
	}

	ch := make(chan []*store.KVPair, 1)

	go func() {
		for {
			select {
			case <-ctx.Done():
				close(ch)
				return
			default:
				// Send the updated list of key-value pairs under the directory
				pairs, err := s.List(ctx, directory, nil)
				if err == nil {
					ch <- pairs
				}
			}
		}
	}()

	return ch, nil
}

// NewLock creates a distributed lock for a given key
func (s *SQLiteStore) NewLock(ctx context.Context, key string, opts *store.LockOptions) (store.Locker, error) {
	return nil, errors.New("distributed locking is not yet supported for this store")
}

// AtomicPut performs an atomic put, ensuring that the current value matches the provided previous value
func (s *SQLiteStore) AtomicPut(ctx context.Context, key string, value []byte, previous *store.KVPair, opts *store.WriteOptions) (bool, *store.KVPair, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	dir, _ := extractDirectory(key)
	dirID, err := s.ensureDirectory(ctx, dir)
	if err != nil {
		return false, nil, err
	}

	var currentIndex uint64
	err = s.db.QueryRowContext(ctx, `
        SELECT last_index FROM kv_store WHERE key = ?
    `, key).Scan(&currentIndex)
	if err == sql.ErrNoRows && previous == nil {
		// Key does not exist, and it's a valid state for AtomicPut
		_, err := s.db.ExecContext(ctx, `
            INSERT INTO kv_store (key, value, last_index, directory_id)
            VALUES (?, ?, 1, ?)
        `, key, value, dirID)
		if err != nil {
			return false, nil, err
		}

		// Send notification for watchers
		s.notify(key, value)
		return true, &store.KVPair{Key: key, Value: value, LastIndex: 1}, nil
	} else if err != nil {
		return false, nil, err
	}

	// Check if the last index matches the expected one
	if previous != nil && previous.LastIndex != currentIndex {
		return false, nil, store.ErrKeyModified
	} else if previous == nil {
		return false, nil, store.ErrKeyExists
	}

	_, err = s.db.ExecContext(ctx, `
        UPDATE kv_store 
        SET value = ?, last_index = last_index + 1 
        WHERE key = ?
    `, value, key)
	if err != nil {
		return false, nil, err
	}

	// Send notification for watchers
	newIndex := currentIndex + 1
	s.notify(key, value)
	return true, &store.KVPair{Key: key, Value: value, LastIndex: newIndex}, nil
}

// AtomicDelete performs an atomic delete, ensuring that the current value matches the provided previous value
func (s *SQLiteStore) AtomicDelete(ctx context.Context, key string, previous *store.KVPair) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var currentIndex uint64
	err := s.db.QueryRowContext(ctx, `
        SELECT last_index FROM kv_store WHERE key = ?
    `, key).Scan(&currentIndex)
	if err == sql.ErrNoRows {
		return false, store.ErrKeyNotFound
	} else if err != nil {
		return false, err
	}

	if previous != nil && previous.LastIndex != currentIndex {
		return false, store.ErrKeyModified
	}

	_, err = s.db.ExecContext(ctx, `
        DELETE FROM kv_store 
        WHERE key = ?
    `, key)
	if err != nil {
		return false, err
	}

	// Send notification for watchers
	s.notify(key, nil)
	return true, nil
}

// Close closes the database connection
func (s *SQLiteStore) Close() error {
	return s.db.Close()
}

func (s *SQLiteStore) startCleanupTask(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				// Delete expired keys
				_, _ = s.db.ExecContext(ctx, `DELETE FROM kv_store WHERE expires_at IS NOT NULL AND expires_at <= ?`, time.Now())
			case <-ctx.Done():
				return
			}
		}
	}()
}
