package usage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// SnapshotStore 定义统计快照的持久化接口。
// 实现方负责把快照以任何形式（文件、数据库等）持久化。
type SnapshotStore interface {
	Save(ctx context.Context, snapshot StatisticsSnapshot) error
}

// SnapshotFileStore 是基于本地 JSON 文件的 SnapshotStore 实现。
// 通过临时文件 + 原子替换写入，保证崩溃时不会产生半写状态的文件。
type SnapshotFileStore struct {
	path string
	mu   sync.Mutex
}

// NewSnapshotFileStore 构造一个写入指定路径的 SnapshotFileStore。
func NewSnapshotFileStore(path string) *SnapshotFileStore {
	return &SnapshotFileStore{path: path}
}

// Path 返回当前快照文件的绝对路径。
func (s *SnapshotFileStore) Path() string {
	if s == nil {
		return ""
	}
	return s.path
}

// 以下两个变量是为了便于在测试中替换底层系统调用，
// 默认实现分别在 persistence_windows.go / persistence_nonwindows.go 中。
var replaceSnapshotFile = defaultReplaceSnapshotFile

var syncSnapshotParentDir = defaultSyncSnapshotParentDir

// loadedSnapshotKey 用来去重：同一个 stats + path 只会加载一次，
// 避免配置重载时把同一份快照重复并入内存导致数据翻倍。
type loadedSnapshotKey struct {
	stats *RequestStatistics
	path  string
}

var loadedUsageSnapshotFiles sync.Map

// SnapshotPath 根据配置文件路径和工作目录，推导出统一的快照文件位置：
// 若提供 configFilePath，则放在配置文件同级目录下的 data/usage-statistics.json；
// 否则回落到 workingDir/data/usage-statistics.json。
func SnapshotPath(configFilePath, workingDir string) string {
	configFilePath = strings.TrimSpace(configFilePath)
	if configFilePath != "" {
		if !filepath.IsAbs(configFilePath) {
			configFilePath = filepath.Join(workingDir, configFilePath)
		}
		return filepath.Join(filepath.Dir(configFilePath), "data", "usage-statistics.json")
	}

	return filepath.Join(workingDir, "data", "usage-statistics.json")
}

// LoadSnapshotFromFileIfEnabled 仅当统计功能启用时才加载快照文件。
// 这样在用户关闭使用量统计时，启动时不会读取历史文件造成内存污染。
func LoadSnapshotFromFileIfEnabled(enabled bool, path string, stats *RequestStatistics) error {
	if !enabled {
		return nil
	}

	return LoadSnapshotFromFile(path, stats)
}

// LoadSnapshotFromFile 从给定路径读取 JSON 快照并合并到 stats 中。
// 对同一 (stats, path) 组合具备幂等性；文件不存在时视为无历史数据，返回 nil。
func LoadSnapshotFromFile(path string, stats *RequestStatistics) error {
	if stats == nil || path == "" {
		return nil
	}

	cleanPath := filepath.Clean(path)
	key := loadedSnapshotKey{stats: stats, path: cleanPath}
	if _, loaded := loadedUsageSnapshotFiles.LoadOrStore(key, struct{}{}); loaded {
		return nil
	}

	data, err := os.ReadFile(cleanPath)
	if err != nil {
		loadedUsageSnapshotFiles.Delete(key)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("read usage snapshot file: %w", err)
	}

	var snapshot StatisticsSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		loadedUsageSnapshotFiles.Delete(key)
		return fmt.Errorf("unmarshal usage snapshot file: %w", err)
	}

	stats.MergeSnapshot(snapshot)
	return nil
}

// Save 把快照以原子方式写入磁盘：
// 1) 先写临时文件并 fsync；2) 通过 replaceSnapshotFile 原子替换目标文件；
// 3) 在非 Windows 平台上 fsync 父目录，确保目录项在断电后仍可恢复。
// 任何步骤失败都会清理临时文件，避免残留。
func (s *SnapshotFileStore) Save(ctx context.Context, snapshot StatisticsSnapshot) error {
	if s == nil {
		return fmt.Errorf("usage snapshot file store is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.path == "" {
		return fmt.Errorf("usage snapshot file path is empty")
	}

	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal usage snapshot: %w", err)
	}
	data = append(data, '\n')

	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create usage snapshot directory: %w", err)
	}

	// 持久化路径在进程内串行化，避免多个 goroutine 同时替换目标文件。
	s.mu.Lock()
	defer s.mu.Unlock()

	tmpFile, err := os.CreateTemp(dir, ".usage-snapshot-*.tmp")
	if err != nil {
		return fmt.Errorf("create usage snapshot temp file: %w", err)
	}
	tmpPath := tmpFile.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpPath)
		}
	}()

	if _, err := tmpFile.Write(data); err != nil {
		_ = tmpFile.Close()
		return fmt.Errorf("write usage snapshot temp file: %w", err)
	}
	if err := tmpFile.Sync(); err != nil {
		_ = tmpFile.Close()
		return fmt.Errorf("sync usage snapshot temp file: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("close usage snapshot temp file: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	if err := replaceSnapshotFile(tmpPath, s.path); err != nil {
		return fmt.Errorf("replace usage snapshot file: %w", err)
	}
	if err := syncSnapshotParentDir(dir); err != nil {
		return fmt.Errorf("sync usage snapshot parent directory: %w", err)
	}

	cleanup = false
	return nil
}
