package usage

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

// 验证一次 Record 之后异步 worker 最终会把快照写入磁盘。
func TestRequestStatisticsRecordPersistsSnapshotToFile(t *testing.T) {
	t.Parallel()

	stats := NewRequestStatistics()
	path := filepath.Join(t.TempDir(), "usage.json")
	stats.SetSnapshotStore(NewSnapshotFileStore(path))

	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "persist-api",
		Model:       "persist-model",
		RequestedAt: time.Date(2026, 4, 9, 10, 0, 0, 0, time.UTC),
		Source:      "persist-test",
		AuthIndex:   "1",
		Detail: coreusage.Detail{
			InputTokens:  5,
			OutputTokens: 7,
			TotalTokens:  12,
		},
	})

	got := waitForSnapshotFile(t, path, func(snapshot StatisticsSnapshot) bool {
		return snapshot.TotalRequests == 1 && snapshot.TotalTokens == 12
	})
	if got.TotalRequests != 1 {
		t.Fatalf("total requests = %d, want 1", got.TotalRequests)
	}
}

// 模拟"重启"：写入 -> Flush -> 新建 stats 并从文件加载，
// 验证持久化与加载构成闭环。
func TestRequestStatisticsSnapshotSurvivesRestart(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "usage.json")

	firstStats := NewRequestStatistics()
	firstStats.SetSnapshotStore(NewSnapshotFileStore(path))
	firstStats.Record(context.Background(), coreusage.Record{
		APIKey:      "restart-api",
		Model:       "restart-model",
		RequestedAt: time.Date(2026, 4, 9, 11, 0, 0, 0, time.UTC),
		Source:      "restart-test",
		AuthIndex:   "9",
		Detail: coreusage.Detail{
			InputTokens:  8,
			OutputTokens: 5,
			TotalTokens:  13,
		},
	})
	if err := firstStats.Flush(context.Background()); err != nil {
		t.Fatalf("flush before restart failed: %v", err)
	}

	secondStats := NewRequestStatistics()
	if err := LoadSnapshotFromFile(path, secondStats); err != nil {
		t.Fatalf("load snapshot failed: %v", err)
	}

	got := secondStats.Snapshot()
	if got.TotalRequests != 1 {
		t.Fatalf("total requests after restart = %d, want 1", got.TotalRequests)
	}
	if got.TotalTokens != 13 {
		t.Fatalf("total tokens after restart = %d, want 13", got.TotalTokens)
	}
}

// readSnapshotFile 辅助函数：把磁盘上的 JSON 读回为 StatisticsSnapshot。
func readSnapshotFile(t *testing.T, path string) StatisticsSnapshot {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read snapshot file failed: %v", err)
	}

	var snapshot StatisticsSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		t.Fatalf("unmarshal snapshot file failed: %v", err)
	}
	return snapshot
}

// waitForSnapshotFile 轮询等待异步 worker 完成落盘，最多等待 3 秒；
// 超时则报告最后读到的快照，便于排查。
func waitForSnapshotFile(t *testing.T, path string, predicate func(StatisticsSnapshot) bool) StatisticsSnapshot {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	var last StatisticsSnapshot
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			snapshot := readSnapshotFile(t, path)
			last = snapshot
			if predicate(snapshot) {
				return snapshot
			}
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for snapshot file update, last snapshot: %+v", last)
	return StatisticsSnapshot{}
}
