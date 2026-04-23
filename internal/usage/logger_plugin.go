// Package usage provides usage tracking and logging functionality for the CLI Proxy API server.
// It includes plugins for monitoring API usage, token consumption, and other metrics
// to help with observability and billing purposes.
package usage

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
	log "github.com/sirupsen/logrus"
)

var statisticsEnabled atomic.Bool

func init() {
	statisticsEnabled.Store(true)
	coreusage.RegisterPlugin(NewLoggerPlugin())
}

// LoggerPlugin collects in-memory request statistics for usage analysis.
// It implements coreusage.Plugin to receive usage records emitted by the runtime.
type LoggerPlugin struct {
	stats *RequestStatistics
}

// NewLoggerPlugin constructs a new logger plugin instance.
//
// Returns:
//   - *LoggerPlugin: A new logger plugin instance wired to the shared statistics store.
func NewLoggerPlugin() *LoggerPlugin { return &LoggerPlugin{stats: defaultRequestStatistics} }

// HandleUsage implements coreusage.Plugin.
// It updates the in-memory statistics store whenever a usage record is received.
//
// Parameters:
//   - ctx: The context for the usage record
//   - record: The usage record to aggregate
func (p *LoggerPlugin) HandleUsage(ctx context.Context, record coreusage.Record) {
	if !statisticsEnabled.Load() {
		return
	}
	if p == nil || p.stats == nil {
		return
	}
	p.stats.Record(ctx, record)
}

// SetStatisticsEnabled toggles whether in-memory statistics are recorded.
func SetStatisticsEnabled(enabled bool) { statisticsEnabled.Store(enabled) }

// StatisticsEnabled reports the current recording state.
func StatisticsEnabled() bool { return statisticsEnabled.Load() }

// RequestStatistics maintains aggregated request metrics in memory.
// 持久化相关字段集中放在 store 及 persist* 一组字段中，通过独立的
// persistMu/persistCond 与后台 worker 配合，实现"写请求立即返回、落盘异步执行"。
type RequestStatistics struct {
	mu sync.RWMutex

	totalRequests int64
	successCount  int64
	failureCount  int64
	totalTokens   int64

	apis map[string]*apiStats

	requestsByDay  map[string]int64
	requestsByHour map[int]int64
	tokensByDay    map[string]int64
	tokensByHour   map[int]int64

	// store 是当前挂载的快照持久化后端；nil 表示仅内存模式。
	store SnapshotStore

	// 以下字段构成异步持久化协作栈：
	// - persistMu 与 persistCond 保护并唤醒 worker；
	// - persistPending 表示"还有待写入的快照"；
	// - persistSaving 表示"worker 正在落盘"；
	// - persistEnqueuedGeneration / persistCompletedGeneration 是单调递增世代号，
	//   Flush 通过比较二者来判断自己触发的写入是否已完成，实现精确等待。
	persistMu                  sync.Mutex
	persistCond                *sync.Cond
	persistWorkerStarted       bool
	persistPending             bool
	persistSaving              bool
	persistSnapshot            StatisticsSnapshot
	persistSnapshotStore       SnapshotStore
	persistEnqueuedGeneration  uint64
	persistCompletedGeneration uint64
}

// apiStats holds aggregated metrics for a single API key.
type apiStats struct {
	TotalRequests int64
	TotalTokens   int64
	Models        map[string]*modelStats
}

// modelStats holds aggregated metrics for a specific model within an API.
type modelStats struct {
	TotalRequests int64
	TotalTokens   int64
	Details       []RequestDetail
}

// RequestDetail stores the timestamp, latency, and token usage for a single request.
type RequestDetail struct {
	Timestamp time.Time  `json:"timestamp"`
	LatencyMs int64      `json:"latency_ms"`
	Source    string     `json:"source"`
	AuthIndex string     `json:"auth_index"`
	Tokens    TokenStats `json:"tokens"`
	Failed    bool       `json:"failed"`
}

// TokenStats captures the token usage breakdown for a request.
type TokenStats struct {
	InputTokens     int64 `json:"input_tokens"`
	OutputTokens    int64 `json:"output_tokens"`
	ReasoningTokens int64 `json:"reasoning_tokens"`
	CachedTokens    int64 `json:"cached_tokens"`
	TotalTokens     int64 `json:"total_tokens"`
}

// StatisticsSnapshot represents an immutable view of the aggregated metrics.
type StatisticsSnapshot struct {
	TotalRequests int64 `json:"total_requests"`
	SuccessCount  int64 `json:"success_count"`
	FailureCount  int64 `json:"failure_count"`
	TotalTokens   int64 `json:"total_tokens"`

	APIs map[string]APISnapshot `json:"apis"`

	RequestsByDay  map[string]int64 `json:"requests_by_day"`
	RequestsByHour map[string]int64 `json:"requests_by_hour"`
	TokensByDay    map[string]int64 `json:"tokens_by_day"`
	TokensByHour   map[string]int64 `json:"tokens_by_hour"`
}

// APISnapshot summarises metrics for a single API key.
type APISnapshot struct {
	TotalRequests int64                    `json:"total_requests"`
	TotalTokens   int64                    `json:"total_tokens"`
	Models        map[string]ModelSnapshot `json:"models"`
}

// ModelSnapshot summarises metrics for a specific model.
type ModelSnapshot struct {
	TotalRequests int64           `json:"total_requests"`
	TotalTokens   int64           `json:"total_tokens"`
	Details       []RequestDetail `json:"details"`
}

var defaultRequestStatistics = NewRequestStatistics()

// GetRequestStatistics returns the shared statistics store.
func GetRequestStatistics() *RequestStatistics { return defaultRequestStatistics }

// NewRequestStatistics constructs an empty statistics store.
// 同时预先构造 persistCond，后续即使未挂载 SnapshotStore，
// Flush 调用也能安全地读写该 Cond 而不需要再次判空。
func NewRequestStatistics() *RequestStatistics {
	stats := &RequestStatistics{
		apis:           make(map[string]*apiStats),
		requestsByDay:  make(map[string]int64),
		requestsByHour: make(map[int]int64),
		tokensByDay:    make(map[string]int64),
		tokensByHour:   make(map[int]int64),
	}
	stats.persistCond = sync.NewCond(&stats.persistMu)
	return stats
}

// SetSnapshotStore 挂载/替换持久化后端。传入 nil 可关闭持久化，
// 退化回纯内存模式。在 Record / MergeSnapshot 触发落盘时会读取该字段。
func (s *RequestStatistics) SetSnapshotStore(store SnapshotStore) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.store = store
}

// Record ingests a new usage record and updates the aggregates.
func (s *RequestStatistics) Record(ctx context.Context, record coreusage.Record) {
	if s == nil {
		return
	}
	if !statisticsEnabled.Load() {
		return
	}
	timestamp := record.RequestedAt
	if timestamp.IsZero() {
		timestamp = time.Now()
	}
	detail := normaliseDetail(record.Detail)
	totalTokens := detail.TotalTokens
	statsKey := record.APIKey
	if statsKey == "" {
		statsKey = resolveAPIIdentifier(ctx, record)
	}
	failed := record.Failed
	if !failed {
		failed = !resolveSuccess(ctx)
	}
	success := !failed
	modelName := record.Model
	if modelName == "" {
		modelName = "unknown"
	}
	dayKey := timestamp.Format("2006-01-02")
	hourKey := timestamp.Hour()

	// 为了不让异步持久化延长写锁持有时间，这里不使用 defer Unlock，
	// 在临界区末尾手动 Unlock，再调用 triggerAsyncSave。
	var snapshot StatisticsSnapshot
	var store SnapshotStore

	s.mu.Lock()

	s.totalRequests++
	if success {
		s.successCount++
	} else {
		s.failureCount++
	}
	s.totalTokens += totalTokens

	stats, ok := s.apis[statsKey]
	if !ok {
		stats = &apiStats{Models: make(map[string]*modelStats)}
		s.apis[statsKey] = stats
	}
	s.updateAPIStats(stats, modelName, RequestDetail{
		Timestamp: timestamp,
		LatencyMs: normaliseLatency(record.Latency),
		Source:    record.Source,
		AuthIndex: record.AuthIndex,
		Tokens:    detail,
		Failed:    failed,
	})

	s.requestsByDay[dayKey]++
	s.requestsByHour[hourKey]++
	s.tokensByDay[dayKey] += totalTokens
	s.tokensByHour[hourKey] += totalTokens

	// 仅在挂载了 SnapshotStore 时才序列化快照，避免纯内存模式下的无谓拷贝。
	store = s.store
	if store != nil {
		snapshot = s.snapshotLocked()
	}
	s.mu.Unlock()

	s.triggerAsyncSave(ctx, store, snapshot)
}

func (s *RequestStatistics) updateAPIStats(stats *apiStats, model string, detail RequestDetail) {
	stats.TotalRequests++
	stats.TotalTokens += detail.Tokens.TotalTokens
	modelStatsValue, ok := stats.Models[model]
	if !ok {
		modelStatsValue = &modelStats{}
		stats.Models[model] = modelStatsValue
	}
	modelStatsValue.TotalRequests++
	modelStatsValue.TotalTokens += detail.Tokens.TotalTokens
	modelStatsValue.Details = append(modelStatsValue.Details, detail)
}

// Snapshot returns a copy of the aggregated metrics for external consumption.
// 对外接口，自行持读锁后委托给 snapshotLocked。
func (s *RequestStatistics) Snapshot() StatisticsSnapshot {
	result := StatisticsSnapshot{}
	if s == nil {
		return result
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.snapshotLocked()
}

// snapshotLocked 在调用方已持有 mu（读或写）时使用，
// 避免同一条写路径再次加锁导致死锁，也消除一次重复遍历。
func (s *RequestStatistics) snapshotLocked() StatisticsSnapshot {
	result := StatisticsSnapshot{
		APIs:           make(map[string]APISnapshot, len(s.apis)),
		RequestsByDay:  make(map[string]int64, len(s.requestsByDay)),
		RequestsByHour: make(map[string]int64, len(s.requestsByHour)),
		TokensByDay:    make(map[string]int64, len(s.tokensByDay)),
		TokensByHour:   make(map[string]int64, len(s.tokensByHour)),
	}

	result.TotalRequests = s.totalRequests
	result.SuccessCount = s.successCount
	result.FailureCount = s.failureCount
	result.TotalTokens = s.totalTokens

	for apiName, stats := range s.apis {
		apiSnapshot := APISnapshot{
			TotalRequests: stats.TotalRequests,
			TotalTokens:   stats.TotalTokens,
			Models:        make(map[string]ModelSnapshot, len(stats.Models)),
		}
		for modelName, modelStatsValue := range stats.Models {
			requestDetails := make([]RequestDetail, len(modelStatsValue.Details))
			copy(requestDetails, modelStatsValue.Details)
			apiSnapshot.Models[modelName] = ModelSnapshot{
				TotalRequests: modelStatsValue.TotalRequests,
				TotalTokens:   modelStatsValue.TotalTokens,
				Details:       requestDetails,
			}
		}
		result.APIs[apiName] = apiSnapshot
	}

	for k, v := range s.requestsByDay {
		result.RequestsByDay[k] = v
	}
	for hour, v := range s.requestsByHour {
		key := formatHour(hour)
		result.RequestsByHour[key] = v
	}
	for k, v := range s.tokensByDay {
		result.TokensByDay[k] = v
	}
	for hour, v := range s.tokensByHour {
		key := formatHour(hour)
		result.TokensByHour[key] = v
	}

	return result
}

type MergeResult struct {
	Added   int64 `json:"added"`
	Skipped int64 `json:"skipped"`
}

// MergeSnapshot merges an exported statistics snapshot into the current store.
// Existing data is preserved and duplicate request details are skipped.
func (s *RequestStatistics) MergeSnapshot(snapshot StatisticsSnapshot) MergeResult {
	result := MergeResult{}
	if s == nil {
		return result
	}

	// 合并完成后也要落盘；与 Record 一样在临界区外触发，缩短写锁时间。
	var persistedSnapshot StatisticsSnapshot
	var store SnapshotStore

	s.mu.Lock()

	seen := make(map[string]struct{})
	for apiName, stats := range s.apis {
		if stats == nil {
			continue
		}
		for modelName, modelStatsValue := range stats.Models {
			if modelStatsValue == nil {
				continue
			}
			for _, detail := range modelStatsValue.Details {
				seen[dedupKey(apiName, modelName, detail)] = struct{}{}
			}
		}
	}

	for apiName, apiSnapshot := range snapshot.APIs {
		apiName = strings.TrimSpace(apiName)
		if apiName == "" {
			continue
		}
		stats, ok := s.apis[apiName]
		if !ok || stats == nil {
			stats = &apiStats{Models: make(map[string]*modelStats)}
			s.apis[apiName] = stats
		} else if stats.Models == nil {
			stats.Models = make(map[string]*modelStats)
		}
		for modelName, modelSnapshot := range apiSnapshot.Models {
			modelName = strings.TrimSpace(modelName)
			if modelName == "" {
				modelName = "unknown"
			}
			for _, detail := range modelSnapshot.Details {
				detail.Tokens = normaliseTokenStats(detail.Tokens)
				if detail.LatencyMs < 0 {
					detail.LatencyMs = 0
				}
				if detail.Timestamp.IsZero() {
					detail.Timestamp = time.Now()
				}
				key := dedupKey(apiName, modelName, detail)
				if _, exists := seen[key]; exists {
					result.Skipped++
					continue
				}
				seen[key] = struct{}{}
				s.recordImported(apiName, modelName, stats, detail)
				result.Added++
			}
		}
	}

	store = s.store
	if store != nil {
		persistedSnapshot = s.snapshotLocked()
	}
	s.mu.Unlock()

	// MergeSnapshot 通常由导入接口触发，没有 gin context 可传，这里给 context.Background 即可。
	s.triggerAsyncSave(context.Background(), store, persistedSnapshot)
	return result
}

func (s *RequestStatistics) recordImported(apiName, modelName string, stats *apiStats, detail RequestDetail) {
	totalTokens := detail.Tokens.TotalTokens
	if totalTokens < 0 {
		totalTokens = 0
	}

	s.totalRequests++
	if detail.Failed {
		s.failureCount++
	} else {
		s.successCount++
	}
	s.totalTokens += totalTokens

	s.updateAPIStats(stats, modelName, detail)

	dayKey := detail.Timestamp.Format("2006-01-02")
	hourKey := detail.Timestamp.Hour()

	s.requestsByDay[dayKey]++
	s.requestsByHour[hourKey]++
	s.tokensByDay[dayKey] += totalTokens
	s.tokensByHour[hourKey] += totalTokens
}

// triggerAsyncSave 把一份已序列化的快照交给后台 worker 落盘。
// 首次调用时惰性启动 worker goroutine，后续调用只做信号通知，无重复开销。
// 若同时有多次触发，只保留最新快照（persistSnapshot 是覆盖写），避免写放大。
func (s *RequestStatistics) triggerAsyncSave(_ context.Context, store SnapshotStore, snapshot StatisticsSnapshot) {
	if s == nil || store == nil {
		return
	}

	s.persistMu.Lock()
	if s.persistCond == nil {
		s.persistCond = sync.NewCond(&s.persistMu)
	}
	if !s.persistWorkerStarted {
		s.persistWorkerStarted = true
		go s.runPersistenceWorker()
	}
	s.persistSnapshot = snapshot
	s.persistSnapshotStore = store
	s.persistPending = true
	// 每次入队都递增世代号，Flush 用它来判定"我之前看到的那次写已经完成"。
	s.persistEnqueuedGeneration++
	s.persistCond.Signal()
	s.persistMu.Unlock()
}

// Flush 阻塞直到调用时刻之前入队的所有快照都已完成落盘。
// 常用于服务优雅关闭，确保最后一批统计数据不会丢失。
// ctx 被取消时会唤醒等待并返回 ctx.Err()。
func (s *RequestStatistics) Flush(ctx context.Context) error {
	if s == nil {
		return nil
	}

	s.persistMu.Lock()
	if s.persistCond == nil {
		s.persistCond = sync.NewCond(&s.persistMu)
	}
	// 记录"本次 Flush 希望至少等到的世代号"。
	target := s.persistEnqueuedGeneration
	if target == s.persistCompletedGeneration && !s.persistPending && !s.persistSaving {
		s.persistMu.Unlock()
		return nil
	}
	// 监听 ctx 取消，通过 Broadcast 唤醒下面的 Wait 以便及时返回。
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			s.persistMu.Lock()
			s.persistCond.Broadcast()
			s.persistMu.Unlock()
		case <-done:
		}
	}()
	defer close(done)
	for target > s.persistCompletedGeneration || s.persistPending || s.persistSaving {
		if err := ctx.Err(); err != nil {
			s.persistMu.Unlock()
			return err
		}
		s.persistCond.Wait()
	}
	s.persistMu.Unlock()
	return nil
}

// runPersistenceWorker 是唯一的后台持久化 goroutine：
// 等待 persistPending 为真，抓起最新快照释放锁，然后调用 store.Save 落盘。
// 保存过程中新的 triggerAsyncSave 仍可入队，但只会覆盖 persistSnapshot，
// worker 下一轮继续处理——这正是我们想要的"合并写"。
func (s *RequestStatistics) runPersistenceWorker() {
	for {
		s.persistMu.Lock()
		for !s.persistPending {
			s.persistCond.Wait()
		}
		snapshot := s.persistSnapshot
		store := s.persistSnapshotStore
		generation := s.persistEnqueuedGeneration
		s.persistPending = false
		s.persistSaving = true
		s.persistMu.Unlock()

		if err := store.Save(context.Background(), snapshot); err != nil {
			log.WithError(err).Error("usage statistics async save failed")
		}

		s.persistMu.Lock()
		s.persistSaving = false
		// 记录当前已完成的最新世代号，Broadcast 唤醒所有等待的 Flush。
		s.persistCompletedGeneration = generation
		s.persistCond.Broadcast()
		s.persistMu.Unlock()
	}
}

func dedupKey(apiName, modelName string, detail RequestDetail) string {
	timestamp := detail.Timestamp.UTC().Format(time.RFC3339Nano)
	tokens := normaliseTokenStats(detail.Tokens)
	return fmt.Sprintf(
		"%s|%s|%s|%s|%s|%t|%d|%d|%d|%d|%d",
		apiName,
		modelName,
		timestamp,
		detail.Source,
		detail.AuthIndex,
		detail.Failed,
		tokens.InputTokens,
		tokens.OutputTokens,
		tokens.ReasoningTokens,
		tokens.CachedTokens,
		tokens.TotalTokens,
	)
}

func resolveAPIIdentifier(ctx context.Context, record coreusage.Record) string {
	if ctx != nil {
		if ginCtx, ok := ctx.Value("gin").(*gin.Context); ok && ginCtx != nil {
			path := ginCtx.FullPath()
			if path == "" && ginCtx.Request != nil {
				path = ginCtx.Request.URL.Path
			}
			method := ""
			if ginCtx.Request != nil {
				method = ginCtx.Request.Method
			}
			if path != "" {
				if method != "" {
					return method + " " + path
				}
				return path
			}
		}
	}
	if record.Provider != "" {
		return record.Provider
	}
	return "unknown"
}

func resolveSuccess(ctx context.Context) bool {
	if ctx == nil {
		return true
	}
	ginCtx, ok := ctx.Value("gin").(*gin.Context)
	if !ok || ginCtx == nil {
		return true
	}
	status := ginCtx.Writer.Status()
	if status == 0 {
		return true
	}
	return status < httpStatusBadRequest
}

const httpStatusBadRequest = 400

func normaliseDetail(detail coreusage.Detail) TokenStats {
	tokens := TokenStats{
		InputTokens:     detail.InputTokens,
		OutputTokens:    detail.OutputTokens,
		ReasoningTokens: detail.ReasoningTokens,
		CachedTokens:    detail.CachedTokens,
		TotalTokens:     detail.TotalTokens,
	}
	if tokens.TotalTokens == 0 {
		tokens.TotalTokens = detail.InputTokens + detail.OutputTokens + detail.ReasoningTokens
	}
	if tokens.TotalTokens == 0 {
		tokens.TotalTokens = detail.InputTokens + detail.OutputTokens + detail.ReasoningTokens + detail.CachedTokens
	}
	return tokens
}

func normaliseTokenStats(tokens TokenStats) TokenStats {
	if tokens.TotalTokens == 0 {
		tokens.TotalTokens = tokens.InputTokens + tokens.OutputTokens + tokens.ReasoningTokens
	}
	if tokens.TotalTokens == 0 {
		tokens.TotalTokens = tokens.InputTokens + tokens.OutputTokens + tokens.ReasoningTokens + tokens.CachedTokens
	}
	return tokens
}

func normaliseLatency(latency time.Duration) int64 {
	if latency <= 0 {
		return 0
	}
	return latency.Milliseconds()
}

func formatHour(hour int) string {
	if hour < 0 {
		hour = 0
	}
	hour = hour % 24
	return fmt.Sprintf("%02d", hour)
}
