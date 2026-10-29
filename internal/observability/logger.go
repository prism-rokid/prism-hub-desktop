package observability

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"
)

// LogField 结构化日志字段
type LogField struct {
	Key   string
	Value interface{}
}

// Field 创建日志字段
func Field(key string, value interface{}) LogField {
	return LogField{Key: key, Value: value}
}

// 标准字段名常量
const (
	// 任务相关
	FieldTaskID         = "task_id"
	FieldTaskStatus     = "task_status"
	FieldConversationID = "conversation_id"
	FieldSourceDevice   = "source_device"
	FieldInputText      = "input_text"
	FieldSummary        = "summary"

	// 队列相关
	FieldQueuePosition = "queue_position"
	FieldQueueSize     = "queue_size"
	FieldQueueReason   = "queue_reason"

	// 审批相关
	FieldApprovalID = "approval_id"
	FieldDecision   = "decision"
	FieldOutcome    = "outcome"

	// 事件相关
	FieldEventType  = "event_type"
	FieldFromStatus = "from_status"
	FieldToStatus   = "to_status"
	FieldReason     = "reason"

	// 系统相关
	FieldDuration  = "duration_ms"
	FieldError     = "error"
	FieldErrorCode = "error_code"
	FieldComponent = "component"
	FieldOperation = "operation"
)

// StructuredLogger 结构化日志记录器
type StructuredLogger struct {
	logger *slog.Logger
	fields []LogField
}

// NewStructuredLogger 创建结构化日志记录器
func NewStructuredLogger(component string) *StructuredLogger {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level:     slog.LevelInfo,
		AddSource: true,
	})

	logger := slog.New(handler)

	return &StructuredLogger{
		logger: logger,
		fields: []LogField{
			{Key: FieldComponent, Value: component},
		},
	}
}

// WithFields 添加字段
func (l *StructuredLogger) WithFields(fields ...LogField) *StructuredLogger {
	newFields := make([]LogField, len(l.fields)+len(fields))
	copy(newFields, l.fields)
	copy(newFields[len(l.fields):], fields)

	return &StructuredLogger{
		logger: l.logger,
		fields: newFields,
	}
}

// WithContext 添加上下文信息
func (l *StructuredLogger) WithContext(ctx context.Context) *StructuredLogger {
	// 可以从 context 中提取 trace_id, span_id 等
	return l.WithFields(Field("trace_id", ctx.Value("trace_id")))
}

// toSlogAttrs 转换为 slog.Attr
func (l *StructuredLogger) toSlogAttrs() []any {
	attrs := make([]any, 0, len(l.fields)*2)
	for _, f := range l.fields {
		attrs = append(attrs, f.Key, f.Value)
	}
	return attrs
}

// Info 记录信息日志
func (l *StructuredLogger) Info(msg string, fields ...LogField) {
	allFields := append(l.fields, fields...)
	l.logger.Info(msg, l.fieldsToArgs(allFields)...)
}

// Error 记录错误日志
func (l *StructuredLogger) Error(msg string, err error, fields ...LogField) {
	allFields := append(l.fields, Field(FieldError, err.Error()))
	allFields = append(allFields, fields...)
	l.logger.Error(msg, l.fieldsToArgs(allFields)...)
}

// Warn 记录警告日志
func (l *StructuredLogger) Warn(msg string, fields ...LogField) {
	allFields := append(l.fields, fields...)
	l.logger.Warn(msg, l.fieldsToArgs(allFields)...)
}

// Debug 记录调试日志
func (l *StructuredLogger) Debug(msg string, fields ...LogField) {
	allFields := append(l.fields, fields...)
	l.logger.Debug(msg, l.fieldsToArgs(allFields)...)
}

// fieldsToArgs 将字段转换为参数
func (l *StructuredLogger) fieldsToArgs(fields []LogField) []any {
	args := make([]any, 0, len(fields)*2)
	for _, f := range fields {
		if f.Value != nil {
			args = append(args, f.Key, f.Value)
		}
	}
	return args
}

// TaskLogger 任务专用日志记录器
type TaskLogger struct {
	*StructuredLogger
}

// NewTaskLogger 创建任务日志记录器
func NewTaskLogger(component string) *TaskLogger {
	return &TaskLogger{
		StructuredLogger: NewStructuredLogger(component),
	}
}

// TaskStarted 记录任务开始
func (l *TaskLogger) TaskStarted(convID, taskID, sourceDevice string) {
	l.Info("task_started",
		Field(FieldConversationID, convID),
		Field(FieldTaskID, taskID),
		Field(FieldSourceDevice, sourceDevice),
	)
}

// TaskCompleted 记录任务完成
func (l *TaskLogger) TaskCompleted(convID, taskID string, duration time.Duration) {
	l.Info("task_completed",
		Field(FieldConversationID, convID),
		Field(FieldTaskID, taskID),
		Field(FieldDuration, duration.Milliseconds()),
	)
}

// TaskFailed 记录任务失败
func (l *TaskLogger) TaskFailed(convID, taskID string, err error) {
	l.Error("task_failed", err,
		Field(FieldConversationID, convID),
		Field(FieldTaskID, taskID),
	)
}

// TaskInterrupted 记录任务中断
func (l *TaskLogger) TaskInterrupted(convID, taskID, reason string) {
	l.Info("task_interrupted",
		Field(FieldConversationID, convID),
		Field(FieldTaskID, taskID),
		Field(FieldReason, reason),
	)
}

// TaskQueued 记录任务入队
func (l *TaskLogger) TaskQueued(convID, taskID string, position int) {
	l.Info("task_queued",
		Field(FieldConversationID, convID),
		Field(FieldTaskID, taskID),
		Field(FieldQueuePosition, position),
	)
}

// TaskPromoted 记录任务提升
func (l *TaskLogger) TaskPromoted(convID, taskID string, fromPosition int) {
	l.Info("task_promoted",
		Field(FieldConversationID, convID),
		Field(FieldTaskID, taskID),
		Field(FieldQueuePosition, fromPosition),
	)
}

// ApprovalCreated 记录审批创建
func (l *TaskLogger) ApprovalCreated(convID, taskID, approvalID string) {
	l.Info("approval_created",
		Field(FieldConversationID, convID),
		Field(FieldTaskID, taskID),
		Field(FieldApprovalID, approvalID),
	)
}

// ApprovalResolved 记录审批决议
func (l *TaskLogger) ApprovalResolved(convID, taskID, approvalID, decision string) {
	l.Info("approval_resolved",
		Field(FieldConversationID, convID),
		Field(FieldTaskID, taskID),
		Field(FieldApprovalID, approvalID),
		Field(FieldDecision, decision),
	)
}

// StatusChanged 记录状态变更
func (l *TaskLogger) StatusChanged(convID, taskID string, from, to string) {
	l.Info("status_changed",
		Field(FieldConversationID, convID),
		Field(FieldTaskID, taskID),
		Field(FieldFromStatus, from),
		Field(FieldToStatus, to),
	)
}

// MetricsCollector 指标收集器
type MetricsCollector struct {
	logger *StructuredLogger
	prefix string
}

// NewMetricsCollector 创建指标收集器
func NewMetricsCollector(component string) *MetricsCollector {
	return &MetricsCollector{
		logger: NewStructuredLogger(component),
		prefix: "metric_",
	}
}

// RecordQueueSize 记录队列大小
func (m *MetricsCollector) RecordQueueSize(convID string, size int) {
	m.logger.Info(m.prefix+"queue_size",
		Field(FieldConversationID, convID),
		Field(FieldQueueSize, size),
	)
}

// RecordTaskDuration 记录任务持续时间
func (m *MetricsCollector) RecordTaskDuration(convID, taskID string, duration time.Duration) {
	m.logger.Info(m.prefix+"task_duration",
		Field(FieldConversationID, convID),
		Field(FieldTaskID, taskID),
		Field(FieldDuration, duration.Milliseconds()),
	)
}

// RecordApprovalLatency 记录审批延迟
func (m *MetricsCollector) RecordApprovalLatency(convID, approvalID string, latency time.Duration) {
	m.logger.Info(m.prefix+"approval_latency",
		Field(FieldConversationID, convID),
		Field(FieldApprovalID, approvalID),
		Field(FieldDuration, latency.Milliseconds()),
	)
}

// RecordError 记录错误指标
func (m *MetricsCollector) RecordError(operation string, errCode string) {
	m.logger.Info(m.prefix+"error",
		Field(FieldOperation, operation),
		Field(FieldErrorCode, errCode),
	)
}

// LogLevel 日志级别
type LogLevel string

const (
	LevelDebug LogLevel = "DEBUG"
	LevelInfo  LogLevel = "INFO"
	LevelWarn  LogLevel = "WARN"
	LevelError LogLevel = "ERROR"
)

// ParseLogLevel 解析日志级别
func ParseLogLevel(level string) LogLevel {
	switch level {
	case "DEBUG":
		return LevelDebug
	case "INFO":
		return LevelInfo
	case "WARN":
		return LevelWarn
	case "ERROR":
		return LevelError
	default:
		return LevelInfo
	}
}

// Config 日志配置
type Config struct {
	Level      LogLevel
	Format     string // json, text
	Output     string // stdout, file
	FilePath   string
	MaxSize    int // MB
	MaxBackups int
	MaxAge     int // days
	Compress   bool
}

// DefaultConfig 默认配置
var DefaultConfig = Config{
	Level:      LevelInfo,
	Format:     "json",
	Output:     "stdout",
	MaxSize:    100,
	MaxBackups: 3,
	MaxAge:     7,
	Compress:   true,
}

// InitLogger 初始化日志系统
func InitLogger(config Config) *StructuredLogger {
	var handler slog.Handler

	opts := &slog.HandlerOptions{
		Level:     parseSlogLevel(config.Level),
		AddSource: true,
	}

	switch config.Output {
	case "file":
		// 实际实现中应该使用 lumberjack 等库实现日志轮转
		file, _ := os.OpenFile(config.FilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		handler = slog.NewJSONHandler(file, opts)
	default:
		handler = slog.NewJSONHandler(os.Stdout, opts)
	}

	logger := slog.New(handler)

	return &StructuredLogger{
		logger: logger,
		fields: []LogField{},
	}
}

func parseSlogLevel(level LogLevel) slog.Level {
	switch level {
	case LevelDebug:
		return slog.LevelDebug
	case LevelInfo:
		return slog.LevelInfo
	case LevelWarn:
		return slog.LevelWarn
	case LevelError:
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// String 返回字符串表示
func (f LogField) String() string {
	return fmt.Sprintf("%s=%v", f.Key, f.Value)
}
