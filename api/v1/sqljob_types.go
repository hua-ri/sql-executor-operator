/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SqlJobSpec 定义了SqlJob的期望状态
type SqlJobSpec struct {
	// 全局数据库配置，可以被阶段级别的配置覆盖
	// +optional
	Database *DatabaseConfig `json:"database,omitempty"`

	// 执行阶段定义
	Stages []Stage `json:"stages"`

	// 全局重试策略
	// +optional
	RetryPolicy *RetryPolicy `json:"retryPolicy,omitempty"`

	// 全局变量，可以在阶段间共享
	// +optional
	Variables map[string]string `json:"variables,omitempty"`
}

// Stage 定义了单个执行阶段
type Stage struct {
	// 阶段名称，必须唯一
	Name string `json:"name"`

	// 阶段描述
	// +optional
	Description string `json:"description,omitempty"`

	// 此阶段的数据库配置（覆盖全局配置）
	// +optional
	Database *DatabaseConfig `json:"database,omitempty"`

	// 操作类型
	Type OperationType `json:"type"`

	// 要执行的SQL语句
	Queries []string `json:"queries"`

	// 输出配置（用于查询操作）
	// +optional
	Output *OutputConfig `json:"output,omitempty"`

	// 执行此阶段的条件
	// +optional
	Conditions []Condition `json:"conditions,omitempty"`

	// 期望结果验证
	// +optional
	Expectations *Expectation `json:"expectations,omitempty"`

	// 失败时的回滚查询
	// +optional
	RollbackQueries []string `json:"rollbackQueries,omitempty"`

	// 此阶段的超时时间（秒）
	// +optional
	Timeout *int32 `json:"timeout,omitempty"`
}

// OperationType 表示SQL操作的类型
// +kubebuilder:validation:Enum=Query;Execute;DDL;DML
type OperationType string

const (
	// Query - SELECT语句，导出结果
	OperationQuery OperationType = "Query"
	// Execute - DML操作（INSERT, UPDATE, DELETE）
	OperationExecute OperationType = "Execute"
	// DDL - 模式变更（CREATE, ALTER, DROP）
	OperationDDL OperationType = "DDL"
	// DML - 支持事务的数据操作
	OperationDML OperationType = "DML"
)

// Condition 定义了阶段的执行条件
type Condition struct {
	// 条件类型
	Type ConditionType `json:"type"`

	// 要检查条件的阶段名称
	// +optional
	Stage string `json:"stage,omitempty"`

	// 要评估的表达式
	// +optional
	Expression string `json:"expression,omitempty"`

	// 用于比较的期望值
	// +optional
	ExpectedValue *string `json:"expectedValue,omitempty"`

	// 自定义条件脚本
	// +optional
	Script *string `json:"script,omitempty"`
}

// ConditionType 表示条件的类型
// +kubebuilder:validation:Enum=Always;OnSuccess;OnFailure;OnRowsAffected;Custom
type ConditionType string

const (
	ConditionAlways         ConditionType = "Always"
	ConditionOnSuccess      ConditionType = "OnSuccess"
	ConditionOnFailure      ConditionType = "OnFailure"
	ConditionOnRowsAffected ConditionType = "OnRowsAffected"
	ConditionCustom         ConditionType = "Custom"
)

// Expectation 定义了验证的期望结果
type Expectation struct {
	// 最小影响行数
	// +optional
	MinRowsAffected *int64 `json:"minRowsAffected,omitempty"`

	// 最大影响行数
	// +optional
	MaxRowsAffected *int64 `json:"maxRowsAffected,omitempty"`

	// 查询的期望行数
	// +optional
	ExpectedRowCount *int64 `json:"expectedRowCount,omitempty"`

	// 自定义验证脚本
	// +optional
	ValidationScript *string `json:"validationScript,omitempty"`
}

// DatabaseConfig 定义了数据库连接参数
type DatabaseConfig struct {
	// 数据库类型：mysql或clickhouse
	Type DatabaseType `json:"type"`

	// 数据库版本（例如："mysql5.7", "mysql8.0", "clickhouse22.8"）
	// +optional
	Version string `json:"version,omitempty"`

	// 连接详情
	Host     string `json:"host"`
	Port     int32  `json:"port"`
	Database string `json:"database"`
	Username string `json:"username"`

	// 密码Secret引用
	PasswordSecret SecretRef `json:"passwordSecret"`

	// 额外的连接参数
	// +optional
	Parameters map[string]string `json:"parameters,omitempty"`

	// SSL/TLS配置
	// +optional
	TLS *TLSConfig `json:"tls,omitempty"`

	// 连接超时时间（秒）
	// +optional
	Timeout *int32 `json:"timeout,omitempty"`
}

// DatabaseType 表示数据库类型
// +kubebuilder:validation:Enum=mysql;clickhouse
type DatabaseType string

const (
	MySQL      DatabaseType = "mysql"
	ClickHouse DatabaseType = "clickhouse"
)

// TLSConfig 定义了SSL/TLS配置
type TLSConfig struct {
	// 启用TLS
	Enabled bool `json:"enabled"`

	// CA证书Secret引用
	// +optional
	CASecret *SecretRef `json:"caSecret,omitempty"`

	// 客户端证书Secret引用
	// +optional
	ClientCertSecret *SecretRef `json:"clientCertSecret,omitempty"`

	// 跳过证书验证
	// +optional
	SkipVerify bool `json:"skipVerify,omitempty"`
}

// SecretRef 引用Kubernetes Secret
type SecretRef struct {
	Name string `json:"name"`
	Key  string `json:"key,omitempty"`
}

// OutputConfig 定义了输出目的地
type OutputConfig struct {
	// CSV配置
	CSV CSVConfig `json:"csv"`

	// 存储目的地
	// +optional
	Destinations []Destination `json:"destinations,omitempty"`
}

// CSVConfig 定义了CSV输出格式
type CSVConfig struct {
	// 文件名前缀
	FilenamePrefix string `json:"filenamePrefix"`

	// 包含标题行
	// +optional
	IncludeHeader *bool `json:"includeHeader,omitempty"`

	// 字段分隔符
	// +optional
	Delimiter string `json:"delimiter,omitempty"`

	// 自定义日期格式
	// +optional
	DateFormat string `json:"dateFormat,omitempty"`
}

// Destination 定义了输出存储目的地
type Destination struct {
	// 目的地类型
	Type DestinationType `json:"type"`

	// 阿里云OSS配置
	// +optional
	AliOSS *AliOSSConfig `json:"aliOSS,omitempty"`

	// 腾讯云COS配置
	// +optional
	TencentCOS *TencentCOSConfig `json:"tencentCOS,omitempty"`
}

// DestinationType 表示存储目的地类型
// +kubebuilder:validation:Enum=alioss;tencentcos;feishu
type DestinationType string

const (
	AliOSS     DestinationType = "alioss"
	TencentCOS DestinationType = "tencentcos"
)

// AliOSSConfig 定义了阿里云OSS配置
type AliOSSConfig struct {
	Bucket   string `json:"bucket"`
	Endpoint string `json:"endpoint"`
	Region   string `json:"region"`

	// 包含访问密钥和秘密密钥的Secret
	SecretRef SecretRef `json:"secretRef"`

	// 对象路径前缀
	// +optional
	PathPrefix string `json:"pathPrefix,omitempty"`
}

// TencentCOSConfig 定义了腾讯云COS配置
type TencentCOSConfig struct {
	Bucket string `json:"bucket"`
	Region string `json:"region"`

	// 包含Secret ID和Secret Key的Secret
	SecretRef SecretRef `json:"secretRef"`

	// 对象路径前缀
	// +optional
	PathPrefix string `json:"pathPrefix,omitempty"`
}

// RetryPolicy 定义了作业重试行为
type RetryPolicy struct {
	// 最大重试次数
	// +optional
	MaxRetries *int32 `json:"maxRetries,omitempty"`

	// 退避策略
	// +optional
	Backoff *BackoffPolicy `json:"backoff,omitempty"`
}

// BackoffPolicy 定义了重试退避行为
type BackoffPolicy struct {
	// 初始退避持续时间（秒）
	// +optional
	InitialInterval *int32 `json:"initialInterval,omitempty"`

	// 最大退避持续时间（秒）
	// +optional
	MaxInterval *int32 `json:"maxInterval,omitempty"`

	// 退避乘数
	// +optional
	Multiplier *int32 `json:"multiplier,omitempty"`
}

// SqlJobStatus 定义了SqlJob的观察状态
type SqlJobStatus struct {
	// 作业的当前阶段
	Phase JobPhase `json:"phase,omitempty"`

	// 当前正在执行的阶段
	// +optional
	CurrentStage string `json:"currentStage,omitempty"`

	// 阶段执行历史
	// +optional
	StageHistory []StageResult `json:"stageHistory,omitempty"`

	// 计算后的全局变量
	// +optional
	ComputedVariables map[string]string `json:"computedVariables,omitempty"`

	// 总体消息
	// +optional
	Message string `json:"message,omitempty"`

	// 作业开始时间
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// 作业完成时间
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// 当前阶段的重试次数
	// +optional
	RetryCount int32 `json:"retryCount,omitempty"`
}

// JobPhase 表示作业的阶段
// +kubebuilder:validation:Enum=Pending;Running;Succeeded;Failed;Retrying
type JobPhase string

const (
	JobPending   JobPhase = "Pending"
	JobRunning   JobPhase = "Running"
	JobSucceeded JobPhase = "Succeeded"
	JobFailed    JobPhase = "Failed"
	JobRetrying  JobPhase = "Retrying"
)

// StageResult 包含阶段的执行结果
type StageResult struct {
	// 阶段名称
	Name string `json:"name"`

	// 执行阶段
	Phase StagePhase `json:"phase"`

	// 详细消息
	// +optional
	Message string `json:"message,omitempty"`

	// 阶段开始时间
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// 阶段完成时间
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// 对于查询操作：导出结果
	// +optional
	ExportResults []ExportResult `json:"exportResults,omitempty"`

	// 对于执行操作：执行统计
	// +optional
	ExecutionStats *ExecutionStats `json:"executionStats,omitempty"`

	// 从此阶段提取的变量
	// +optional
	Variables map[string]string `json:"variables,omitempty"`

	// 条件评估结果
	// +optional
	ConditionResults []ConditionResult `json:"conditionResults,omitempty"`
}

// StagePhase 表示阶段的阶段
// +kubebuilder:validation:Enum=Pending;Running;Succeeded;Failed;Skipped;RolledBack
type StagePhase string

const (
	StagePending    StagePhase = "Pending"
	StageRunning    StagePhase = "Running"
	StageSucceeded  StagePhase = "Succeeded"
	StageFailed     StagePhase = "Failed"
	StageSkipped    StagePhase = "Skipped"
	StageRolledBack StagePhase = "RolledBack"
)

// ExecutionStats 包含执行操作的统计信息
type ExecutionStats struct {
	// 所有查询影响的总行数
	RowsAffected int64 `json:"rowsAffected"`

	// 执行时间（毫秒）
	ExecutionTime int64 `json:"executionTime"`

	// 单个查询结果
	// +optional
	QueryResults []QueryResult `json:"queryResults,omitempty"`
}

// QueryResult 包含单个查询的结果
type QueryResult struct {
	// 查询索引
	Index int32 `json:"index"`

	// 影响的行数
	RowsAffected int64 `json:"rowsAffected"`

	// 最后插入的ID（如果适用）
	// +optional
	LastInsertID *int64 `json:"lastInsertId,omitempty"`

	// 如果查询失败，错误消息
	// +optional
	Error *string `json:"error,omitempty"`
}

// ExportResult 包含查询操作的导出信息
type ExportResult struct {
	// 目的地类型
	DestinationType DestinationType `json:"destinationType"`

	// 文件位置或标识符
	Location string `json:"location"`

	// 导出的行数
	RowsExported int64 `json:"rowsExported"`

	// 文件大小（字节）
	FileSize int64 `json:"fileSize"`

	// 上传时间戳
	UploadTime metav1.Time `json:"uploadTime"`
}

// ConditionResult 包含条件评估结果
type ConditionResult struct {
	// 条件描述
	Description string `json:"description"`

	// 条件是否满足
	Met bool `json:"met"`

	// 详细消息
	// +optional
	Message string `json:"message,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Current Stage",type=string,JSONPath=`.status.currentStage`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:printcolumn:name="Message",type=string,JSONPath=`.status.message`

// SqlJob 是SqlJob API的Schema
type SqlJob struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SqlJobSpec   `json:"spec,omitempty"`
	Status SqlJobStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// SqlJobList 包含SqlJob列表
type SqlJobList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SqlJob `json:"items"`
}

func init() {
	SchemeBuilder.Register(&SqlJob{}, &SqlJobList{})
}
