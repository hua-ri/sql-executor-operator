package database

import (
	"database/sql"
	v1 "github.com/hua-ri/sql-executor-operator/api/v1"
)

// DatabaseType 表示数据库类型
type DatabaseType string

const (
	MySQL      DatabaseType = "mysql"
	ClickHouse DatabaseType = "clickhouse"
)

type DatabaseExecute interface {
	Connect(config v1.DatabaseConfig, password string) (*sql.DB, error)
	ExecuteQuery(db *sql.DB, query string) (*sql.Rows, error)
	ExecuteStatement(db *sql.DB, query string) (sql.Result, error)
	BeginTransaction(db *sql.DB) (*sql.Tx, error)
	Close(db *sql.DB) error
	ValidateConnection(db *sql.DB) error
	GetDatabaseVersion(db *sql.DB, dbType v1.DatabaseType) (string, error)
	IsQueryReadOnly(query string) bool
}

// DatabaseConfig 定义了数据库连接参数
type DatabaseConfig struct {
	// 数据库类型：mysql或clickhouse
	Type DatabaseType `json:"type"`

	// 数据库版本（例如："mysql5.7", "mysql8.0", "clickhouse22.8"）
	Version string `json:"version,omitempty"`

	// 连接详情
	Host     string `json:"host"`
	Port     int32  `json:"port"`
	Database string `json:"database"`
	Username string `json:"username"`

	// 额外的连接参数
	Parameters map[string]string `json:"parameters,omitempty"`

	// SSL/TLS配置
	// +optional
	TLS *TLSConfig `json:"tls,omitempty"`

	// 连接超时时间（秒）
	Timeout *int32 `json:"timeout,omitempty"`
}

// TLSConfig定义了SSL/TLS配置
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

// SecretRef引用Kubernetes Secret
type SecretRef struct {
	Name string `json:"name"`
	Key  string `json:"key,omitempty"`
}
