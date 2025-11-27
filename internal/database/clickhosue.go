package database

import (
	"fmt"
	v1 "github.com/hua-ri/sql-executor-operator/api/v1"
)

// buildClickHouseDSN 构建ClickHouse DSN，考虑版本差异
func (dm *databaseManager) buildClickHouseDSN(config v1.DatabaseConfig, password string) string {
	// 基础DSN
	dsn := fmt.Sprintf("tcp://%s:%d?username=%s&password=%s&database=%s",
		config.Host, config.Port, config.Username, password, config.Database)

	// ClickHouse版本特定参数
	if config.Version >= "21.8" {
		dsn += "&enable_http_compression=1"
	}

	// 如果启用了TLS，添加TLS配置
	if config.TLS != nil && config.TLS.Enabled {
		dsn += "&secure=true"
		if config.TLS.SkipVerify {
			dsn += "&skip_verify=true"
		}
	}

	// 如果指定了超时时间，添加超时参数
	if config.Timeout != nil {
		dsn += fmt.Sprintf("&read_timeout=%ds", *config.Timeout)
	}

	// 添加自定义参数
	for key, value := range config.Parameters {
		dsn += fmt.Sprintf("&%s=%s", key, value)
	}

	return dsn
}
