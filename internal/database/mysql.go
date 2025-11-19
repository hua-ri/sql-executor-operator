package database

import (
	"fmt"
	v1 "github.com/hua-ri/sql-executor-operator/api/v1"
)

// buildMySQLDSN构建MySQL DSN，考虑版本差异
func (dm *databaseManager) buildMySQLDSN(config v1.DatabaseConfig, password string) string {
	// 基础DSN
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", config.Username, password, config.Host, config.Port, config.Database)

	// 根据版本添加参数
	params := "charset=utf8mb4&parseTime=True&loc=Local"

	// MySQL 8.0+ 特定参数
	if config.Version >= "8.0" {
		params += "&allowNativePasswords=true"
	}

	// MySQL 5.7 特定参数
	if config.Version >= "5.7" && config.Version < "8.0" {
		params += "&checkConnLiveness=true"
	}

	// 如果启用了TLS，添加TLS配置
	if config.TLS != nil && config.TLS.Enabled {
		if config.TLS.SkipVerify {
			params += "&tls=skip-verify"
		} else {
			params += "&tls=true"
		}
	}

	// 如果指定了超时时间，添加超时参数
	if config.Timeout != nil {
		params += fmt.Sprintf("&timeout=%ds", *config.Timeout)
	}

	// 添加自定义参数
	for key, value := range config.Parameters {
		params += fmt.Sprintf("&%s=%s", key, value)
	}

	return dsn + "?" + params
}
