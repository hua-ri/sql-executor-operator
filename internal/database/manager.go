package database

import (
	"database/sql"
	"fmt"
	v1 "github.com/hua-ri/sql-executor-operator/api/v1"
	"strings"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	// 导入数据库驱动
	_ "github.com/go-sql-driver/mysql"
)

// databaseManager 处理数据库连接和查询
type databaseManager struct{}

// NewDatabaseManager 创建新的数据库管理器
func NewDatabaseManager() DatabaseExecute {
	return &databaseManager{}
}

// Connect 根据配置建立数据库连接
func (dm *databaseManager) Connect(config v1.DatabaseConfig, password string) (*sql.DB, error) {
	var dsn string
	var driver string

	switch config.Type {
	case v1.MySQL:
		driver = "mysql"
		dsn = dm.buildMySQLDSN(config, password)
	case v1.ClickHouse:
		driver = "clickhouse"
		dsn = dm.buildClickHouseDSN(config, password)
	default:
		return nil, fmt.Errorf("不支持的数据库类型: %s", config.Type)
	}

	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, fmt.Errorf("打开数据库连接失败: %v", err)
	}

	// 设置连接池设置
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Hour)

	// 测试连接
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("数据库连接测试失败: %v", err)
	}

	return db, nil
}

// ExecuteQuery 执行SQL查询并返回结果
func (dm *databaseManager) ExecuteQuery(db *sql.DB, query string) (*sql.Rows, error) {
	return db.Query(query)
}

// ExecuteStatement 执行SQL语句（INSERT, UPDATE, DELETE等）
func (dm *databaseManager) ExecuteStatement(db *sql.DB, query string) (sql.Result, error) {
	return db.Exec(query)
}

// BeginTransaction 开始数据库事务
func (dm *databaseManager) BeginTransaction(db *sql.DB) (*sql.Tx, error) {
	return db.Begin()
}

// Close 关闭数据库连接
func (dm *databaseManager) Close(db *sql.DB) error {
	return db.Close()
}

// ValidateConnection 验证数据库连接是否有效
func (dm *databaseManager) ValidateConnection(db *sql.DB) error {
	return db.Ping()
}

// GetDatabaseVersion 获取数据库版本信息
func (dm *databaseManager) GetDatabaseVersion(db *sql.DB, dbType v1.DatabaseType) (string, error) {
	var version string
	var err error

	switch dbType {
	case v1.MySQL:
		err = db.QueryRow("SELECT VERSION()").Scan(&version)
	case v1.ClickHouse:
		err = db.QueryRow("SELECT version()").Scan(&version)
	default:
		return "", fmt.Errorf("不支持的数据库类型: %s", dbType)
	}

	if err != nil {
		return "", fmt.Errorf("获取数据库版本失败: %v", err)
	}

	return version, nil
}

// IsQueryReadOnly 检查查询是否为只读（SELECT语句）
func (dm *databaseManager) IsQueryReadOnly(query string) bool {
	query = strings.TrimSpace(query)
	query = strings.ToUpper(query)

	return strings.HasPrefix(query, "SELECT") ||
		strings.HasPrefix(query, "SHOW") ||
		strings.HasPrefix(query, "DESCRIBE") ||
		strings.HasPrefix(query, "EXPLAIN")
}
