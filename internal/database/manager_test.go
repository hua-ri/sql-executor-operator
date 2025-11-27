package database

import (
	v1 "github.com/hua-ri/sql-executor-operator/api/v1"
	"testing"
	"time"
)

// 测试配置定义
var testConfigs = []struct {
	name     string
	config   v1.DatabaseConfig
	password string
}{
	{
		name: "MySQL5.7",
		config: v1.DatabaseConfig{
			Type:     v1.MySQL,
			Version:  "5.7",
			Host:     "localhost",
			Port:     3306,
			Database: "testdb",
			Username: "root",
			Timeout:  intPtr(30),
		},
		password: "123456",
	},
	{
		name: "MySQL8.0",
		config: v1.DatabaseConfig{
			Type:     v1.MySQL,
			Version:  "8.0",
			Host:     "localhost",
			Port:     3307,
			Database: "testdb",
			Username: "root",
			Timeout:  intPtr(30),
		},
		password: "123456",
	},
	{
		name: "ClickHouse20.1",
		config: v1.DatabaseConfig{
			Type:     v1.ClickHouse,
			Version:  "20.1",
			Host:     "localhost",
			Port:     9000,
			Database: "default",
			Username: "default",
			Timeout:  intPtr(30),
		},
		password: "", // ClickHouse默认无密码
	},
	{
		name: "ClickHouse21.8",
		config: v1.DatabaseConfig{
			Type:     v1.ClickHouse,
			Version:  "21.8",
			Host:     "localhost",
			Port:     9001,
			Database: "default",
			Username: "default",
			Timeout:  intPtr(30),
		},
		password: "",
	},
}

func intPtr(i int32) *int32 {
	return &i
}

// TestDatabaseConnection 测试数据库连接功能
func TestDatabaseConnection(t *testing.T) {
	manager := NewDatabaseManager()

	for _, tc := range testConfigs {
		t.Run(tc.name, func(t *testing.T) {
			// 测试连接
			db, err := manager.Connect(tc.config, tc.password)
			if err != nil {
				t.Fatalf("连接失败: %v", err)
			}
			defer manager.Close(db)

			// 测试版本查询
			version, err := manager.GetDatabaseVersion(db, tc.config.Type)
			if err != nil {
				t.Fatalf("获取版本失败: %v", err)
			}
			t.Logf("%s 版本: %s", tc.name, version)

			// 验证连接有效性
			if err := manager.ValidateConnection(db); err != nil {
				t.Fatalf("连接验证失败: %v", err)
			}
		})
	}
}

// TestQueryExecution 测试查询执行功能
func TestQueryExecution(t *testing.T) {
	manager := NewDatabaseManager()

	for _, tc := range testConfigs {
		t.Run(tc.name, func(t *testing.T) {
			db, err := manager.Connect(tc.config, tc.password)
			if err != nil {
				t.Fatalf("连接失败: %v", err)
			}
			defer manager.Close(db)

			// 根据数据库类型执行不同的测试查询
			var testQuery string
			switch tc.config.Type {
			case v1.MySQL:
				testQuery = "SELECT 1 as result, NOW() as timestamp"
			case v1.ClickHouse:
				testQuery = "SELECT 1 as result, now() as timestamp"
			}

			rows, err := manager.ExecuteQuery(db, testQuery)
			if err != nil {
				t.Fatalf("查询执行失败: %v", err)
			}
			defer rows.Close()

			// 验证结果
			var result int
			var timestamp time.Time
			if rows.Next() {
				if err := rows.Scan(&result, &timestamp); err != nil {
					t.Fatalf("结果扫描失败: %v", err)
				}
				if result != 1 {
					t.Errorf("期望结果 1, 得到 %d", result)
				}
				t.Logf("查询结果: %d, 时间: %v", result, timestamp)
			}
		})
	}
}

// TestStatementExecution 测试SQL语句执行功能
func TestStatementExecution(t *testing.T) {
	manager := NewDatabaseManager()

	for _, tc := range testConfigs {
		t.Run(tc.name, func(t *testing.T) {
			db, err := manager.Connect(tc.config, tc.password)
			if err != nil {
				t.Fatalf("连接失败: %v", err)
			}
			defer manager.Close(db)

			// 创建测试表
			var createTableSQL string
			switch tc.config.Type {
			case v1.MySQL:
				createTableSQL = `
                    CREATE TEMPORARY TABLE test_users (
                        id INT PRIMARY KEY AUTO_INCREMENT,
                        name VARCHAR(50),
                        email VARCHAR(100),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )`
			case v1.ClickHouse:
				createTableSQL = `
                    CREATE TEMPORARY TABLE test_users (
                        id Int32,
                        name String,
                        email String,
                        created_at DateTime DEFAULT now()
                    ) ENGINE = Memory`
			}

			_, err = manager.ExecuteStatement(db, createTableSQL)
			if err != nil {
				t.Fatalf("创建表失败: %v", err)
			}

			// 插入数据
			insertSQL := "INSERT INTO test_users (id, name, email) VALUES (1, 'test_user', 'test@example.com')"
			result, err := manager.ExecuteStatement(db, insertSQL)
			if err != nil {
				t.Fatalf("插入数据失败: %v", err)
			}

			rowsAffected, _ := result.RowsAffected()
			t.Logf("插入影响行数: %d", rowsAffected)

			// 查询数据
			rows, err := manager.ExecuteQuery(db, "SELECT name, email FROM test_users WHERE id = 1")
			if err != nil {
				t.Fatalf("查询数据失败: %v", err)
			}
			defer rows.Close()

			var name, email string
			if rows.Next() {
				if err := rows.Scan(&name, &email); err != nil {
					t.Fatalf("数据扫描失败: %v", err)
				}
				if name != "test_user" || email != "test@example.com" {
					t.Errorf("数据不匹配: 期望 (test_user, test@example.com), 得到 (%s, %s)", name, email)
				}
			}
		})
	}
}

// TestTransaction 测试事务功能（MySQL）
func TestTransaction(t *testing.T) {
	manager := NewDatabaseManager()

	mysqlConfigs := []struct {
		name   string
		config v1.DatabaseConfig
	}{
		{"MySQL5.7", testConfigs[0].config},
		{"MySQL8.0", testConfigs[1].config},
	}

	for _, tc := range mysqlConfigs {
		t.Run(tc.name, func(t *testing.T) {
			db, err := manager.Connect(tc.config, "123456")
			if err != nil {
				t.Fatalf("连接失败: %v", err)
			}
			defer manager.Close(db)

			// 开始事务
			tx, err := manager.BeginTransaction(db)
			if err != nil {
				t.Fatalf("开始事务失败: %v", err)
			}

			// 在事务中执行操作
			_, err = tx.Exec("CREATE TEMPORARY TABLE tx_test (id INT, name VARCHAR(50))")
			if err != nil {
				tx.Rollback()
				t.Fatalf("事务执行失败: %v", err)
			}

			_, err = tx.Exec("INSERT INTO tx_test VALUES (1, 'transaction_test')")
			if err != nil {
				tx.Rollback()
				t.Fatalf("事务插入失败: %v", err)
			}

			// 提交事务
			if err := tx.Commit(); err != nil {
				t.Fatalf("事务提交失败: %v", err)
			}

			t.Logf("事务测试完成")
		})
	}
}

// TestConnectionPool 测试连接池功能
func TestConnectionPool(t *testing.T) {
	manager := NewDatabaseManager()

	for _, tc := range testConfigs {
		t.Run(tc.name, func(t *testing.T) {
			db, err := manager.Connect(tc.config, tc.password)
			if err != nil {
				t.Fatalf("连接失败: %v", err)
			}
			defer manager.Close(db)

			// 测试连接池统计
			stats := db.Stats()
			t.Logf("连接池统计 - 打开连接数: %d, 空闲连接数: %d, 等待次数: %d",
				stats.OpenConnections, stats.Idle, stats.WaitCount)

			// 测试并发查询
			results := make(chan bool, 5)
			for i := 0; i < 5; i++ {
				go func(index int) {
					var query string
					if tc.config.Type == v1.MySQL {
						query = "SELECT SLEEP(0.1)"
					} else {
						query = "SELECT sleepEachRow(0.1) FROM numbers(1)"
					}

					_, err := manager.ExecuteQuery(db, query)
					if err != nil {
						results <- false
					} else {
						results <- true
					}
				}(i)
			}

			successCount := 0
			for i := 0; i < 5; i++ {
				if <-results {
					successCount++
				}
			}

			if successCount != 5 {
				t.Errorf("并发测试失败: 成功 %d/5", successCount)
			}
		})
	}
}

// TestErrorHandling 测试错误处理
func TestErrorHandling(t *testing.T) {
	manager := NewDatabaseManager()

	// 测试错误密码
	invalidConfig := testConfigs[0].config
	_, err := manager.Connect(invalidConfig, "wrong_password")
	if err == nil {
		t.Error("期望密码错误但连接成功")
	}

	// 测试错误查询
	db, err := manager.Connect(testConfigs[0].config, "123456")
	if err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer manager.Close(db)

	_, err = manager.ExecuteQuery(db, "SELECT * FROM non_existent_table")
	if err == nil {
		t.Error("期望查询错误但执行成功")
	}
}
