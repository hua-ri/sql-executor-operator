package database

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	v1 "github.com/hua-ri/sql-executor-operator/api/v1"
	"github.com/hua-ri/sql-executor-operator/internal/csv_processor"
	"strings"
	"testing"
	"time"
)

// TestCSVExportIntegration 测试数据库操作与CSV导出的完整流程
func TestCSVExportIntegration(t *testing.T) {
	dbManager := NewDatabaseManager()
	csvProcessor := csv_processor.NewCSVProcessor()

	for _, tc := range testConfigs {
		t.Run(tc.name, func(t *testing.T) {
			// 1. 建立数据库连接
			db, err := dbManager.Connect(tc.config, tc.password)
			if err != nil {
				t.Fatalf("数据库连接失败: %v", err)
			}
			defer dbManager.Close(db)

			// 2. 创建测试表
			err = createTestTable(db, tc.config.Type)
			if err != nil {
				t.Fatalf("创建测试表失败: %v", err)
			}

			// 3. 插入测试数据
			err = insertTestData(db, tc.config.Type)
			if err != nil {
				t.Fatalf("插入测试数据失败: %v", err)
			}

			// 4. 查询数据
			rows, err := dbManager.ExecuteQuery(db, "SELECT * FROM export_test")
			if err != nil {
				t.Fatalf("查询数据失败: %v", err)
			}
			defer rows.Close()

			// 5. 配置CSV导出
			csvConfig := v1.CSVConfig{
				FilenamePrefix: fmt.Sprintf("%s_export", strings.ToLower(string(tc.config.Type))),
				IncludeHeader:  boolPtr(true),
				Delimiter:      ",",
				DateFormat:     "2006-01-02 15:04:05",
			}

			// 6. 验证CSV配置
			if err := csvProcessor.ValidateCSVConfig(csvConfig); err != nil {
				t.Fatalf("CSV配置验证失败: %v", err)
			}

			// 7. 转换为CSV
			csvData, rowCount, err := csvProcessor.ConvertToCSV(rows, csvConfig)
			if err != nil {
				t.Fatalf("CSV转换失败: %v", err)
			}

			// 8. 验证结果
			if rowCount <= 0 {
				t.Errorf("期望至少1行数据，得到 %d 行", rowCount)
			}

			if len(csvData) == 0 {
				t.Error("CSV数据为空")
			}

			// 9. 验证CSV格式
			csvString := string(csvData)
			if err := validateCSVFormat(csvString, csvConfig, tc.config.Type); err != nil {
				t.Errorf("CSV格式验证失败: %v", err)
			}

			// 10. 生成文件名
			filename := csvProcessor.GenerateFilename(csvConfig.FilenamePrefix)
			t.Logf("导出成功: %s - %d 行数据", filename, rowCount)
			t.Logf("CSV预览:\n%s", csvString)

			// 11. 清理测试表
			if err := cleanupTestTable(db, tc.config.Type); err != nil {
				t.Logf("清理测试表失败: %v", err)
			}
		})
	}
}

// TestCSVExportWithDifferentConfigs 测试不同的CSV配置
func TestCSVExportWithDifferentConfigs(t *testing.T) {
	dbManager := NewDatabaseManager()
	csvProcessor := csv_processor.NewCSVProcessor()

	// 使用MySQL进行配置测试（比较稳定）
	mysqlConfig := testConfigs[0]
	db, err := dbManager.Connect(mysqlConfig.config, mysqlConfig.password)
	if err != nil {
		t.Fatalf("数据库连接失败: %v", err)
	}
	defer dbManager.Close(db)

	// 准备测试数据
	if err := createTestTable(db, mysqlConfig.config.Type); err != nil {
		t.Fatalf("创建测试表失败: %v", err)
	}
	defer cleanupTestTable(db, mysqlConfig.config.Type)

	if err := insertTestData(db, mysqlConfig.config.Type); err != nil {
		t.Fatalf("插入测试数据失败: %v", err)
	}

	tests := []struct {
		name   string
		config v1.CSVConfig
	}{
		{
			name: "默认配置",
			config: v1.CSVConfig{
				FilenamePrefix: "default_export",
				IncludeHeader:  boolPtr(true),
			},
		},
		{
			name: "无标题行",
			config: v1.CSVConfig{
				FilenamePrefix: "no_header_export",
				IncludeHeader:  boolPtr(false),
			},
		},
		{
			name: "分号分隔符",
			config: v1.CSVConfig{
				FilenamePrefix: "semicolon_export",
				IncludeHeader:  boolPtr(true),
				Delimiter:      ";",
			},
		},
		{
			name: "自定义日期格式",
			config: v1.CSVConfig{
				FilenamePrefix: "custom_date_export",
				IncludeHeader:  boolPtr(true),
				DateFormat:     "2006-01-02",
			},
		},
		{
			name: "制表符分隔",
			config: v1.CSVConfig{
				FilenamePrefix: "tab_export",
				IncludeHeader:  boolPtr(true),
				Delimiter:      "\t",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 查询数据
			rows, err := dbManager.ExecuteQuery(db, "SELECT * FROM export_test")
			if err != nil {
				t.Fatalf("查询数据失败: %v", err)
			}
			defer rows.Close()

			// 转换为CSV
			csvData, rowCount, err := csvProcessor.ConvertToCSV(rows, tt.config)
			if err != nil {
				t.Fatalf("CSV转换失败: %v", err)
			}

			t.Logf("%s: 生成 %d 行数据", tt.name, rowCount)
			t.Logf("CSV样本:\n%s", string(csvData[:min(200, len(csvData))])+"...")
		})
	}
}

// TestCSVExportWithComplexData 测试复杂数据类型
func TestCSVExportWithComplexData(t *testing.T) {
	dbManager := NewDatabaseManager()
	csvProcessor := csv_processor.NewCSVProcessor()

	// 使用MySQL进行测试
	mysqlConfig := testConfigs[0]
	db, err := dbManager.Connect(mysqlConfig.config, mysqlConfig.password)
	if err != nil {
		t.Fatalf("数据库连接失败: %v", err)
	}
	defer dbManager.Close(db)

	// 创建复杂数据表
	err = createComplexTestTable(db, mysqlConfig.config.Type)
	if err != nil {
		t.Fatalf("创建复杂测试表失败: %v", err)
	}
	defer cleanupComplexTestTable(db, mysqlConfig.config.Type)

	// 插入复杂测试数据
	err = insertComplexTestData(db, mysqlConfig.config.Type)
	if err != nil {
		t.Fatalf("插入复杂测试数据失败: %v", err)
	}

	// 查询数据
	rows, err := dbManager.ExecuteQuery(db, "SELECT * FROM complex_export_test")
	if err != nil {
		t.Fatalf("查询数据失败: %v", err)
	}
	defer rows.Close()

	// 配置CSV导出
	csvConfig := v1.CSVConfig{
		FilenamePrefix: "complex_data_export",
		IncludeHeader:  boolPtr(true),
		Delimiter:      ",",
		DateFormat:     "2006-01-02 15:04:05",
	}

	// 转换为CSV
	csvData, rowCount, err := csvProcessor.ConvertToCSV(rows, csvConfig)
	if err != nil {
		t.Fatalf("CSV转换失败: %v", err)
	}

	t.Logf("复杂数据导出: %d 行数据", rowCount)
	t.Logf("CSV内容:\n%s", string(csvData))
}

// TestCSVExportPerformance 测试大数据量导出性能
func TestCSVExportPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过性能测试")
	}

	dbManager := NewDatabaseManager()
	csvProcessor := csv_processor.NewCSVProcessor()

	// 使用MySQL进行性能测试
	mysqlConfig := testConfigs[0]
	db, err := dbManager.Connect(mysqlConfig.config, mysqlConfig.password)
	if err != nil {
		t.Fatalf("数据库连接失败: %v", err)
	}
	defer dbManager.Close(db)

	// 创建大数据量表
	if err := createLargeTestTable(db, mysqlConfig.config.Type); err != nil {
		t.Fatalf("创建大数据量表失败: %v", err)
	}
	defer cleanupLargeTestTable(db, mysqlConfig.config.Type)

	// 插入大量测试数据
	rowCount, err := insertLargeTestData(db, mysqlConfig.config.Type, 1000)
	if err != nil {
		t.Fatalf("插入大量测试数据失败: %v", err)
	}

	t.Logf("已插入 %d 行测试数据", rowCount)

	// 测试导出性能
	startTime := time.Now()

	// 查询数据
	rows, err := dbManager.ExecuteQuery(db, "SELECT * FROM large_export_test")
	if err != nil {
		t.Fatalf("查询数据失败: %v", err)
	}
	defer rows.Close()

	// 配置CSV导出
	csvConfig := v1.CSVConfig{
		FilenamePrefix: "performance_export",
		IncludeHeader:  boolPtr(true),
	}

	// 转换为CSV
	csvData, exportedCount, err := csvProcessor.ConvertToCSV(rows, csvConfig)
	if err != nil {
		t.Fatalf("CSV转换失败: %v", err)
	}

	duration := time.Since(startTime)

	t.Logf("性能测试: 导出 %d 行数据，耗时 %v", exportedCount, duration)
	t.Logf("CSV文件大小: %.2f KB", float64(len(csvData))/1024)
	t.Logf("处理速度: %.2f 行/秒", float64(exportedCount)/duration.Seconds())
}

// 创建测试表
func createTestTable(db *sql.DB, dbType v1.DatabaseType) error {
	var createSQL string

	switch dbType {
	case v1.MySQL:
		createSQL = `
            CREATE TEMPORARY TABLE export_test (
                id INT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(50) NOT NULL,
                email VARCHAR(100),
                age INT,
                salary DECIMAL(10,2),
                active BOOLEAN DEFAULT true,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME,
                notes TEXT
            )`
	case v1.ClickHouse:
		createSQL = `
            CREATE TEMPORARY TABLE export_test (
                id Int32,
                name String,
                email String,
                age Int32,
                salary Float64,
                active UInt8,
                created_at DateTime,
                updated_at DateTime,
                notes String
            ) ENGINE = Memory`
	default:
		return fmt.Errorf("不支持的数据库类型: %s", dbType)
	}

	_, err := db.Exec(createSQL)
	return err
}

// 插入测试数据 - 重构版本
func insertTestData(db *sql.DB, dbType v1.DatabaseType) error {
	now := time.Now()

	switch dbType {
	case v1.MySQL:
		return insertMySQLData(db, now)
	case v1.ClickHouse:
		return insertClickHouseData(db, now)
	default:
		return fmt.Errorf("不支持的数据库类型: %s", dbType)
	}
}

// 插入MySQL测试数据
func insertMySQLData(db *sql.DB, now time.Time) error {
	insertSQL := `
        INSERT INTO export_test (name, email, age, salary, active, updated_at, notes) VALUES
        ('张三', 'zhangsan@example.com', 28, 50000.50, true, ?, '这是备注信息'),
        ('李四', 'lisi@example.com', 32, 65000.75, true, ?, '另一个备注'),
        ('王五', 'wangwu@example.com', 25, 45000.00, false, ?, '测试备注')`

	_, err := db.Exec(insertSQL, now, now, now)
	return err
}

// 插入ClickHouse测试数据
func insertClickHouseData(db *sql.DB, now time.Time) error {
	// ClickHouse 可以使用更简单的插入方式，避免参数绑定问题
	insertSQL := fmt.Sprintf(`
        INSERT INTO export_test (id, name, email, age, salary, active, created_at, updated_at, notes) VALUES
        (1, '张三', 'zhangsan@example.com', 28, 50000.50, 1, '%s', '%s', '这是备注信息'),
        (2, '李四', 'lisi@example.com', 32, 65000.75, 1, '%s', '%s', '另一个备注'),
        (3, '王五', 'wangwu@example.com', 25, 45000.00, 0, '%s', '%s', '测试备注')`,
		now.Format("2006-01-02 15:04:05"), now.Format("2006-01-02 15:04:05"),
		now.Format("2006-01-02 15:04:05"), now.Format("2006-01-02 15:04:05"),
		now.Format("2006-01-02 15:04:05"), now.Format("2006-01-02 15:04:05"))

	_, err := db.Exec(insertSQL)
	return err
}

// 创建复杂数据测试表
func createComplexTestTable(db *sql.DB, dbType v1.DatabaseType) error {
	var createSQL string

	switch dbType {
	case v1.MySQL:
		createSQL = `
            CREATE TEMPORARY TABLE complex_export_test (
                id INT PRIMARY KEY AUTO_INCREMENT,
                string_field VARCHAR(100),
                int_field INT,
                bigint_field BIGINT,
                float_field FLOAT,
                double_field DOUBLE,
                decimal_field DECIMAL(10,4),
                bool_field BOOLEAN,
                date_field DATE,
                time_field TIME,
                datetime_field DATETIME,
                timestamp_field TIMESTAMP,
                text_field TEXT,
                blob_field BLOB,
                json_field JSON,
                null_field VARCHAR(100) NULL
            )`
	case v1.ClickHouse:
		createSQL = `
            CREATE TEMPORARY TABLE complex_export_test (
                id Int32,
                string_field String,
                int_field Int32,
                bigint_field Int64,
                float_field Float32,
                double_field Float64,
                decimal_field Decimal(10,4),
                bool_field UInt8,
                date_field Date,
                time_field String,
                datetime_field DateTime,
                timestamp_field DateTime,
                text_field String,
                blob_field String,
                json_field String,
                null_field Nullable(String)
            ) ENGINE = Memory`
	}

	_, err := db.Exec(createSQL)
	return err
}

// 插入复杂测试数据
func insertComplexTestData(db *sql.DB, dbType v1.DatabaseType) error {
	now := time.Now()
	today := time.Now().Format("2006-01-02")
	currentTime := time.Now().Format("15:04:05")

	var insertSQL string
	switch dbType {
	case v1.MySQL:
		insertSQL = `
            INSERT INTO complex_export_test (
                string_field, int_field, bigint_field, float_field, double_field, 
                decimal_field, bool_field, date_field, time_field, datetime_field, 
                timestamp_field, text_field, blob_field, json_field, null_field
            ) VALUES
            ('普通字符串', 123, 1234567890, 123.45, 678.90, 999.9999, true, ?, ?, ?, ?, '长文本内容', '二进制数据', '{"key": "value"}', NULL),
            ('特殊字符: ,;\t"''', -456, -9876543210, -67.89, -123.45, -888.8888, false, ?, ?, ?, ?, '多行\n文本\n内容', '更多二进制', '{"array": [1,2,3]}', '非空值')`
	case v1.ClickHouse:
		insertSQL = `
            INSERT INTO complex_export_test VALUES
            (1, '普通字符串', 123, 1234567890, 123.45, 678.90, 999.9999, 1, ?, ?, ?, ?, '长文本内容', '二进制数据', '{"key": "value"}', NULL),
            (2, '特殊字符: ,;\t"''', -456, -9876543210, -67.89, -123.45, -888.8888, 0, ?, ?, ?, ?, '多行\n文本\n内容', '更多二进制', '{"array": [1,2,3]}', '非空值')`
	}

	_, err := db.Exec(insertSQL,
		today, currentTime, now, now,
		today, currentTime, now, now)
	return err
}

// 创建大数据量表
func createLargeTestTable(db *sql.DB, dbType v1.DatabaseType) error {
	var createSQL string

	switch dbType {
	case v1.MySQL:
		createSQL = `
            CREATE TEMPORARY TABLE large_export_test (
                id INT PRIMARY KEY AUTO_INCREMENT,
                user_id VARCHAR(50),
                product_name VARCHAR(100),
                category VARCHAR(50),
                price DECIMAL(10,2),
                quantity INT,
                purchase_date DATE,
                status VARCHAR(20)
            )`
	case v1.ClickHouse:
		createSQL = `
            CREATE TEMPORARY TABLE large_export_test (
                id Int32,
                user_id String,
                product_name String,
                category String,
                price Float64,
                quantity Int32,
                purchase_date Date,
                status String
            ) ENGINE = Memory`
	}

	_, err := db.Exec(createSQL)
	return err
}

// 插入大量测试数据
func insertLargeTestData(db *sql.DB, dbType v1.DatabaseType, count int) (int, error) {
	categories := []string{"电子", "服装", "食品", "家居", "图书"}
	statuses := []string{"已完成", "处理中", "已取消", "待发货"}

	batchSize := 100
	inserted := 0

	for i := 0; i < count; i += batchSize {
		currentBatch := min(batchSize, count-i)
		var valuePlaceholders []string
		var values []interface{}

		for j := 0; j < currentBatch; j++ {
			idx := i + j
			category := categories[idx%len(categories)]
			status := statuses[idx%len(statuses)]
			purchaseDate := time.Now().AddDate(0, 0, -idx).Format("2006-01-02")

			if dbType == v1.MySQL {
				valuePlaceholders = append(valuePlaceholders, "(?, ?, ?, ?, ?, ?, ?)")
			} else {
				valuePlaceholders = append(valuePlaceholders, "(?, ?, ?, ?, ?, ?, ?)")
			}

			values = append(values,
				fmt.Sprintf("user_%d", idx),
				fmt.Sprintf("产品_%d", idx),
				category,
				float64(idx*10+5),
				idx%10+1,
				purchaseDate,
				status,
			)
		}

		var insertSQL string
		if dbType == v1.MySQL {
			insertSQL = fmt.Sprintf(
				"INSERT INTO large_export_test (user_id, product_name, category, price, quantity, purchase_date, status) VALUES %s",
				strings.Join(valuePlaceholders, ","))
		} else {
			insertSQL = fmt.Sprintf(
				"INSERT INTO large_export_test VALUES %s",
				strings.Join(valuePlaceholders, ","))
		}

		_, err := db.Exec(insertSQL, values...)
		if err != nil {
			return inserted, err
		}

		inserted += currentBatch
	}

	return inserted, nil
}

// 清理测试表
func cleanupTestTable(db *sql.DB, dbType v1.DatabaseType) error {
	_, err := db.Exec("DROP TABLE IF EXISTS export_test")
	return err
}

// 清理复杂测试表
func cleanupComplexTestTable(db *sql.DB, dbType v1.DatabaseType) error {
	_, err := db.Exec("DROP TABLE IF EXISTS complex_export_test")
	return err
}

// 清理大数据量表
func cleanupLargeTestTable(db *sql.DB, dbType v1.DatabaseType) error {
	_, err := db.Exec("DROP TABLE IF EXISTS large_export_test")
	return err
}

// 验证CSV格式
func validateCSVFormat(csvString string, config v1.CSVConfig, dbType v1.DatabaseType) error {
	reader := csv.NewReader(strings.NewReader(csvString))

	// 根据配置设置分隔符
	if config.Delimiter != "" {
		reader.Comma = rune(config.Delimiter[0])
	}

	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("CSV解析失败: %v", err)
	}

	if len(records) == 0 {
		return fmt.Errorf("CSV为空")
	}

	// 检查标题行
	if config.IncludeHeader != nil && *config.IncludeHeader {
		if len(records) < 2 {
			return fmt.Errorf("CSV应包含标题行和数据行")
		}

		expectedColumns := []string{"id", "name", "email", "age", "salary", "active", "created_at", "updated_at", "notes"}
		header := records[0]

		// 简单的列数检查
		if len(header) < len(expectedColumns) {
			return fmt.Errorf("标题列数不足，期望至少%d列，实际%d列", len(expectedColumns), len(header))
		}
	}

	// 检查数据行
	dataStartIndex := 0
	if config.IncludeHeader != nil && *config.IncludeHeader {
		dataStartIndex = 1
	}

	for i := dataStartIndex; i < len(records); i++ {
		if len(records[i]) == 0 {
			return fmt.Errorf("第%d行为空", i+1)
		}
	}

	return nil
}

// 辅助函数
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func boolPtr(b bool) *bool {
	return &b
}
