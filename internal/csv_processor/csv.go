package csv_processor

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"fmt"
	v1 "github.com/hua-ri/sql-executor-operator/api/v1"
	"strconv"
	"time"
)

// CSVProcessor 处理从SQL结果到CSV的转换
type CSVProcessor struct {
}

// NewCSVProcessor 创建新的CSV处理器
func NewCSVProcessor() *CSVProcessor {
	return &CSVProcessor{}
}

// ConvertToCSV 将SQL行转换为CSV格式
func (cp *CSVProcessor) ConvertToCSV(rows *sql.Rows, config v1.CSVConfig) ([]byte, int64, error) {
	var buffer bytes.Buffer
	writer := csv.NewWriter(&buffer)

	// 如果指定了自定义分隔符，设置分隔符
	if config.Delimiter != "" {
		writer.Comma = rune(config.Delimiter[0])
	}

	columns, err := rows.Columns()
	if err != nil {
		return nil, 0, fmt.Errorf("获取列失败: %v", err)
	}

	// 如果启用了标题，写入标题
	includeHeader := true
	if config.IncludeHeader != nil {
		includeHeader = *config.IncludeHeader
	}

	if includeHeader {
		if err := writer.Write(columns); err != nil {
			return nil, 0, fmt.Errorf("写入标题失败: %v", err)
		}
	}

	// 准备值指针
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	var rowCount int64

	// 处理每一行
	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, 0, fmt.Errorf("扫描行失败: %v", err)
		}

		record := make([]string, len(columns))
		for i, val := range values {
			record[i] = cp.formatValue(val, config.DateFormat)
		}

		if err := writer.Write(record); err != nil {
			return nil, 0, fmt.Errorf("写入记录失败: %v", err)
		}

		rowCount++
	}

	writer.Flush()

	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("迭代行时出错: %v", err)
	}

	return buffer.Bytes(), rowCount, nil
}

// formatValue为CSV 输出格式化值
func (cp *CSVProcessor) formatValue(value interface{}, dateFormat string) string {
	if value == nil {
		return ""
	}

	switch v := value.(type) {
	case time.Time:
		if dateFormat != "" {
			return v.Format(dateFormat)
		}
		return v.Format(time.RFC3339)
	case []byte:
		return string(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// ValidateCSVConfig 验证CSV配置
func (cp *CSVProcessor) ValidateCSVConfig(config v1.CSVConfig) error {
	if config.FilenamePrefix == "" {
		return fmt.Errorf("filenamePrefix是必需的")
	}

	if config.Delimiter != "" && len(config.Delimiter) != 1 {
		return fmt.Errorf("分隔符必须是单个字符")
	}

	return nil
}

// GenerateFilename 生成带时间戳的文件名
func (cp *CSVProcessor) GenerateFilename(prefix string) string {
	timestamp := time.Now().Format("20060102-150405")
	return fmt.Sprintf("%s-%s.csv", prefix, timestamp)
}

// GenerateFilenameWithPart 生成带部分编号的文件名
func (cp *CSVProcessor) GenerateFilenameWithPart(prefix string, part int) string {
	timestamp := time.Now().Format("20060102-150405")
	return fmt.Sprintf("%s-%s-part%d.csv", prefix, timestamp, part)
}
