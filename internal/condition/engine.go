package condition

import (
	"fmt"
	v1 "github.com/hua-ri/sql-executor-operator/api/v1"
	"strconv"
	"strings"
)

// Engine 评估执行条件
type engine struct {
	variables    map[string]string
	stageHistory map[string]*v1.StageResult
}

// NewEngine 创建新的条件引擎
func NewEngine(variables map[string]string, stageHistory map[string]*v1.StageResult) Engine {
	return &engine{
		variables:    variables,
		stageHistory: stageHistory,
	}
}

// EvaluateConditions 评估阶段的所有条件
func (e *engine) EvaluateConditions(conditions []v1.Condition, currentStage string) ([]v1.ConditionResult, bool, error) {
	var results []v1.ConditionResult
	allMet := true

	for _, condition := range conditions {
		result, err := e.evaluateCondition(condition, currentStage)
		if err != nil {
			return nil, false, err
		}

		results = append(results, result)
		if !result.Met {
			allMet = false
		}
	}

	return results, allMet, nil
}

// evaluateCondition 评估单个条件
func (e *engine) evaluateCondition(condition v1.Condition, currentStage string) (v1.ConditionResult, error) {
	result := v1.ConditionResult{
		Description: string(condition.Type),
	}

	switch condition.Type {
	case v1.ConditionAlways:
		result.Met = true
		result.Message = "条件始终满足"

	case v1.ConditionOnSuccess:
		if stage, exists := e.stageHistory[condition.Stage]; exists {
			result.Met = (stage.Phase == v1.StageSucceeded)
			result.Message = fmt.Sprintf("阶段 %s: %s", condition.Stage, stage.Phase)
		} else {
			result.Met = false
			result.Message = fmt.Sprintf("阶段 %s 在历史记录中未找到", condition.Stage)
		}

	case v1.ConditionOnFailure:
		if stage, exists := e.stageHistory[condition.Stage]; exists {
			result.Met = (stage.Phase == v1.StageFailed)
			result.Message = fmt.Sprintf("阶段 %s: %s", condition.Stage, stage.Phase)
		} else {
			result.Met = false
			result.Message = fmt.Sprintf("阶段 %s 在历史记录中未找到", condition.Stage)
		}

	case v1.ConditionOnRowsAffected:
		if stage, exists := e.stageHistory[condition.Stage]; exists {
			if stage.ExecutionStats != nil {
				rowsAffected := stage.ExecutionStats.RowsAffected
				if condition.ExpectedValue != nil {
					expected, err := strconv.ParseInt(*condition.ExpectedValue, 10, 64)
					if err != nil {
						return result, fmt.Errorf("影响行数的期望值无效: %v", err)
					}
					result.Met = (rowsAffected == expected)
					result.Message = fmt.Sprintf("影响行数: %d, 期望: %d", rowsAffected, expected)
				} else if condition.Expression != "" {
					// 评估表达式如 "> 0", ">= 10" 等
					met, err := e.evaluateRowsExpression(rowsAffected, condition.Expression)
					if err != nil {
						return result, err
					}
					result.Met = met
					result.Message = fmt.Sprintf("影响行数: %d, 表达式: %s", rowsAffected, condition.Expression)
				}
			} else {
				result.Met = false
				result.Message = "无执行统计信息可用"
			}
		} else {
			result.Met = false
			result.Message = fmt.Sprintf("阶段 %s 在历史记录中未找到", condition.Stage)
		}

	case v1.ConditionCustom:
		if condition.Script != nil {
			met, err := e.evaluateCustomScript(*condition.Script)
			if err != nil {
				return result, err
			}
			result.Met = met
			result.Message = "自定义脚本已评估"
		} else {
			result.Met = false
			result.Message = "未提供自定义脚本"
		}

	default:
		return result, fmt.Errorf("不支持的条件类型: %s", condition.Type)
	}

	return result, nil
}

// evaluateRowsExpression 评估行数表达式
func (e *engine) evaluateRowsExpression(rows int64, expression string) (bool, error) {
	expression = strings.TrimSpace(expression)

	switch {
	case strings.HasPrefix(expression, ">="):
		value, err := strconv.ParseInt(strings.TrimSpace(expression[2:]), 10, 64)
		if err != nil {
			return false, err
		}
		return rows >= value, nil

	case strings.HasPrefix(expression, "<="):
		value, err := strconv.ParseInt(strings.TrimSpace(expression[2:]), 10, 64)
		if err != nil {
			return false, err
		}
		return rows <= value, nil

	case strings.HasPrefix(expression, ">"):
		value, err := strconv.ParseInt(strings.TrimSpace(expression[1:]), 10, 64)
		if err != nil {
			return false, err
		}
		return rows > value, nil

	case strings.HasPrefix(expression, "<"):
		value, err := strconv.ParseInt(strings.TrimSpace(expression[1:]), 10, 64)
		if err != nil {
			return false, err
		}
		return rows < value, nil

	case strings.HasPrefix(expression, "=="):
		value, err := strconv.ParseInt(strings.TrimSpace(expression[2:]), 10, 64)
		if err != nil {
			return false, err
		}
		return rows == value, nil

	case strings.HasPrefix(expression, "!="):
		value, err := strconv.ParseInt(strings.TrimSpace(expression[2:]), 10, 64)
		if err != nil {
			return false, err
		}
		return rows != value, nil

	default:
		return false, fmt.Errorf("不支持的表达式格式: %s", expression)
	}
}

// evaluateCustomScript 评估自定义条件脚本
func (e *engine) evaluateCustomScript(script string) (bool, error) {
	// 简单的模板变量替换
	evaluatedScript := script
	for key, value := range e.variables {
		placeholder := fmt.Sprintf("${%s}", key)
		evaluatedScript = strings.ReplaceAll(evaluatedScript, placeholder, value)
	}

	// 对于生产环境，您可能希望与脚本引擎集成
	// 如 Goja (JavaScript) 或 Starlark (Python-like)
	// 这是一个简化的实现
	return e.evaluateSimpleExpression(evaluatedScript)
}

// evaluateSimpleExpression 评估简单的布尔表达式
func (e *engine) evaluateSimpleExpression(expr string) (bool, error) {
	// 移除空格
	expr = strings.ReplaceAll(expr, " ", "")

	// 简单的 true/false 评估
	switch strings.ToLower(expr) {
	case "true", "1", "yes":
		return true, nil
	case "false", "0", "no":
		return false, nil
	default:
		// 尝试解析为比较
		if strings.Contains(expr, "==") {
			parts := strings.Split(expr, "==")
			if len(parts) == 2 {
				return parts[0] == parts[1], nil
			}
		} else if strings.Contains(expr, "!=") {
			parts := strings.Split(expr, "!=")
			if len(parts) == 2 {
				return parts[0] != parts[1], nil
			}
		}
		return false, fmt.Errorf("无法评估表达式: %s", expr)
	}
}

// ExtractVariables 从阶段结果中提取变量
func (e *engine) ExtractVariables(stageResult v1.StageResult) map[string]string {
	variables := make(map[string]string)

	// 从执行统计中提取变量
	if stageResult.ExecutionStats != nil {
		variables[stageResult.Name+"_rows_affected"] = fmt.Sprintf("%d", stageResult.ExecutionStats.RowsAffected)
		variables[stageResult.Name+"_execution_time_ms"] = fmt.Sprintf("%d", stageResult.ExecutionStats.ExecutionTime)
	}

	// 从导出结果中提取变量
	if stageResult.ExportResults != nil && len(stageResult.ExportResults) > 0 {
		totalRows := int64(0)
		for _, result := range stageResult.ExportResults {
			totalRows += result.RowsExported
		}
		variables[stageResult.Name+"_rows_exported"] = fmt.Sprintf("%d", totalRows)
	}

	// 添加自定义变量
	if stageResult.Variables != nil {
		for k, v := range stageResult.Variables {
			variables[stageResult.Name+"_"+k] = v
		}
	}

	return variables
}
