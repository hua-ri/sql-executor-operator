package condition

import v1 "github.com/hua-ri/sql-executor-operator/api/v1"

type Engine interface {
	EvaluateConditions(conditions []v1.Condition, currentStage string) ([]v1.ConditionResult, bool, error)
	ExtractVariables(stageResult v1.StageResult) map[string]string
}
