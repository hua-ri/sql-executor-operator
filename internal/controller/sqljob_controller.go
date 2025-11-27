package controller

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	batchv1 "github.com/hua-ri/sql-executor-operator/api/v1"
	"github.com/hua-ri/sql-executor-operator/internal/condition"
	"github.com/hua-ri/sql-executor-operator/internal/csv_processor"
	"github.com/hua-ri/sql-executor-operator/internal/database"
	"github.com/hua-ri/sql-executor-operator/internal/storage"
	"text/template"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	// sqlJobFinalizer用于在删除SqlJob时进行清理
	sqlJobFinalizer = "batch.yourcompany.com/finalizer"
)

// SqlJobReconciler 协调SqlJob对象
type SqlJobReconciler struct {
	client.Client
	Log    logr.Logger
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=batch.yourcompany.com,resources=sqljobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch.yourcompany.com,resources=sqljobs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=batch.yourcompany.com,resources=sqljobs/finalizers,verbs=update
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch

// Reconcile 是主要的协调循环
func (r *SqlJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithValues("sqljob", req.NamespacedName)

	// 获取SqlJob实例
	var sqlJob batchv1.SqlJob
	if err := r.Get(ctx, req.NamespacedName, &sqlJob); err != nil {
		if errors.IsNotFound(err) {
			// SqlJob已被删除
			log.Info("SqlJob资源未找到，可能已被删除")
			return ctrl.Result{}, nil
		}

		// 读取对象时出错
		log.Error(err, "获取SqlJob失败")
		return ctrl.Result{}, err
	}

	// 检查对象是否正在被删除
	if !sqlJob.ObjectMeta.DeletionTimestamp.IsZero() {
		return r.finalizeSqlJob(ctx, &sqlJob, log)
	}

	// 如果不存在，添加finalizer
	if !controllerutil.ContainsFinalizer(&sqlJob, sqlJobFinalizer) {
		controllerutil.AddFinalizer(&sqlJob, sqlJobFinalizer)
		if err := r.Update(ctx, &sqlJob); err != nil {
			log.Error(err, "添加finalizer失败")
			return ctrl.Result{}, err
		}
	}

	// 如果作业已完成，跳过
	if sqlJob.Status.Phase == batchv1.JobSucceeded || sqlJob.Status.Phase == batchv1.JobFailed {
		log.Info("作业已完成", "phase", sqlJob.Status.Phase)
		return ctrl.Result{}, nil
	}

	// 初始化状态（如果需要）
	if sqlJob.Status.StageHistory == nil {
		sqlJob.Status.StageHistory = []batchv1.StageResult{}
	}
	if sqlJob.Status.ComputedVariables == nil {
		sqlJob.Status.ComputedVariables = make(map[string]string)
	}

	// 更新全局变量
	r.updateGlobalVariables(&sqlJob)

	// 执行阶段
	result, err := r.executeStages(ctx, &sqlJob, log)
	if err != nil {
		log.Error(err, "执行阶段失败")
		return r.handleExecutionError(ctx, &sqlJob, err)
	}

	return result, nil
}

// executeStages 按顺序执行所有阶段
func (r *SqlJobReconciler) executeStages(ctx context.Context, sqlJob *batchv1.SqlJob, log logr.Logger) (ctrl.Result, error) {
	for i, stage := range sqlJob.Spec.Stages {
		// 检查阶段是否已执行
		if r.isStageExecuted(sqlJob, stage.Name) {
			log.Info("阶段已执行，跳过", "stage", stage.Name)
			continue
		}

		// 更新当前阶段
		sqlJob.Status.CurrentStage = stage.Name
		if err := r.Status().Update(ctx, sqlJob); err != nil {
			log.Error(err, "更新当前阶段失败")
			return ctrl.Result{}, err
		}

		// 评估此阶段的条件
		conditionResults, shouldExecute, err := r.evaluateStageConditions(sqlJob, stage)
		if err != nil {
			log.Error(err, "评估阶段条件失败", "stage", stage.Name)
			return ctrl.Result{}, fmt.Errorf("评估阶段 %s 的条件失败: %v", stage.Name, err)
		}

		var stageResult batchv1.StageResult
		if !shouldExecute {
			// 跳过阶段
			log.Info("跳过阶段", "stage", stage.Name, "reason", "条件未满足")
			stageResult = batchv1.StageResult{
				Name:             stage.Name,
				Phase:            batchv1.StageSkipped,
				Message:          "由于条件未满足，跳过阶段",
				StartTime:        &metav1.Time{Time: time.Now()},
				CompletionTime:   &metav1.Time{Time: time.Now()},
				ConditionResults: conditionResults,
			}
		} else {
			// 执行阶段
			log.Info("执行阶段", "stage", stage.Name)
			stageResult, err = r.executeStage(ctx, sqlJob, stage, log)
			if err != nil {
				log.Error(err, "执行阶段失败", "stage", stage.Name)

				// 如果配置了回滚，尝试回滚
				rollbackErr := r.executeRollback(ctx, sqlJob, stage, log)
				if rollbackErr != nil {
					log.Error(rollbackErr, "执行阶段回滚失败", "stage", stage.Name)
				} else {
					log.Info("阶段回滚成功", "stage", stage.Name)
					stageResult.Phase = batchv1.StageRolledBack
				}

				stageResult.Message = fmt.Sprintf("阶段执行失败: %v", err)
			}

			stageResult.ConditionResults = conditionResults
		}

		// 更新阶段历史
		sqlJob.Status.StageHistory = append(sqlJob.Status.StageHistory, stageResult)

		// 从此阶段更新计算变量
		if stageResult.Variables != nil {
			for k, v := range stageResult.Variables {
				sqlJob.Status.ComputedVariables[k] = v
			}
		}

		// 更新作业状态
		if stageResult.Phase == batchv1.StageFailed || stageResult.Phase == batchv1.StageRolledBack {
			sqlJob.Status.Phase = batchv1.JobFailed
			sqlJob.Status.Message = fmt.Sprintf("阶段 %s 失败", stage.Name)
			sqlJob.Status.CompletionTime = &metav1.Time{Time: time.Now()}
		} else if i == len(sqlJob.Spec.Stages)-1 && stageResult.Phase == batchv1.StageSucceeded {
			// 最后一个阶段成功
			sqlJob.Status.Phase = batchv1.JobSucceeded
			sqlJob.Status.Message = "所有阶段成功完成"
			sqlJob.Status.CompletionTime = &metav1.Time{Time: time.Now()}
		} else if stageResult.Phase == batchv1.StageSucceeded {
			// 阶段成功，继续下一个
			sqlJob.Status.Phase = batchv1.JobRunning
			sqlJob.Status.Message = fmt.Sprintf("阶段 %s 成功完成", stage.Name)
		}

		// 更新状态
		if err := r.Status().Update(ctx, sqlJob); err != nil {
			log.Error(err, "更新SqlJob状态失败")
			return ctrl.Result{}, err
		}

		// 如果阶段失败，停止执行
		if stageResult.Phase == batchv1.StageFailed {
			return ctrl.Result{}, nil
		}
	}

	return ctrl.Result{}, nil
}

// executeStage 执行单个阶段
func (r *SqlJobReconciler) executeStage(ctx context.Context, sqlJob *batchv1.SqlJob, stage batchv1.Stage, log logr.Logger) (batchv1.StageResult, error) {
	stageResult := batchv1.StageResult{
		Name:      stage.Name,
		Phase:     batchv1.StageRunning,
		StartTime: &metav1.Time{Time: time.Now()},
	}

	// 获取数据库配置
	dbConfig := r.getStageDatabaseConfig(sqlJob, stage)
	if dbConfig == nil {
		return stageResult, fmt.Errorf("阶段 %s 没有数据库配置", stage.Name)
	}

	// 获取数据库密码
	password, err := r.getPasswordFromSecret(ctx, sqlJob.Namespace, dbConfig.PasswordSecret)
	if err != nil {
		return stageResult, fmt.Errorf("获取数据库密码失败: %v", err)
	}

	// 连接到数据库
	dbManager := database.NewDatabaseManager()
	db, err := dbManager.Connect(*dbConfig, password)
	if err != nil {
		return stageResult, fmt.Errorf("连接到数据库失败: %v", err)
	}
	defer dbManager.Close(db)

	// 根据操作类型执行
	switch stage.Type {
	case batchv1.OperationQuery:
		stageResult, err = r.executeQueryStage(ctx, sqlJob, stage, db, log)
	case batchv1.OperationExecute, batchv1.OperationDML, batchv1.OperationDDL:
		stageResult, err = r.executeChangeStage(sqlJob, stage, db, log)
	default:
		err = fmt.Errorf("不支持的操作类型: %s", stage.Type)
	}

	if err != nil {
		stageResult.Phase = batchv1.StageFailed
		stageResult.Message = err.Error()
	} else {
		stageResult.Phase = batchv1.StageSucceeded
		stageResult.CompletionTime = &metav1.Time{Time: time.Now()}
	}

	return stageResult, err
}

// renderTemplate 渲染 SQL 模板
func (r *SqlJobReconciler) renderTemplate(sql string, variables map[string]string) (string, error) {
	tmpl, err := template.New("sql").Parse(sql)
	if err != nil {
		return "", err
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, variables); err != nil {
		return "", err
	}

	return buf.String(), nil
}

// executeQueryStage 执行查询阶段并导出
func (r *SqlJobReconciler) executeQueryStage(ctx context.Context, sqlJob *batchv1.SqlJob, stage batchv1.Stage, db *sql.DB, log logr.Logger) (batchv1.StageResult, error) {
	stageResult := batchv1.StageResult{
		Name:      stage.Name,
		Phase:     batchv1.StageRunning,
		StartTime: &metav1.Time{Time: time.Now()},
	}

	// 获取合并的变量
	variables := r.getMergedVariables(sqlJob)

	// 执行每个查询
	var allExportResults []batchv1.ExportResult
	var totalRows int64

	for i, query := range stage.Queries {
		// 渲染模板
		renderedQuery, err := r.renderTemplate(query, variables)
		if err != nil {
			return stageResult, fmt.Errorf("渲染查询 %d 的模板失败: %v", i+1, err)
		}

		log.Info("执行查询", "stage", stage.Name, "query", i+1, "renderedQuery", renderedQuery)

		// 执行查询
		rows, err := db.Query(renderedQuery)
		if err != nil {
			return stageResult, fmt.Errorf("执行查询 %d 失败: %v", i+1, err)
		}

		// 如果有输出配置，处理导出
		if stage.Output != nil {
			// 验证CSV配置
			csvProcessor := csv_processor.NewCSVProcessor()
			if err := csvProcessor.ValidateCSVConfig(stage.Output.CSV); err != nil {
				rows.Close()
				return stageResult, fmt.Errorf("CSV配置无效: %v", err)
			}

			// 转换为CSV
			csvData, rowCount, err := csvProcessor.ConvertToCSV(rows, stage.Output.CSV)
			rows.Close()

			if err != nil {
				return stageResult, fmt.Errorf("将查询 %d 的结果转换为CSV失败: %v", i+1, err)
			}

			// 生成文件名
			filename := csvProcessor.GenerateFilename(stage.Output.CSV.FilenamePrefix)
			if len(stage.Queries) > 1 {
				filename = csvProcessor.GenerateFilenameWithPart(stage.Output.CSV.FilenamePrefix, i+1)
			}

			// 上传到目的地
			storageManager := storage.NewStorageManager(r.Client)
			exportResults, err := storageManager.UploadToDestinations(
				ctx,
				sqlJob.Namespace,
				csvData,
				filename,
				rowCount,
				stage.Output.Destinations,
			)
			if err != nil {
				return stageResult, fmt.Errorf("上传查询 %d 的结果失败: %v", i+1, err)
			}

			allExportResults = append(allExportResults, exportResults...)
			totalRows += rowCount
		} else {
			// 没有输出配置，只执行查询并将前100条记录记录到日志
			csvData, rowCount, err := r.convertRowsToCSVWithLimit(rows, 100, log)
			rows.Close()

			if err != nil {
				return stageResult, fmt.Errorf("处理查询 %d 的结果失败: %v", i+1, err)
			}

			// 记录到日志
			if len(csvData) > 0 {
				log.Info("查询结果样本",
					"stage", stage.Name,
					"query", i+1,
					"totalRows", rowCount,
					"sampleRows", len(csvData)-1, // 减去标题行
					"csvData", string(csvData))
			} else {
				log.Info("查询结果为空",
					"stage", stage.Name,
					"query", i+1)
			}

			totalRows += rowCount
		}
	}

	stageResult.ExportResults = allExportResults
	stageResult.Variables = map[string]string{
		"rows_exported": fmt.Sprintf("%d", totalRows),
	}

	return stageResult, nil
}

// convertRowsToCSVWithLimit 将查询结果转换为CSV格式，限制最大行数
func (r *SqlJobReconciler) convertRowsToCSVWithLimit(rows *sql.Rows, maxRows int, log logr.Logger) ([]byte, int64, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, 0, fmt.Errorf("获取列名失败: %v", err)
	}

	// 准备CSV数据
	var csvData [][]string
	csvData = append(csvData, columns) // 添加标题行

	var rowCount int64
	buffer := &bytes.Buffer{}
	writer := csv.NewWriter(buffer)

	// 写入标题行
	if err := writer.Write(columns); err != nil {
		return nil, 0, fmt.Errorf("写入CSV标题失败: %v", err)
	}

	// 读取数据行
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	for rows.Next() {
		if rowCount >= int64(maxRows) && maxRows > 0 {
			break
		}

		err := rows.Scan(valuePtrs...)
		if err != nil {
			return nil, rowCount, fmt.Errorf("扫描行数据失败: %v", err)
		}

		// 转换值为字符串
		rowData := make([]string, len(columns))
		for i, val := range values {
			if val == nil {
				rowData[i] = "NULL"
			} else {
				switch v := val.(type) {
				case []byte:
					rowData[i] = string(v)
				case time.Time:
					rowData[i] = v.Format("2006-01-02 15:04:05")
				default:
					rowData[i] = fmt.Sprintf("%v", v)
				}
			}
		}

		// 写入CSV行
		if err := writer.Write(rowData); err != nil {
			return nil, rowCount, fmt.Errorf("写入CSV行失败: %v", err)
		}

		rowCount++
	}

	// 检查是否有读取错误
	if err := rows.Err(); err != nil {
		return nil, rowCount, fmt.Errorf("读取行时发生错误: %v", err)
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return nil, rowCount, fmt.Errorf("刷新CSV写入器失败: %v", err)
	}

	// 如果达到了限制，添加提示信息
	if maxRows > 0 && rowCount >= int64(maxRows) {
		buffer.WriteString(fmt.Sprintf("\n\n-- 注意: 只显示前 %d 条记录，总共 %d 条记录 --", maxRows, rowCount))
	}

	return buffer.Bytes(), rowCount, nil
}

// executeChangeStage 执行变更阶段（DDL/DML）
func (r *SqlJobReconciler) executeChangeStage(sqlJob *batchv1.SqlJob, stage batchv1.Stage, db *sql.DB, log logr.Logger) (batchv1.StageResult, error) {
	stageResult := batchv1.StageResult{
		Name:      stage.Name,
		Phase:     batchv1.StageRunning,
		StartTime: &metav1.Time{Time: time.Now()},
	}

	startTime := time.Now()
	var totalRowsAffected int64
	var queryResults []batchv1.QueryResult

	// 获取合并的变量
	variables := r.getMergedVariables(sqlJob)

	// 对于DML操作，开始事务（如果支持）
	var tx *sql.Tx
	var err error
	if stage.Type == batchv1.OperationDML {
		tx, err = db.Begin()
		if err != nil {
			return stageResult, fmt.Errorf("开始事务失败: %v", err)
		}
		defer func() {
			if err != nil && tx != nil {
				tx.Rollback()
			}
		}()
	}

	for i, query := range stage.Queries {
		// 渲染模板
		renderedQuery, err := r.renderTemplate(query, variables)
		if err != nil {
			return stageResult, fmt.Errorf("渲染查询 %d 的模板失败: %v", i+1, err)
		}

		log.Info("执行变更查询", "stage", stage.Name, "query", i+1, "renderedQuery", renderedQuery)

		var result sql.Result
		if tx != nil {
			result, err = tx.Exec(renderedQuery)
		} else {
			result, err = db.Exec(renderedQuery)
		}

		queryResult := batchv1.QueryResult{
			Index: int32(i + 1),
		}

		if err != nil {
			errorMsg := err.Error()
			queryResult.Error = &errorMsg
			queryResults = append(queryResults, queryResult)

			// 如果我们在事务中，回滚事务
			if tx != nil {
				tx.Rollback()
			}
			return stageResult, fmt.Errorf("执行查询 %d 失败: %v", i+1, err)
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			// 某些数据库不支持DDL的RowsAffected
			rowsAffected = 0
		}

		lastInsertID, err := result.LastInsertId()
		if err == nil {
			queryResult.LastInsertID = &lastInsertID
		}

		queryResult.RowsAffected = rowsAffected
		queryResults = append(queryResults, queryResult)
		totalRowsAffected += rowsAffected
	}

	// 如果我们有事务，提交事务
	if tx != nil {
		if err := tx.Commit(); err != nil {
			return stageResult, fmt.Errorf("提交事务失败: %v", err)
		}
	}

	// 验证期望
	if stage.Expectations != nil {
		if err := r.validateExpectations(stage.Expectations, totalRowsAffected); err != nil {
			return stageResult, fmt.Errorf("期望未满足: %v", err)
		}
	}

	executionTime := time.Since(startTime).Milliseconds()
	stageResult.ExecutionStats = &batchv1.ExecutionStats{
		RowsAffected:  totalRowsAffected,
		ExecutionTime: executionTime,
		QueryResults:  queryResults,
	}

	stageResult.Variables = map[string]string{
		"rows_affected":     fmt.Sprintf("%d", totalRowsAffected),
		"execution_time_ms": fmt.Sprintf("%d", executionTime),
	}

	return stageResult, nil
}

// 辅助方法...

// evaluateStageConditions 评估阶段的执行条件
func (r *SqlJobReconciler) evaluateStageConditions(sqlJob *batchv1.SqlJob, stage batchv1.Stage) ([]batchv1.ConditionResult, bool, error) {
	if len(stage.Conditions) == 0 {
		return nil, true, nil
	}

	// 为条件评估构建阶段历史映射
	stageHistory := make(map[string]*batchv1.StageResult)
	for i := range sqlJob.Status.StageHistory {
		stageHistory[sqlJob.Status.StageHistory[i].Name] = &sqlJob.Status.StageHistory[i]
	}

	// 合并全局变量和计算变量
	variables := make(map[string]string)
	if sqlJob.Spec.Variables != nil {
		for k, v := range sqlJob.Spec.Variables {
			variables[k] = v
		}
	}
	for k, v := range sqlJob.Status.ComputedVariables {
		variables[k] = v
	}

	engine := condition.NewEngine(variables, stageHistory)
	return engine.EvaluateConditions(stage.Conditions, stage.Name)
}

// validateExpectations 验证期望是否满足
func (r *SqlJobReconciler) validateExpectations(expectations *batchv1.Expectation, rowsAffected int64) error {
	if expectations.MinRowsAffected != nil && rowsAffected < *expectations.MinRowsAffected {
		return fmt.Errorf("影响行数 (%d) 小于最小期望 (%d)", rowsAffected, *expectations.MinRowsAffected)
	}

	if expectations.MaxRowsAffected != nil && rowsAffected > *expectations.MaxRowsAffected {
		return fmt.Errorf("影响行数 (%d) 大于最大期望 (%d)", rowsAffected, *expectations.MaxRowsAffected)
	}

	return nil
}

// executeRollback 执行阶段的回滚查询
func (r *SqlJobReconciler) executeRollback(ctx context.Context, sqlJob *batchv1.SqlJob, stage batchv1.Stage, log logr.Logger) error {
	if len(stage.RollbackQueries) == 0 {
		return nil
	}

	log.Info("执行回滚查询", "stage", stage.Name)

	// 获取数据库配置
	dbConfig := r.getStageDatabaseConfig(sqlJob, stage)
	if dbConfig == nil {
		return fmt.Errorf("阶段 %s 没有数据库配置", stage.Name)
	}

	// 获取数据库密码
	password, err := r.getPasswordFromSecret(ctx, sqlJob.Namespace, dbConfig.PasswordSecret)
	if err != nil {
		return fmt.Errorf("获取数据库密码失败: %v", err)
	}

	// 连接到数据库
	dbManager := database.NewDatabaseManager()
	db, err := dbManager.Connect(*dbConfig, password)
	if err != nil {
		return fmt.Errorf("连接到数据库失败: %v", err)
	}
	defer dbManager.Close(db)

	// 获取合并的变量
	variables := r.getMergedVariables(sqlJob)

	// 执行回滚查询
	for i, query := range stage.RollbackQueries {
		// 渲染模板
		renderedQuery, err := r.renderTemplate(query, variables)
		if err != nil {
			return fmt.Errorf("渲染回滚查询 %d 的模板失败: %v", i+1, err)
		}

		log.Info("执行回滚查询", "stage", stage.Name, "query", i+1, "renderedQuery", renderedQuery)
		_, err = db.Exec(renderedQuery)
		if err != nil {
			return fmt.Errorf("执行回滚查询 %d 失败: %v", i+1, err)
		}
	}

	return nil
}

// getMergedVariables 获取合并的变量（全局变量 + 计算变量）
func (r *SqlJobReconciler) getMergedVariables(sqlJob *batchv1.SqlJob) map[string]string {
	variables := make(map[string]string)

	// 添加全局变量
	if sqlJob.Spec.Variables != nil {
		for k, v := range sqlJob.Spec.Variables {
			variables[k] = v
		}
	}

	// 添加计算变量
	for k, v := range sqlJob.Status.ComputedVariables {
		variables[k] = v
	}

	return variables
}

// getStageDatabaseConfig 获取阶段的数据库配置
func (r *SqlJobReconciler) getStageDatabaseConfig(sqlJob *batchv1.SqlJob, stage batchv1.Stage) *batchv1.DatabaseConfig {
	if stage.Database != nil {
		return stage.Database
	}
	return sqlJob.Spec.Database
}

// isStageExecuted 检查阶段是否已执行
func (r *SqlJobReconciler) isStageExecuted(sqlJob *batchv1.SqlJob, stageName string) bool {
	for _, history := range sqlJob.Status.StageHistory {
		if history.Name == stageName {
			return true
		}
	}
	return false
}

// updateGlobalVariables 更新全局变量
func (r *SqlJobReconciler) updateGlobalVariables(sqlJob *batchv1.SqlJob) {
	if sqlJob.Spec.Variables != nil {
		for k, v := range sqlJob.Spec.Variables {
			sqlJob.Status.ComputedVariables[k] = v
		}
	}
}

// getPasswordFromSecret 从Kubernetes Secret获取密码
func (r *SqlJobReconciler) getPasswordFromSecret(ctx context.Context, namespace string, secretRef batchv1.SecretRef) (string, error) {
	secret := &corev1.Secret{}
	err := r.Get(ctx, types.NamespacedName{
		Namespace: namespace,
		Name:      secretRef.Name,
	}, secret)
	if err != nil {
		return "", fmt.Errorf("获取Secret %s 失败: %v", secretRef.Name, err)
	}

	key := secretRef.Key
	if key == "" {
		key = "password"
	}

	password := string(secret.Data[key])
	if password == "" {
		return "", fmt.Errorf("在Secret %s 的键 %s 中未找到密码", secretRef.Name, key)
	}

	return password, nil
}

// handleExecutionError 处理执行错误
func (r *SqlJobReconciler) handleExecutionError(ctx context.Context, sqlJob *batchv1.SqlJob, err error) (ctrl.Result, error) {
	sqlJob.Status.Phase = batchv1.JobFailed
	sqlJob.Status.Message = err.Error()
	sqlJob.Status.CompletionTime = &metav1.Time{Time: time.Now()}

	if updateErr := r.Status().Update(ctx, sqlJob); updateErr != nil {
		return ctrl.Result{}, updateErr
	}

	return ctrl.Result{}, nil
}

// finalizeSqlJob 处理SqlJob的最终化
func (r *SqlJobReconciler) finalizeSqlJob(ctx context.Context, sqlJob *batchv1.SqlJob, log logr.Logger) (ctrl.Result, error) {
	if controllerutil.ContainsFinalizer(sqlJob, sqlJobFinalizer) {
		// 在此执行任何清理
		log.Info("最终化SqlJob", "name", sqlJob.Name)

		controllerutil.RemoveFinalizer(sqlJob, sqlJobFinalizer)
		if err := r.Update(ctx, sqlJob); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, nil
}

// SetupWithManager 设置控制器与Manager
func (r *SqlJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&batchv1.SqlJob{}).
		Complete(r)
}
