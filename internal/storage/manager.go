package storage

import (
	"bytes"
	"context"
	"fmt"
	v1 "github.com/hua-ri/sql-executor-operator/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// StorageManager 处理文件上传到各种存储目的地
type storageManager struct {
	client client.Client
}

// NewStorageManager 创建新的存储管理器
func NewStorageManager(client client.Client) Storage {
	return &storageManager{
		client: client,
	}
}

// UploadToDestinations 将数据上传到所有配置的目的地
func (sm *storageManager) UploadToDestinations(
	ctx context.Context,
	namespace string,
	data []byte,
	filename string,
	rowCount int64,
	destinations []v1.Destination,
) ([]v1.ExportResult, error) {

	var results []v1.ExportResult

	for _, dest := range destinations {
		var result v1.ExportResult
		var err error

		switch dest.Type {
		case v1.AliOSS:
			result, err = sm.uploadToAliOSS(ctx, namespace, data, filename, rowCount, *dest.AliOSS)
		case v1.TencentCOS:
			result, err = sm.uploadToTencentCOS(ctx, namespace, data, filename, rowCount, *dest.TencentCOS)
		default:
			err = fmt.Errorf("不支持的目的地类型: %s", dest.Type)
		}

		if err != nil {
			return nil, fmt.Errorf("上传到 %s 失败: %v", dest.Type, err)
		}

		result.DestinationType = dest.Type
		results = append(results, result)
	}

	return results, nil
}

// uploadToAliOSS 上传数据到阿里云OSS
func (sm *storageManager) uploadToAliOSS(
	ctx context.Context,
	namespace string,
	data []byte,
	filename string,
	rowCount int64,
	config v1.AliOSSConfig,
) (v1.ExportResult, error) {

	// 从Secret获取凭据
	accessKey, secretKey, err := sm.getAliOSSCredentials(ctx, namespace, config.SecretRef)
	if err != nil {
		return v1.ExportResult{}, err
	}

	// 初始化OSS客户端
	ossClient, err := NewAliOSSClient(config.Endpoint, config.Region, accessKey, secretKey)
	if err != nil {
		return v1.ExportResult{}, err
	}

	// 构建对象路径
	objectPath := filename
	if config.PathPrefix != "" {
		objectPath = config.PathPrefix + "/" + filename
	}

	// 上传文件
	location, fileSize, err := ossClient.UploadFile(config.Bucket, objectPath, bytes.NewReader(data))
	if err != nil {
		return v1.ExportResult{}, err
	}

	return v1.ExportResult{
		Location:     location,
		RowsExported: rowCount,
		FileSize:     fileSize,
		UploadTime:   metav1.Time{Time: time.Now()},
	}, nil
}

// uploadToTencentCOS 上传数据到腾讯云COS
func (sm *storageManager) uploadToTencentCOS(
	ctx context.Context,
	namespace string,
	data []byte,
	filename string,
	rowCount int64,
	config v1.TencentCOSConfig,
) (v1.ExportResult, error) {

	// 从Secret获取凭据
	secretID, secretKey, err := sm.getTencentCOSCredentials(ctx, namespace, config.SecretRef)
	if err != nil {
		return v1.ExportResult{}, err
	}

	// 初始化COS客户端
	cosClient, err := NewTencentCOSClient(config.Region, secretID, secretKey)
	if err != nil {
		return v1.ExportResult{}, err
	}

	// 构建对象路径
	objectPath := filename
	if config.PathPrefix != "" {
		objectPath = config.PathPrefix + "/" + filename
	}

	// 上传文件
	location, fileSize, err := cosClient.UploadFile(config.Bucket, objectPath, bytes.NewReader(data))
	if err != nil {
		return v1.ExportResult{}, err
	}

	return v1.ExportResult{
		Location:     location,
		RowsExported: rowCount,
		FileSize:     fileSize,
		UploadTime:   metav1.Time{Time: time.Now()},
	}, nil
}

// 辅助方法从Secret获取凭据
func (sm *storageManager) getAliOSSCredentials(ctx context.Context, namespace string, secretRef v1.SecretRef) (string, string, error) {
	secret := &corev1.Secret{}
	err := sm.client.Get(ctx, types.NamespacedName{
		Namespace: namespace,
		Name:      secretRef.Name,
	}, secret)
	if err != nil {
		return "", "", fmt.Errorf("获取Secret %s 失败: %v", secretRef.Name, err)
	}

	accessKeyKey := "accessKey"
	secretKeyKey := "secretKey"

	if secretRef.Key != "" {
		accessKeyKey = secretRef.Key + "-accessKey"
		secretKeyKey = secretRef.Key + "-secretKey"
	}

	accessKey := string(secret.Data[accessKeyKey])
	secretKey := string(secret.Data[secretKeyKey])

	// 回退到默认键名
	if accessKey == "" {
		accessKey = string(secret.Data["accessKey"])
	}
	if secretKey == "" {
		secretKey = string(secret.Data["secretKey"])
	}

	if accessKey == "" || secretKey == "" {
		return "", "", fmt.Errorf("在Secret %s 中缺少accessKey或secretKey", secretRef.Name)
	}

	return accessKey, secretKey, nil
}

func (sm *storageManager) getTencentCOSCredentials(ctx context.Context, namespace string, secretRef v1.SecretRef) (string, string, error) {
	secret := &corev1.Secret{}
	err := sm.client.Get(ctx, types.NamespacedName{
		Namespace: namespace,
		Name:      secretRef.Name,
	}, secret)
	if err != nil {
		return "", "", fmt.Errorf("获取Secret %s 失败: %v", secretRef.Name, err)
	}

	accessKeyKey := "accessKey"
	secretKeyKey := "secretKey"

	if secretRef.Key != "" {
		accessKeyKey = secretRef.Key + "-accessKey"
		secretKeyKey = secretRef.Key + "-secretKey"
	}

	secretID := string(secret.Data[accessKeyKey])
	secretKey := string(secret.Data[secretKeyKey])

	// 回退到默认键名
	if secretID == "" {
		secretID = string(secret.Data["accessKey"])
	}
	if secretKey == "" {
		secretKey = string(secret.Data["secretKey"])
	}

	if secretID == "" || secretKey == "" {
		return "", "", fmt.Errorf("在Secret %s 中缺少accessKey或secretKey", secretRef.Name)
	}

	return secretID, secretKey, nil
}
