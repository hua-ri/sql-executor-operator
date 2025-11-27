package storage

import (
	"errors"
	"fmt"
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"io"
)

// aliOSSClient 处理阿里云OSS操作
type aliOSSClient struct {
	client *oss.Client
	region string
}

// NewAliOSSClient 创建新的阿里云OSS客户端
func NewAliOSSClient(endpoint, region, accessKey, secretKey string) (OSSProvider, error) {
	// 如果没有提供endpoint，从region构造
	if endpoint == "" {
		endpoint = fmt.Sprintf("oss-%s.aliyuncs.com", region)
	}

	client, err := oss.New(endpoint, accessKey, secretKey)
	if err != nil {
		return nil, fmt.Errorf("创建OSS客户端失败: %v", err)
	}

	return &aliOSSClient{
		client: client,
		region: region,
	}, nil
}

// UploadFile 上传文件到OSS
func (c *aliOSSClient) UploadFile(bucketName, objectName string, reader io.Reader) (string, int64, error) {
	bucket, err := c.client.Bucket(bucketName)
	if err != nil {
		return "", 0, fmt.Errorf("获取bucket %s 失败: %v", bucketName, err)
	}

	// 上传文件
	err = bucket.PutObject(objectName, reader)
	if err != nil {
		return "", 0, fmt.Errorf("上传对象 %s 失败: %v", objectName, err)
	}

	// 获取文件信息以确定大小
	props, err := bucket.GetObjectMeta(objectName)
	if err != nil {
		return "", 0, fmt.Errorf("获取对象元数据失败: %v", err)
	}

	// 构造公开URL（在生产环境中可能需要使用签名URL）
	location := fmt.Sprintf("https://%s.%s/%s", bucketName, c.client.Config.Endpoint, objectName)

	// 解析内容长度
	var fileSize int64
	if contentLength, ok := props["Content-Length"]; ok && len(contentLength) > 0 {
		fmt.Sscanf(contentLength[0], "%d", &fileSize)
	}

	return location, fileSize, nil
}

// CheckBucketExists 检查bucket是否存在
func (c *aliOSSClient) CheckBucketExists(bucketName string) (bool, error) {
	_, err := c.client.GetBucketInfo(bucketName)
	if err != nil {
		var ossErr oss.ServiceError
		if errors.As(err, &ossErr) {
			if ossErr.StatusCode == 404 {
				return false, nil
			}
		}
		return false, fmt.Errorf("检查bucket存在性失败: %v", err)
	}
	return true, nil
}

// ListObjects 列出bucket中的对象
func (c *aliOSSClient) ListObjects(bucketName, prefix string) ([]string, error) {
	bucket, err := c.client.Bucket(bucketName)
	if err != nil {
		return nil, fmt.Errorf("获取bucket %s 失败: %v", bucketName, err)
	}

	var objects []string
	marker := oss.Marker("")
	pre := oss.Prefix(prefix)

	for {
		lor, err := bucket.ListObjects(marker, pre)
		if err != nil {
			return nil, fmt.Errorf("列出对象失败: %v", err)
		}

		for _, object := range lor.Objects {
			objects = append(objects, object.Key)
		}

		pre = oss.Prefix(lor.Prefix)
		marker = oss.Marker(lor.NextMarker)
		if !lor.IsTruncated {
			break
		}
	}

	return objects, nil
}

// DeleteObject 删除对象
func (c *aliOSSClient) DeleteObject(bucketName, objectName string) error {
	bucket, err := c.client.Bucket(bucketName)
	if err != nil {
		return fmt.Errorf("获取bucket %s 失败: %v", bucketName, err)
	}

	err = bucket.DeleteObject(objectName)
	if err != nil {
		return fmt.Errorf("删除对象 %s 失败: %v", objectName, err)
	}

	return nil
}
