package storage

import (
	"context"
	"errors"
	"fmt"
	"github.com/tencentyun/cos-go-sdk-v5"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// tencentCOSClient 处理腾讯云COS操作
type tencentCOSClient struct {
	client *cos.Client
	region string
}

// NewTencentCOSClient 创建新的腾讯云COS客户端
func NewTencentCOSClient(region, secretID, secretKey string) (OSSProvider, error) {
	// 构造基础URL
	u, err := url.Parse(fmt.Sprintf("https://cos.%s.myqcloud.com", region))
	if err != nil {
		return nil, fmt.Errorf("解析COS URL失败: %v", err)
	}

	b := &cos.BaseURL{BucketURL: u}
	client := cos.NewClient(b, &http.Client{
		Transport: &cos.AuthorizationTransport{
			SecretID:  secretID,
			SecretKey: secretKey,
		},
	})

	return &tencentCOSClient{
		client: client,
		region: region,
	}, nil
}

// UploadFile 上传文件到COS
func (c *tencentCOSClient) UploadFile(bucketName, objectName string, reader io.Reader) (string, int64, error) {
	// 更新客户端使用实际的bucket
	u, err := url.Parse(fmt.Sprintf("https://%s.cos.%s.myqcloud.com", bucketName, c.region))
	if err != nil {
		return "", 0, fmt.Errorf("解析bucket URL失败: %v", err)
	}
	c.client.BaseURL.BucketURL = u

	// 上传文件
	opt := &cos.ObjectPutOptions{
		ObjectPutHeaderOptions: &cos.ObjectPutHeaderOptions{
			ContentType: "text/csv",
		},
	}

	resp, err := c.client.Object.Put(context.Background(), objectName, reader, opt)
	if err != nil {
		return "", 0, fmt.Errorf("上传对象 %s 失败: %v", objectName, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", 0, fmt.Errorf("上传失败，状态码: %s", resp.Status)
	}

	// 获取对象信息以确定大小
	headResp, err := c.client.Object.Head(context.Background(), objectName, nil)
	if err != nil {
		return "", 0, fmt.Errorf("获取对象信息失败: %v", err)
	}
	defer headResp.Body.Close()

	fileSize := headResp.ContentLength
	location := fmt.Sprintf("https://%s.cos.%s.myqcloud.com/%s", bucketName, c.region, objectName)

	return location, fileSize, nil
}

// CheckBucketExists 检查bucket是否存在
func (c *tencentCOSClient) CheckBucketExists(bucketName string) (bool, error) {
	u, err := url.Parse(fmt.Sprintf("https://%s.cos.%s.myqcloud.com", bucketName, c.region))
	if err != nil {
		return false, fmt.Errorf("解析bucket URL失败: %v", err)
	}
	c.client.BaseURL.BucketURL = u

	_, err = c.client.Bucket.Head(context.Background())
	if err != nil {
		var cosErr *cos.ErrorResponse
		if errors.As(err, &cosErr) {
			if cosErr.Response.StatusCode == 404 {
				return false, nil
			}
		}
		return false, fmt.Errorf("检查bucket存在性失败: %v", err)
	}
	return true, nil
}

// ListObjects 列出bucket中的对象
func (c *tencentCOSClient) ListObjects(bucketName, prefix string) ([]string, error) {
	u, err := url.Parse(fmt.Sprintf("https://%s.cos.%s.myqcloud.com", bucketName, c.region))
	if err != nil {
		return nil, fmt.Errorf("解析bucket URL失败: %v", err)
	}
	c.client.BaseURL.BucketURL = u

	var objects []string
	var marker string

	for {
		opt := &cos.BucketGetOptions{
			Prefix:  prefix,
			Marker:  marker,
			MaxKeys: 1000,
		}

		v, _, err := c.client.Bucket.Get(context.Background(), opt)
		if err != nil {
			return nil, fmt.Errorf("列出对象失败: %v", err)
		}

		for _, content := range v.Contents {
			objects = append(objects, content.Key)
		}

		// 检查是否还有更多对象
		if v.IsTruncated {
			marker = v.NextMarker
		} else {
			break
		}
	}

	return objects, nil
}

// DeleteObject 删除对象
func (c *tencentCOSClient) DeleteObject(bucketName, objectName string) error {
	u, err := url.Parse(fmt.Sprintf("https://%s.cos.%s.myqcloud.com", bucketName, c.region))
	if err != nil {
		return fmt.Errorf("解析bucket URL失败: %v", err)
	}
	c.client.BaseURL.BucketURL = u

	_, err = c.client.Object.Delete(context.Background(), objectName)
	if err != nil {
		return fmt.Errorf("删除对象 %s 失败: %v", objectName, err)
	}

	return nil
}

// getRegionFromURL 从URL中提取region
func (c *tencentCOSClient) getRegionFromURL(urlStr string) string {
	// 从COS URL中提取region
	// 实现取决于您的URL格式
	parts := strings.Split(urlStr, ".")
	if len(parts) >= 3 {
		return parts[2] // ap-beijing 等
	}
	return c.region // 默认
}
