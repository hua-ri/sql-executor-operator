package storage

import (
	"context"
	v1 "github.com/hua-ri/sql-executor-operator/api/v1"
	"io"
)

type OSSProvider interface {
	UploadFile(bucketName, objectName string, reader io.Reader) (string, int64, error)
	CheckBucketExists(bucketName string) (bool, error)
	ListObjects(bucketName, prefix string) ([]string, error)
	DeleteObject(bucketName, objectName string) error
}

type Storage interface {
	UploadToDestinations(
		ctx context.Context,
		namespace string,
		data []byte,
		filename string,
		rowCount int64,
		destinations []v1.Destination,
	) ([]v1.ExportResult, error)
}
