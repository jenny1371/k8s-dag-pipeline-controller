package internal

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type StorageChecker struct {
	s3Client *s3.Client
}

// getEnv returns the value of the environment variable key, or fallback if unset/empty.
func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// NewStorageChecker builds an S3 client for MinIO/S3. It reads:
//   MINIO_ENDPOINT    (default "http://localhost:9000")
//   MINIO_ACCESS_KEY  (default "minioadmin")
//   MINIO_SECRET_KEY  (default "minioadmin")
func NewStorageChecker(ctx context.Context) (*StorageChecker, error) {
	endpoint := getEnv("MINIO_ENDPOINT", "http://localhost:9000")
	accessKey := getEnv("MINIO_ACCESS_KEY", "minioadmin")
	secretKey := getEnv("MINIO_SECRET_KEY", "minioadmin")

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			accessKey, secretKey, "",
		)),
	)
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})

	return &StorageChecker{s3Client: client}, nil
}

func (sc *StorageChecker) MarkerExists(ctx context.Context, markerPath string) (bool, error) {
	bucket, key, err := parseS3Path(markerPath)
	if err != nil {
		return false, err
	}

	_, err = sc.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return false, nil
	}
	return true, nil
}

func parseS3Path(path string) (string, string, error) {
	trimmed := strings.TrimPrefix(path, "s3://")
	parts := strings.SplitN(trimmed, "/", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("無效的 S3 路徑: %s", path)
	}
	return parts[0], parts[1], nil
}