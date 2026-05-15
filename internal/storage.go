package internal

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type StorageChecker struct {
	s3Client *s3.Client
}

func NewStorageChecker(ctx context.Context) (*StorageChecker, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			"minioadmin", "minioadmin", "",
		)),
	)
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String("http://localhost:9000")
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