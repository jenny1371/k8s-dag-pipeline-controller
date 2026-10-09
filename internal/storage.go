package internal

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
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
//
//	MINIO_ENDPOINT    (default "http://localhost:9000")
//	MINIO_ACCESS_KEY  (default "minioadmin")
//	MINIO_SECRET_KEY  (default "minioadmin")
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

// MarkerExists reports whether the marker object exists. A missing object is
// (false, nil); any other failure (network, credentials, ...) is returned as an
// error so it is not mistaken for "job not finished yet".
func (sc *StorageChecker) MarkerExists(ctx context.Context, markerPath string) (bool, error) {
	bucket, key, err := parseS3Path(markerPath)
	if err != nil {
		return false, err
	}

	_, err = sc.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err == nil {
		return true, nil
	}
	if isNotFound(err) {
		return false, nil
	}
	return false, fmt.Errorf("checking marker %s: %w", markerPath, err)
}

// ClearMarker deletes the marker object so a leftover marker from a previous
// run cannot make a new run look finished. Deleting a missing object is not an error.
func (sc *StorageChecker) ClearMarker(ctx context.Context, markerPath string) error {
	bucket, key, err := parseS3Path(markerPath)
	if err != nil {
		return err
	}
	_, err = sc.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil && !isNotFound(err) {
		return fmt.Errorf("clearing marker %s: %w", markerPath, err)
	}
	return nil
}

func isNotFound(err error) bool {
	var nf *types.NotFound
	if errors.As(err, &nf) {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NotFound", "NoSuchKey", "NoSuchBucket":
			return true
		}
	}
	return false
}

// parseS3Path splits "s3://bucket/key" into bucket and key.
func parseS3Path(path string) (string, string, error) {
	if !strings.HasPrefix(path, "s3://") {
		return "", "", fmt.Errorf("invalid S3 path %q: must start with s3://", path)
	}
	parts := strings.SplitN(strings.TrimPrefix(path, "s3://"), "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("invalid S3 path %q: expected s3://<bucket>/<key>", path)
	}
	return parts[0], parts[1], nil
}
