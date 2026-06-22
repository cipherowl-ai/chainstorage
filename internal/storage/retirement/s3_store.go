package retirement

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	awss3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/s3"
)

type S3ObjectStore struct {
	client s3.Client
}

func NewS3ObjectStore(client s3.Client) *S3ObjectStore {
	return &S3ObjectStore{client: client}
}

func (s *S3ObjectStore) HeadObject(ctx context.Context, bucket string, key string) (ObjectHead, error) {
	out, err := s.client.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isObjectNotFound(err) {
			return ObjectHead{}, nil
		}
		return ObjectHead{}, xerrors.Errorf("failed to head object (bucket=%s key=%s): %w", bucket, key, err)
	}
	head := ObjectHead{
		Exists:     true,
		VersionID:  aws.ToString(out.VersionId),
		Metadata:   out.Metadata,
		DeleteMark: aws.ToBool(out.DeleteMarker),
	}
	if out.ContentLength != nil && *out.ContentLength > 0 {
		head.Bytes = uint64(*out.ContentLength)
	}
	return head, nil
}

func (s *S3ObjectStore) CurrentObjectVersion(ctx context.Context, bucket string, key string) (ObjectVersion, error) {
	out, err := s.client.ListObjectVersions(ctx, &awss3.ListObjectVersionsInput{
		Bucket:  aws.String(bucket),
		Prefix:  aws.String(key),
		MaxKeys: aws.Int32(10),
	})
	if err != nil {
		return ObjectVersion{}, xerrors.Errorf("failed to list object versions (bucket=%s key=%s): %w", bucket, key, err)
	}

	for _, marker := range out.DeleteMarkers {
		if aws.ToString(marker.Key) == key && aws.ToBool(marker.IsLatest) {
			return ObjectVersion{
				Exists:              true,
				CurrentDeleteMarker: true,
				VersionID:           aws.ToString(marker.VersionId),
				LastModified:        cloneTime(marker.LastModified),
			}, nil
		}
	}
	for _, version := range out.Versions {
		if aws.ToString(version.Key) == key && aws.ToBool(version.IsLatest) {
			result := ObjectVersion{
				Exists:       true,
				VersionID:    aws.ToString(version.VersionId),
				LastModified: cloneTime(version.LastModified),
			}
			if version.Size != nil && *version.Size > 0 {
				result.Bytes = uint64(*version.Size)
			}
			return result, nil
		}
	}

	head, err := s.HeadObject(ctx, bucket, key)
	if err != nil {
		return ObjectVersion{}, err
	}
	if !head.Exists {
		return ObjectVersion{}, nil
	}
	return ObjectVersion{
		Exists:              true,
		CurrentDeleteMarker: head.DeleteMark,
		VersionID:           head.VersionID,
		Bytes:               head.Bytes,
	}, nil
}

func (s *S3ObjectStore) DeleteObjectVersion(ctx context.Context, bucket string, key string, versionID string) error {
	if versionID == "" {
		return xerrors.New("version id is required for retirement delete")
	}
	_, err := s.client.DeleteObject(ctx, &awss3.DeleteObjectInput{
		Bucket:    aws.String(bucket),
		Key:       aws.String(key),
		VersionId: aws.String(versionID),
	})
	if err != nil {
		return xerrors.Errorf("failed to delete object version (bucket=%s key=%s version_id=%s): %w", bucket, key, versionID, err)
	}
	return nil
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func isObjectNotFound(err error) bool {
	var notFound *awss3types.NotFound
	if errors.As(err, &notFound) {
		return true
	}
	var noSuchKey *awss3types.NoSuchKey
	if errors.As(err, &noSuchKey) {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NotFound", "NoSuchKey", "404":
			return true
		}
	}
	return false
}
