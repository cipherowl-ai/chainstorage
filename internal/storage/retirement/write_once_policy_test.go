package retirement

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	awss3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	s3mocks "github.com/coinbase/chainstorage/internal/s3/mocks"
)

const (
	testWriteOnceBucket = "co-chainstorage-solana-mainnet-prod"
	testWriteOnceKey    = "BLOCKCHAIN_SOLANA/NETWORK_SOLANA_MAINNET/consolidated/v=7/object.cscb.zstd"
)

func TestS3ObjectStore_InspectObjectRetentionSafety(t *testing.T) {
	controller := gomock.NewController(t)
	client := s3mocks.NewMockClient(controller)
	store := NewS3ObjectStore(client)
	policy := validWriteOncePolicy(`{"AWS":"*"}`)
	client.EXPECT().GetBucketPolicy(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, input *awss3.GetBucketPolicyInput, options ...func(*awss3.Options)) (*awss3.GetBucketPolicyOutput, error) {
			require.Equal(t, testWriteOnceBucket, aws.ToString(input.Bucket))
			return &awss3.GetBucketPolicyOutput{Policy: aws.String(url.QueryEscape(policy))}, nil
		},
	)
	client.EXPECT().GetBucketLifecycleConfiguration(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, input *awss3.GetBucketLifecycleConfigurationInput, options ...func(*awss3.Options)) (*awss3.GetBucketLifecycleConfigurationOutput, error) {
			require.Equal(t, testWriteOnceBucket, aws.ToString(input.Bucket))
			return nil, &smithy.GenericAPIError{Code: "NoSuchLifecycleConfiguration"}
		},
	)
	snapshot, err := store.InspectObjectRetentionSafety(context.Background(), testWriteOnceBucket, testWriteOnceKey)
	require.NoError(t, err)
	require.True(t, isSHA256Hex(snapshot.ConfigurationSHA256))
}

func TestS3ObjectStore_InspectObjectRetentionSafetyRequiresAllControlPlaneProtection(t *testing.T) {
	tests := []struct {
		name       string
		statements []string
	}{
		{
			name:       "missing delete protection",
			statements: []string{validConditionalWriteStatement(`"*"`), validLifecycleConfigurationProtectionStatement(`"*"`)},
		},
		{
			name:       "missing conditional write protection",
			statements: []string{validMutationProtectionStatement(`"*"`), validLifecycleConfigurationProtectionStatement(`"*"`)},
		},
		{
			name:       "missing lifecycle configuration protection",
			statements: []string{validConditionalWriteStatement(`"*"`), validMutationProtectionStatement(`"*"`)},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			controller := gomock.NewController(t)
			client := s3mocks.NewMockClient(controller)
			store := NewS3ObjectStore(client)
			policy := fmt.Sprintf(`{"Version":"2012-10-17","Statement":[%s]}`, strings.Join(test.statements, ","))
			client.EXPECT().GetBucketPolicy(gomock.Any(), gomock.Any()).Return(
				&awss3.GetBucketPolicyOutput{Policy: aws.String(policy)},
				nil,
			)
			client.EXPECT().GetBucketLifecycleConfiguration(gomock.Any(), gomock.Any()).Return(
				nil,
				&smithy.GenericAPIError{Code: "NoSuchLifecycleConfiguration"},
			)
			_, err := store.InspectObjectRetentionSafety(context.Background(), testWriteOnceBucket, testWriteOnceKey)
			require.Error(t, err)
		})
	}
}

func TestS3ObjectStore_InspectObjectRetentionSafetyRejectsMatchingLifecycleActions(t *testing.T) {
	const (
		rejectedRule          = "can expire or transition CSCB object data"
		missingTieringDeny    = "does not deny s3:PutIntelligentTieringConfiguration"
		tieringConfigured     = "has an Intelligent-Tiering configuration"
		tieringListFailed     = "failed to list CSCB bucket Intelligent-Tiering configurations"
		tieringListEmptyReply = "Intelligent-Tiering configurations returned an empty response"
		tieringListTruncated  = "Intelligent-Tiering configurations listing is truncated"
	)
	consolidatedPrefix := &awss3types.LifecycleRuleFilter{Prefix: aws.String("BLOCKCHAIN_SOLANA/NETWORK_SOLANA_MAINNET/consolidated/")}
	toIntelligentTiering := []awss3types.Transition{{Days: aws.Int32(0), StorageClass: awss3types.TransitionStorageClassIntelligentTiering}}
	tieringRule := awss3types.LifecycleRule{
		ID: aws.String("intelligent-tiering"), Status: awss3types.ExpirationStatusEnabled,
		Filter: consolidatedPrefix, Transitions: toIntelligentTiering,
	}
	noTieringConfigurations := &awss3.ListBucketIntelligentTieringConfigurationsOutput{IsTruncated: aws.Bool(false)}
	tests := []struct {
		name  string
		rules []awss3types.LifecycleRule
		// tieringDenied adds the bucket policy statement that denies
		// s3:PutIntelligentTieringConfiguration to everyone.
		tieringDenied bool
		// listsTiering is set when the check must read the bucket's
		// Intelligent-Tiering configurations; tieringList and tieringListErr
		// are what that read returns.
		listsTiering   bool
		tieringList    *awss3.ListBucketIntelligentTieringConfigurationsOutput
		tieringListErr error
		// wantErr is empty when the configuration is retention-safe.
		wantErr string
	}{
		{
			name:          "transition to intelligent tiering, archive configuration denied and absent",
			rules:         []awss3types.LifecycleRule{tieringRule},
			tieringDenied: true, listsTiering: true, tieringList: noTieringConfigurations,
		},
		{
			name: "current and noncurrent transitions to intelligent tiering",
			rules: []awss3types.LifecycleRule{{
				ID: aws.String("intelligent-tiering-all-versions"), Status: awss3types.ExpirationStatusEnabled,
				Filter: consolidatedPrefix, Transitions: toIntelligentTiering,
				NoncurrentVersionTransitions:   []awss3types.NoncurrentVersionTransition{{NoncurrentDays: aws.Int32(1), StorageClass: awss3types.TransitionStorageClassIntelligentTiering}},
				AbortIncompleteMultipartUpload: &awss3types.AbortIncompleteMultipartUpload{DaysAfterInitiation: aws.Int32(7)},
			}},
			tieringDenied: true, listsTiering: true, tieringList: noTieringConfigurations,
		},
		{
			name:    "transition to intelligent tiering without the archive configuration denied",
			rules:   []awss3types.LifecycleRule{tieringRule},
			wantErr: missingTieringDeny,
		},
		{
			name:          "transition to intelligent tiering with an archive configuration on the bucket",
			rules:         []awss3types.LifecycleRule{tieringRule},
			tieringDenied: true, listsTiering: true,
			tieringList: &awss3.ListBucketIntelligentTieringConfigurationsOutput{
				IntelligentTieringConfigurationList: []awss3types.IntelligentTieringConfiguration{{
					Id: aws.String("archive"), Status: awss3types.IntelligentTieringStatusEnabled,
					Tierings: []awss3types.Tiering{{AccessTier: awss3types.IntelligentTieringAccessTierDeepArchiveAccess, Days: aws.Int32(180)}},
				}},
			},
			wantErr: tieringConfigured,
		},
		{
			name:          "transition to intelligent tiering with a disabled archive configuration on the bucket",
			rules:         []awss3types.LifecycleRule{tieringRule},
			tieringDenied: true, listsTiering: true,
			tieringList: &awss3.ListBucketIntelligentTieringConfigurationsOutput{
				IntelligentTieringConfigurationList: []awss3types.IntelligentTieringConfiguration{{
					Id: aws.String("archive"), Status: awss3types.IntelligentTieringStatusDisabled,
				}},
			},
			wantErr: tieringConfigured,
		},
		{
			name:          "transition to intelligent tiering with a truncated configuration listing",
			rules:         []awss3types.LifecycleRule{tieringRule},
			tieringDenied: true, listsTiering: true,
			tieringList: &awss3.ListBucketIntelligentTieringConfigurationsOutput{IsTruncated: aws.Bool(true)},
			wantErr:     tieringListTruncated,
		},
		{
			name:          "transition to intelligent tiering when the configurations cannot be read",
			rules:         []awss3types.LifecycleRule{tieringRule},
			tieringDenied: true, listsTiering: true,
			tieringListErr: &smithy.GenericAPIError{Code: "AccessDenied"},
			wantErr:        tieringListFailed,
		},
		{
			name:          "transition to intelligent tiering when the configurations read returns nothing",
			rules:         []awss3types.LifecycleRule{tieringRule},
			tieringDenied: true, listsTiering: true,
			wantErr: tieringListEmptyReply,
		},
		{
			name: "transition to intelligent tiering that also expires",
			rules: []awss3types.LifecycleRule{{
				ID: aws.String("tier-then-expire"), Status: awss3types.ExpirationStatusEnabled,
				Filter: consolidatedPrefix, Transitions: toIntelligentTiering,
				Expiration: &awss3types.LifecycleExpiration{Days: aws.Int32(365)},
			}},
			tieringDenied: true,
			wantErr:       rejectedRule,
		},
		{
			name: "transition to intelligent tiering that also removes expired delete markers",
			rules: []awss3types.LifecycleRule{{
				ID: aws.String("tier-and-clean-markers"), Status: awss3types.ExpirationStatusEnabled,
				Filter: consolidatedPrefix, Transitions: toIntelligentTiering,
				Expiration: &awss3types.LifecycleExpiration{ExpiredObjectDeleteMarker: aws.Bool(true)},
			}},
			tieringDenied: true,
			wantErr:       rejectedRule,
		},
		{
			name: "transition to intelligent tiering that also expires noncurrent versions",
			rules: []awss3types.LifecycleRule{{
				ID: aws.String("tier-then-expire-noncurrent"), Status: awss3types.ExpirationStatusEnabled,
				Filter: consolidatedPrefix, Transitions: toIntelligentTiering,
				NoncurrentVersionExpiration: &awss3types.NoncurrentVersionExpiration{NoncurrentDays: aws.Int32(1)},
			}},
			tieringDenied: true,
			wantErr:       rejectedRule,
		},
		{
			name: "transition to intelligent tiering then to an archive class",
			rules: []awss3types.LifecycleRule{{
				ID: aws.String("tier-then-archive"), Status: awss3types.ExpirationStatusEnabled,
				Filter: consolidatedPrefix,
				Transitions: []awss3types.Transition{
					{Days: aws.Int32(0), StorageClass: awss3types.TransitionStorageClassIntelligentTiering},
					{Days: aws.Int32(180), StorageClass: awss3types.TransitionStorageClassDeepArchive},
				},
			}},
			tieringDenied: true,
			wantErr:       rejectedRule,
		},
		{
			name: "current intelligent tiering with a noncurrent archive transition",
			rules: []awss3types.LifecycleRule{{
				ID: aws.String("tier-current-archive-noncurrent"), Status: awss3types.ExpirationStatusEnabled,
				Filter: consolidatedPrefix, Transitions: toIntelligentTiering,
				NoncurrentVersionTransitions: []awss3types.NoncurrentVersionTransition{{NoncurrentDays: aws.Int32(1), StorageClass: awss3types.TransitionStorageClassGlacier}},
			}},
			tieringDenied: true,
			wantErr:       rejectedRule,
		},
		{
			name: "current archive transition with noncurrent intelligent tiering",
			rules: []awss3types.LifecycleRule{{
				ID: aws.String("archive-current-tier-noncurrent"), Status: awss3types.ExpirationStatusEnabled,
				Filter:                       consolidatedPrefix,
				Transitions:                  []awss3types.Transition{{Days: aws.Int32(0), StorageClass: awss3types.TransitionStorageClassGlacier}},
				NoncurrentVersionTransitions: []awss3types.NoncurrentVersionTransition{{NoncurrentDays: aws.Int32(1), StorageClass: awss3types.TransitionStorageClassIntelligentTiering}},
			}},
			tieringDenied: true,
			wantErr:       rejectedRule,
		},
		{
			name: "noncurrent transition to deep archive",
			rules: []awss3types.LifecycleRule{{
				ID: aws.String("archive-noncurrent"), Status: awss3types.ExpirationStatusEnabled,
				Filter:                       consolidatedPrefix,
				NoncurrentVersionTransitions: []awss3types.NoncurrentVersionTransition{{NoncurrentDays: aws.Int32(1), StorageClass: awss3types.TransitionStorageClassDeepArchive}},
			}},
			tieringDenied: true,
			wantErr:       rejectedRule,
		},
		{
			name: "intelligent tiering rule beside an archive rule",
			rules: []awss3types.LifecycleRule{
				tieringRule,
				{
					ID: aws.String("archive-consolidated"), Status: awss3types.ExpirationStatusEnabled,
					Filter:      consolidatedPrefix,
					Transitions: []awss3types.Transition{{Days: aws.Int32(30), StorageClass: awss3types.TransitionStorageClassGlacierIr}},
				},
			},
			tieringDenied: true,
			wantErr:       rejectedRule,
		},
		{
			name: "transition to glacier instant retrieval",
			rules: []awss3types.LifecycleRule{{
				ID: aws.String("glacier-ir"), Status: awss3types.ExpirationStatusEnabled,
				Filter:      consolidatedPrefix,
				Transitions: []awss3types.Transition{{Days: aws.Int32(0), StorageClass: awss3types.TransitionStorageClassGlacierIr}},
			}},
			tieringDenied: true,
			wantErr:       rejectedRule,
		},
		{
			name: "transition with no storage class",
			rules: []awss3types.LifecycleRule{{
				ID: aws.String("no-class"), Status: awss3types.ExpirationStatusEnabled,
				Filter: consolidatedPrefix, Transitions: []awss3types.Transition{{Days: aws.Int32(0)}},
			}},
			tieringDenied: true,
			wantErr:       rejectedRule,
		},
		{
			name: "disabled intelligent tiering rule needs no archive configuration deny",
			rules: []awss3types.LifecycleRule{{
				ID: aws.String("intelligent-tiering"), Status: awss3types.ExpirationStatusDisabled,
				Filter: consolidatedPrefix, Transitions: toIntelligentTiering,
			}},
		},
		{
			name: "intelligent tiering for an unrelated prefix needs no archive configuration deny",
			rules: []awss3types.LifecycleRule{{
				ID: aws.String("single-block-tiering"), Status: awss3types.ExpirationStatusEnabled,
				Filter: &awss3types.LifecycleRuleFilter{Prefix: aws.String("single-block/")}, Transitions: toIntelligentTiering,
			}},
		},
		{
			name: "abort incomplete multipart upload only",
			rules: []awss3types.LifecycleRule{{
				ID: aws.String("abort-multipart"), Status: awss3types.ExpirationStatusEnabled,
				AbortIncompleteMultipartUpload: &awss3types.AbortIncompleteMultipartUpload{DaysAfterInitiation: aws.Int32(7)},
			}},
		},
		{
			name: "disabled expiration",
			rules: []awss3types.LifecycleRule{{
				ID: aws.String("disabled"), Status: awss3types.ExpirationStatusDisabled,
				Expiration: &awss3types.LifecycleExpiration{Days: aws.Int32(1)},
			}},
		},
		{
			name: "expiration for unrelated prefix",
			rules: []awss3types.LifecycleRule{{
				ID: aws.String("single-block-only"), Status: awss3types.ExpirationStatusEnabled,
				Filter:     &awss3types.LifecycleRuleFilter{Prefix: aws.String("single-block/")},
				Expiration: &awss3types.LifecycleExpiration{Days: aws.Int32(1)},
			}},
		},
		{
			name: "current expiration",
			rules: []awss3types.LifecycleRule{{
				ID: aws.String("expire-all"), Status: awss3types.ExpirationStatusEnabled,
				Expiration: &awss3types.LifecycleExpiration{Days: aws.Int32(1)},
			}},
			wantErr: rejectedRule,
		},
		{
			name: "current transition",
			rules: []awss3types.LifecycleRule{{
				ID: aws.String("archive-consolidated"), Status: awss3types.ExpirationStatusEnabled,
				Filter:      &awss3types.LifecycleRuleFilter{And: &awss3types.LifecycleRuleAndOperator{Prefix: aws.String("BLOCKCHAIN_SOLANA/")}},
				Transitions: []awss3types.Transition{{Days: aws.Int32(1), StorageClass: awss3types.TransitionStorageClassGlacier}},
			}},
			wantErr: rejectedRule,
		},
		{
			name: "noncurrent expiration",
			rules: []awss3types.LifecycleRule{{
				ID: aws.String("expire-noncurrent"), Status: awss3types.ExpirationStatusEnabled,
				NoncurrentVersionExpiration: &awss3types.NoncurrentVersionExpiration{NoncurrentDays: aws.Int32(1)},
			}},
			wantErr: rejectedRule,
		},
		{
			name: "noncurrent transition with tag-only filter is conservatively unsafe",
			rules: []awss3types.LifecycleRule{{
				ID: aws.String("tag-filtered"), Status: awss3types.ExpirationStatusEnabled,
				Filter:                       &awss3types.LifecycleRuleFilter{Tag: &awss3types.Tag{Key: aws.String("archive"), Value: aws.String("true")}},
				NoncurrentVersionTransitions: []awss3types.NoncurrentVersionTransition{{NoncurrentDays: aws.Int32(1), StorageClass: awss3types.TransitionStorageClassDeepArchive}},
			}},
			wantErr: rejectedRule,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			controller := gomock.NewController(t)
			client := s3mocks.NewMockClient(controller)
			store := NewS3ObjectStore(client)
			statements := []string{
				validConditionalWriteStatement(`"*"`),
				validMutationProtectionStatement(`"*"`),
				validLifecycleConfigurationProtectionStatement(`"*"`),
			}
			if test.tieringDenied {
				statements = append(statements, validIntelligentTieringConfigurationProtectionStatement(`"*"`))
			}
			policy := fmt.Sprintf(`{"Version":"2012-10-17","Statement":[%s]}`, strings.Join(statements, ","))
			client.EXPECT().GetBucketPolicy(gomock.Any(), gomock.Any()).Return(
				&awss3.GetBucketPolicyOutput{Policy: aws.String(policy)}, nil,
			)
			client.EXPECT().GetBucketLifecycleConfiguration(gomock.Any(), gomock.Any()).Return(
				&awss3.GetBucketLifecycleConfigurationOutput{Rules: test.rules}, nil,
			)
			// The mock fails the test on a read that is not expected here, so
			// the cases without listsTiering also prove the read is skipped.
			if test.listsTiering {
				client.EXPECT().ListBucketIntelligentTieringConfigurations(gomock.Any(), gomock.Any()).DoAndReturn(
					func(ctx context.Context, input *awss3.ListBucketIntelligentTieringConfigurationsInput, options ...func(*awss3.Options)) (*awss3.ListBucketIntelligentTieringConfigurationsOutput, error) {
						require.Equal(t, testWriteOnceBucket, aws.ToString(input.Bucket))
						return test.tieringList, test.tieringListErr
					},
				)
			}

			_, err := store.InspectObjectRetentionSafety(context.Background(), testWriteOnceBucket, testWriteOnceKey)
			if test.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, test.wantErr)
			}
		})
	}
}

func TestRetentionSafetySnapshotTracksControlPlaneConfiguration(t *testing.T) {
	policy, err := decodeBucketPolicy(validWriteOncePolicy(`"*"`))
	require.NoError(t, err)
	firstRules := []awss3types.LifecycleRule{{
		ID:     aws.String("abort-multipart"),
		Status: awss3types.ExpirationStatusEnabled,
		AbortIncompleteMultipartUpload: &awss3types.AbortIncompleteMultipartUpload{
			DaysAfterInitiation: aws.Int32(7),
		},
	}}

	first, err := retentionSafetySnapshot(policy, firstRules)
	require.NoError(t, err)
	repeated, err := retentionSafetySnapshot(policy, firstRules)
	require.NoError(t, err)
	require.Equal(t, first.ConfigurationSHA256, repeated.ConfigurationSHA256)

	changedRules := append([]awss3types.LifecycleRule(nil), firstRules...)
	changedRules[0].AbortIncompleteMultipartUpload = &awss3types.AbortIncompleteMultipartUpload{
		DaysAfterInitiation: aws.Int32(14),
	}
	changed, err := retentionSafetySnapshot(policy, changedRules)
	require.NoError(t, err)
	require.NotEqual(t, first.ConfigurationSHA256, changed.ConfigurationSHA256)
}

func TestStatementEnforcesCSCBWriteOnce(t *testing.T) {
	objectARN := "arn:aws:s3:::" + testWriteOnceBucket + "/" + testWriteOnceKey
	valid := validConditionalWriteStatement(`"*"`)
	tests := []struct {
		name   string
		policy string
		valid  bool
	}{
		{name: "valid all principal", policy: valid, valid: true},
		{name: "valid terraform principal", policy: validConditionalWriteStatement(`{"AWS":["*"]}`), valid: true},
		{name: "valid multipart exemption", policy: replacePolicy(valid, `"Null":{"s3:if-none-match":"true"}`, `"Null":{"s3:if-none-match":"true"},"Bool":{"s3:ObjectCreationOperation":"true"}`), valid: true},
		{name: "allow is insufficient", policy: replacePolicy(valid, `"Effect":"Deny"`, `"Effect":"Allow"`)},
		{name: "restricted principal", policy: replacePolicy(valid, `"Principal":"*"`, `"Principal":{"AWS":"arn:aws:iam::992382740726:role/writer"}`)},
		{name: "wrong action", policy: replacePolicy(valid, `"Action":"s3:PutObject"`, `"Action":"s3:GetObject"`)},
		{name: "wrong resource", policy: replacePolicy(valid, `/*/consolidated/*`, `/single-block/*`)},
		{name: "missing condition", policy: replacePolicy(valid, `,"Condition":{"Null":{"s3:if-none-match":"true"}}`, ``)},
		{name: "condition false", policy: replacePolicy(valid, `"s3:if-none-match":"true"`, `"s3:if-none-match":"false"`)},
		{name: "narrowing extra condition", policy: replacePolicy(valid, `"Null":{"s3:if-none-match":"true"}`, `"Null":{"s3:if-none-match":"true"},"Bool":{"aws:SecureTransport":"true"}`)},
		{name: "not principal", policy: replacePolicy(valid, `"Principal":"*"`, `"Principal":"*","NotPrincipal":{"AWS":"arn:aws:iam::992382740726:role/writer"}`)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var statement bucketPolicyStatement
			err := json.Unmarshal([]byte(test.policy), &statement)
			require.NoError(t, err)
			require.Equal(t, test.valid, statementEnforcesCSCBWriteOnce(statement, objectARN))
		})
	}
}

func TestStatementEnforcesCSCBMutationProtection(t *testing.T) {
	objectARN := "arn:aws:s3:::" + testWriteOnceBucket + "/" + testWriteOnceKey
	valid := validMutationProtectionStatement(`"*"`)
	tests := []struct {
		name   string
		policy string
		valid  bool
	}{
		{name: "valid all principal", policy: valid, valid: true},
		{name: "valid terraform principal", policy: validMutationProtectionStatement(`{"AWS":["*"]}`), valid: true},
		{name: "allow is insufficient", policy: replacePolicy(valid, `"Effect":"Deny"`, `"Effect":"Allow"`)},
		{name: "restricted principal", policy: replacePolicy(valid, `"Principal":"*"`, `"Principal":{"AWS":"arn:aws:iam::992382740726:role/writer"}`)},
		{name: "missing delete object", policy: replacePolicy(valid, `"s3:DeleteObject",`, ``)},
		{name: "missing delete version", policy: replacePolicy(valid, `,"s3:DeleteObjectVersion"`, ``)},
		{name: "missing replicate object", policy: replacePolicy(valid, `,"s3:ReplicateObject"`, ``)},
		{name: "missing replicate delete", policy: replacePolicy(valid, `,"s3:ReplicateDelete"`, ``)},
		{name: "wrong resource", policy: replacePolicy(valid, `/*/consolidated/*`, `/single-block/*`)},
		{name: "narrowing condition", policy: replacePolicy(valid, `}`, `,"Condition":{"Bool":{"aws:SecureTransport":"true"}}}`)},
		{name: "not action", policy: replacePolicy(valid, `"Action"`, `"NotAction"`)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var statement bucketPolicyStatement
			err := json.Unmarshal([]byte(test.policy), &statement)
			require.NoError(t, err)
			require.Equal(t, test.valid, statementEnforcesCSCBMutationProtection(statement, objectARN))
		})
	}
}

func TestStatementEnforcesLifecycleConfigurationProtection(t *testing.T) {
	bucketARN := "arn:aws:s3:::" + testWriteOnceBucket
	valid := validLifecycleConfigurationProtectionStatement(`"*"`)
	tests := []struct {
		name   string
		policy string
		valid  bool
	}{
		{name: "valid all principal", policy: valid, valid: true},
		{name: "valid terraform principal", policy: validLifecycleConfigurationProtectionStatement(`{"AWS":["*"]}`), valid: true},
		{name: "allow is insufficient", policy: replacePolicy(valid, `"Effect":"Deny"`, `"Effect":"Allow"`)},
		{name: "restricted principal", policy: replacePolicy(valid, `"Principal":"*"`, `"Principal":{"AWS":"arn:aws:iam::992382740726:role/admin"}`)},
		{name: "wrong action", policy: replacePolicy(valid, `s3:PutLifecycleConfiguration`, `s3:GetLifecycleConfiguration`)},
		{name: "object resource is insufficient", policy: replacePolicy(valid, testWriteOnceBucket+`"`, testWriteOnceBucket+`/*"`)},
		{name: "narrowing condition", policy: replacePolicy(valid, `}`, `,"Condition":{"Bool":{"aws:SecureTransport":"true"}}}`)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var statement bucketPolicyStatement
			err := json.Unmarshal([]byte(test.policy), &statement)
			require.NoError(t, err)
			require.Equal(t, test.valid, statementEnforcesLifecycleConfigurationProtection(statement, bucketARN))
		})
	}
}

func TestStatementEnforcesIntelligentTieringConfigurationProtection(t *testing.T) {
	bucketARN := "arn:aws:s3:::" + testWriteOnceBucket
	valid := validIntelligentTieringConfigurationProtectionStatement(`"*"`)
	tests := []struct {
		name   string
		policy string
		valid  bool
	}{
		{name: "valid all principal", policy: valid, valid: true},
		{name: "valid terraform principal", policy: validIntelligentTieringConfigurationProtectionStatement(`{"AWS":["*"]}`), valid: true},
		{name: "allow is insufficient", policy: replacePolicy(valid, `"Effect":"Deny"`, `"Effect":"Allow"`)},
		{name: "restricted principal", policy: replacePolicy(valid, `"Principal":"*"`, `"Principal":{"AWS":"arn:aws:iam::992382740726:role/admin"}`)},
		{name: "wrong action", policy: replacePolicy(valid, `s3:PutIntelligentTieringConfiguration`, `s3:GetIntelligentTieringConfiguration`)},
		{name: "lifecycle deny is not enough", policy: validLifecycleConfigurationProtectionStatement(`"*"`)},
		{name: "object resource is insufficient", policy: replacePolicy(valid, testWriteOnceBucket+`"`, testWriteOnceBucket+`/*"`)},
		{name: "narrowing condition", policy: replacePolicy(valid, `}`, `,"Condition":{"Bool":{"aws:SecureTransport":"true"}}}`)},
		{name: "not action", policy: replacePolicy(valid, `"Action"`, `"NotAction"`)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var statement bucketPolicyStatement
			err := json.Unmarshal([]byte(test.policy), &statement)
			require.NoError(t, err)
			require.Equal(t, test.valid, statementEnforcesIntelligentTieringConfigurationProtection(statement, bucketARN))
		})
	}
}

func validWriteOncePolicy(principal string) string {
	return fmt.Sprintf(
		`{"Version":"2012-10-17","Statement":[%s,%s,%s]}`,
		validConditionalWriteStatement(principal),
		validMutationProtectionStatement(principal),
		validLifecycleConfigurationProtectionStatement(principal),
	)
}

func validConditionalWriteStatement(principal string) string {
	return fmt.Sprintf(`{"Effect":"Deny","Principal":%s,"Action":"s3:PutObject","Resource":"arn:aws:s3:::%s/*/consolidated/*","Condition":{"Null":{"s3:if-none-match":"true"}}}`, principal, testWriteOnceBucket)
}

func validMutationProtectionStatement(principal string) string {
	return fmt.Sprintf(`{"Effect":"Deny","Principal":%s,"Action":["s3:DeleteObject","s3:DeleteObjectVersion","s3:ReplicateObject","s3:ReplicateDelete"],"Resource":"arn:aws:s3:::%s/*/consolidated/*"}`, principal, testWriteOnceBucket)
}

func validLifecycleConfigurationProtectionStatement(principal string) string {
	return fmt.Sprintf(`{"Effect":"Deny","Principal":%s,"Action":"s3:PutLifecycleConfiguration","Resource":"arn:aws:s3:::%s"}`, principal, testWriteOnceBucket)
}

func validIntelligentTieringConfigurationProtectionStatement(principal string) string {
	return fmt.Sprintf(`{"Effect":"Deny","Principal":%s,"Action":"s3:PutIntelligentTieringConfiguration","Resource":"arn:aws:s3:::%s"}`, principal, testWriteOnceBucket)
}

func replacePolicy(policy string, old string, replacement string) string {
	return strings.Replace(policy, old, replacement, 1)
}
