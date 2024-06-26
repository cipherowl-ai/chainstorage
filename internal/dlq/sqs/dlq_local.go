package sqs

import (
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/sqs"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/utils/pointer"
)

func (q *dlqImpl) resetLocalResources() error {
	q.logger.Info("initializing dlq")

	if err := q.initQueueURL(); err != nil {
		var aerr awserr.Error
		if !xerrors.As(err, &aerr) || aerr.Code() != sqs.ErrCodeQueueDoesNotExist {
			return xerrors.Errorf("failed to init queue url: %w", err)
		}
	}

	if q.config.AWS.IsResetLocal && q.queueURL != "" {
		if _, err := q.client.DeleteQueue(&sqs.DeleteQueueInput{
			QueueUrl: pointer.Ref(q.queueURL),
		}); err != nil {
			return xerrors.Errorf("failed to delete queue: %w", err)
		}

		q.logger.Info("deleted sqs queue")
		q.queueURL = ""
	}

	if q.queueURL == "" {
		output, err := q.client.CreateQueue(&sqs.CreateQueueInput{
			QueueName: pointer.Ref(q.config.AWS.DLQ.Name),
		})
		if err != nil {
			return xerrors.Errorf("failed to create queue: %w", err)
		}

		q.logger.Info("created sqs queue", zap.Reflect("output", output))
	}

	return nil
}
