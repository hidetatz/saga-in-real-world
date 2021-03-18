package orchestration

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type MessageHandler interface {
	HandleMessage(ctx context.Context, msg *types.Message) error
}

type SNSSQS struct {
	sns         *sns.Client
	snsTopicARN string

	sqs            *sqs.Client
	sqsQueueName   string
	messageHandler MessageHandler
}

func NewSNSSQS(sns *sns.Client, sqs *sqs.Client, snsTopicARN, sqsQueueName string, messageHandler MessageHandler) *SNSSQS {
	return &SNSSQS{
		sns:            sns,
		snsTopicARN:    snsTopicARN,
		sqs:            sqs,
		sqsQueueName:   sqsQueueName,
		messageHandler: messageHandler,
	}
}

func (s *SNSSQS) SendMessage(ctx context.Context, message string) error {
	input := &sns.PublishInput{Message: &message, TopicArn: &s.snsTopicARN}
	if _, err := s.sns.Publish(ctx, input); err != nil {
		return fmt.Errorf("publish to AWS SNS: %w", err)
	}

	return nil
}

// RunReceiver runs a loop to receive a message from SQS queue.
// In the loop, it fetches a message then pass it to the injected messageHandler.
// The message is never deleted until the messageHandler succeeds.
// In case failures, the message is not deleted then retried automatically.
// Therefore, the handler must be idempotent.
func (s *SNSSQS) RunReceiver(ctx context.Context) {
	wg := &sync.WaitGroup{}
	for {
		select {
		case <-ctx.Done():
			wg.Wait() // make sure all the goroutines finish before return
			return
		default:
			wg.Add(1)
			go func() {
				queueURLResult, err := s.sqs.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: &s.sqsQueueName})
				if err != nil {
					// even if it comes here, the message is not explicitly deleted and will be delivered again
					log.Printf("failed to get SQS Queue URL: %s\n", err)
					return
				}

				output, err := s.sqs.ReceiveMessage(ctx,
					&sqs.ReceiveMessageInput{
						MessageAttributeNames: []string{string(types.QueueAttributeNameAll)},
						QueueUrl:              queueURLResult.QueueUrl,
						MaxNumberOfMessages:   1, // Receive only 1 message at once
					},
				)
				if err != nil {
					log.Printf("failed to get a message from SQS: %s\n", err)
					return
				}

				if len(output.Messages) == 0 {
					// because using SQS short polling, responded messages can be empty
					return
				}

				// output messges length must be 1 because we specified it, but use for loop just in case
				for _, msg := range output.Messages {
					// this implementation assumes handleMessage() will succeed eventually,
					// while there are temporary failures.
					// If it should not be assumed, you need to implement your own dead letter queue.
					if err := s.messageHandler.HandleMessage(ctx, &msg); err != nil {
						log.Printf("failed to process a message: %s\n", err)
						continue
					}

					if _, err := s.sqs.DeleteMessage(ctx, &sqs.DeleteMessageInput{
						QueueUrl:      queueURLResult.QueueUrl,
						ReceiptHandle: msg.ReceiptHandle,
					}); err != nil {
						log.Printf("failed to delete a message: %s\n", err)
					}
				}

				wg.Done()
			}()
		}
	}
}
