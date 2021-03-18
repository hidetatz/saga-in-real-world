package orchestration

import "context"

type MessageQueue interface {
	SendMessage(ctx context.Context, message string) error
	RunReceiver(ctx context.Context)
}
