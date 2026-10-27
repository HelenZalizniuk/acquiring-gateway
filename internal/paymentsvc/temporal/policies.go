package paymenttemporal

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	nonRetryableErrorType = "InvalidArgument"

	defaultBackoffCoefficient = 2.0
	transientInitialInterval  = 500 * time.Millisecond
	transientMaxInterval      = 10 * time.Second
	transientMaxAttempts      = 5

	chargeStartToClose    = 20 * time.Second
	chargeScheduleToClose = 2 * time.Minute

	notifyStartToClose     = 10 * time.Second
	notifyScheduleToClose  = 30 * time.Second
	notifyInitialInterval  = 300 * time.Millisecond
	notifyMaxInterval      = 3 * time.Second
	notifyMaxAttempts      = 3

	completeStartToClose    = 15 * time.Second
	completeScheduleToClose = 90 * time.Second

	compensateStartToClose    = 20 * time.Second
	compensateScheduleToClose = 3 * time.Minute
	compensateInitialInterval = 500 * time.Millisecond
	compensateMaxInterval     = 15 * time.Second
	compensateMaxAttempts     = 8

	defaultWorkflowExecutionTimeout = 5 * time.Minute
	defaultWorkflowRunTimeout       = 5 * time.Minute
	defaultWorkflowRetryInterval    = time.Second
	defaultWorkflowRetryMaxInterval = 30 * time.Second
	defaultWorkflowRetryAttempts    = 3
)

// ActivityPolicies holds timeouts and retry/backoff for saga steps.
// Policies absorb transient database and network faults; InvalidArgument is not retried.
type ActivityPolicies struct {
	Charge     workflow.ActivityOptions
	Notify     workflow.ActivityOptions
	Complete   workflow.ActivityOptions
	Compensate workflow.ActivityOptions
}

func DefaultActivityPolicies() ActivityPolicies {
	transientRetry := &temporal.RetryPolicy{
		InitialInterval:        transientInitialInterval,
		BackoffCoefficient:     defaultBackoffCoefficient,
		MaximumInterval:        transientMaxInterval,
		MaximumAttempts:        transientMaxAttempts,
		NonRetryableErrorTypes: []string{nonRetryableErrorType},
	}

	return ActivityPolicies{
		Charge: workflow.ActivityOptions{
			StartToCloseTimeout:    chargeStartToClose,
			ScheduleToCloseTimeout: chargeScheduleToClose,
			RetryPolicy:            transientRetry,
		},
		Notify: workflow.ActivityOptions{
			StartToCloseTimeout:    notifyStartToClose,
			ScheduleToCloseTimeout: notifyScheduleToClose,
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval:        notifyInitialInterval,
				BackoffCoefficient:     defaultBackoffCoefficient,
				MaximumInterval:        notifyMaxInterval,
				MaximumAttempts:        notifyMaxAttempts,
				NonRetryableErrorTypes: []string{nonRetryableErrorType},
			},
		},
		Complete: workflow.ActivityOptions{
			StartToCloseTimeout:    completeStartToClose,
			ScheduleToCloseTimeout: completeScheduleToClose,
			RetryPolicy:            transientRetry,
		},
		Compensate: workflow.ActivityOptions{
			StartToCloseTimeout:    compensateStartToClose,
			ScheduleToCloseTimeout: compensateScheduleToClose,
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval:        compensateInitialInterval,
				BackoffCoefficient:     defaultBackoffCoefficient,
				MaximumInterval:        compensateMaxInterval,
				MaximumAttempts:        compensateMaxAttempts,
				NonRetryableErrorTypes: []string{nonRetryableErrorType},
			},
		},
	}
}
