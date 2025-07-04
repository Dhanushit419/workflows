package workflows

import (
	"encoding/json"
	"fmt"
	"gitlab/rFlow/workflow-worker/activities"
	"gitlab/rFlow/workflow-worker/common"
	"gitlab/rFlow/workflow-worker/dependencies"
	"time"

	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/zap"
)

// Document-Name: autobindTest3
// Version: 1
// Type or Namespace: _autobindTest3
// Dsl: 1

type WorkflowInstance struct {
	dataMap          map[string]interface{}
	EventMap         map[string]Event
	currentTask      string
	previousTask     string
	taskHistory      map[string]interface{}
	failedActivities map[string]ActivityFailure
	activityWaitChan map[string]workflow.Channel
	activityAttempts map[string]int
}
type _autobindTest3 struct {
	configMap         map[string]interface{}
	workflowIOManager dependencies.WorkflowIOManager
	logger            log.Logger
	signalChan        workflow.ReceiveChannel
}

type ActivityFailure struct {
	ActivityName string    `json:"activity_name"`
	TaskName     string    `json:"task_name"`
	Error        string    `json:"error"`
	FailedAt     time.Time `json:"failed_at"`
	RetryCount   int       `json:"retry_count"`
}

func New_autobindTest3(workflowIOManager dependencies.WorkflowIOManager) *_autobindTest3 {
	return &_autobindTest3{
		configMap:         map[string]interface{}{},
		workflowIOManager: workflowIOManager,
		logger:            nil,
		signalChan:        nil,
	}
}

type Event struct {
	EventID   string                 `json:"event_id"`
	EventType string                 `json:"event_type"`
	Name      string                 `json:"name"`
	Result    map[string]interface{} `json:"result"`
}

func (w *_autobindTest3) Execute(ctx workflow.Context, input interface{}) (string, error) {
	instance := &WorkflowInstance{
		dataMap:          map[string]interface{}{},
		EventMap:         map[string]Event{},
		currentTask:      "",
		previousTask:     "",
		taskHistory:      map[string]interface{}{},
		failedActivities: make(map[string]ActivityFailure),
		activityWaitChan: make(map[string]workflow.Channel),
		activityAttempts: make(map[string]int),
	}

	w.logger = workflow.GetLogger(ctx)
	w.logger.Info("Executing RdexDemo workflow with input: %v", input)
	retrypolicy := &temporal.RetryPolicy{
		InitialInterval:    time.Second,
		BackoffCoefficient: 2.0,
		MaximumAttempts:    1,
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 120 * time.Second,
		HeartbeatTimeout:    120 * time.Second,
		RetryPolicy:         retrypolicy,
	}
	lao := workflow.LocalActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	ctx = workflow.WithLocalActivityOptions(ctx, lao)

	w.signalChan = workflow.GetSignalChannel(ctx, "retryActivity")
	// Add signal processing goroutine that only activates when activities fail
	workflow.Go(ctx, func(ctx workflow.Context) {
		// Register a query handler for failed activities
		if err := workflow.SetQueryHandler(ctx, "getFailedActivities", func() (map[string]ActivityFailure, error) {
			return instance.failedActivities, nil
		}); err != nil {
			w.logger.Error("failed to register failed activities query handler", zap.Error(err))
			return
		}

		// Only start processing signals when there's at least one failed activity
		for {
			// Check if there are any failed activities to retry
			if len(instance.failedActivities) == 0 {
				// No failed activities, sleep briefly and check again
				_ = workflow.Sleep(ctx, 30*time.Second)
				continue
			}

			// Now we have failed activities, start processing signals
			var taskName string
			w.signalChan.Receive(ctx, &taskName)

			w.logger.Info("Received retry signal for activity",
				zap.String("Task", taskName))

			// Verify it's a known failed activity
			if _, exists := instance.failedActivities[taskName]; !exists {
				w.logger.Warn("Received retry signal for unknown failed activity",
					zap.String("Task", taskName))
				continue
			}

			// Find the channel for this activity and signal it
			if ch, exists := instance.activityWaitChan[taskName]; exists {
				// Forward the signal to the activity's channel
				ch.Send(ctx, true)
				w.logger.Info("Forwarded retry signal to activity",
					zap.String("Task", taskName))
			} else {
				w.logger.Warn("No waiting channel found for activity",
					zap.String("Task", taskName))
			}
		}
	})

	// Register query handler for tracking task execution
	if err := workflow.SetQueryHandler(ctx, "getCurrentTask", func() (string, error) {
		if instance.currentTask == "" {
			return "No task currently executing", nil
		}
		return instance.currentTask, nil
	}); err != nil {
		w.logger.Error("failed to register query handler : _autobindTest3", err)
		return "", err
	}

	if err := workflow.SetQueryHandler(ctx, "getTaskHistory", func() (map[string]interface{}, error) {
		return instance.taskHistory, nil
	}); err != nil {
		w.logger.Error("failed to register task history query handler: %v", err)
		return "", err
	}

	inputMap, err := common.ConvertStructToMap(input)
	if err != nil {
		w.logger.Error("error converting input to map: %v", err)
		return "", err
	}

	instance.dataMap["workflow_id"] = workflow.GetInfo(ctx).WorkflowExecution.ID
	instance.dataMap["run_id"] = workflow.GetInfo(ctx).WorkflowExecution.RunID
	instance.dataMap["tenant_id"] = workflow.GetInfo(ctx).Namespace
	instance.dataMap["workflow_name"] = workflow.GetInfo(ctx).WorkflowType.Name

	instance.EventMap["Trigger"] = Event{
		EventID:   "Trigger",
		EventType: "Trigger",
		Name:      "Trigger",
		Result:    inputMap,
	}

	// Execute first task
	if err := w.File_Copy1(ctx, instance, instance.dataMap); err != nil {
		return "", err
	}

	dataId, err := w.PersistActualInput(ctx, instance.dataMap)
	if err != nil {
		w.logger.Error("Error persisting actual input: %v", err)
		return "", err
	}

	return dataId, nil
}

func (w *_autobindTest3) PersistActualInput(ctx workflow.Context, data interface{}) (string, error) {
	persister := dependencies.NewWorkflowDataPersister().
		WithTenantID(workflow.GetInfo(ctx).Namespace).
		WithData(data).
		WithPersister(w.workflowIOManager)

	var dataId string
	handle := workflow.ExecuteLocalActivity(ctx, persister.Persist)

	if err := handle.Get(ctx, &dataId); err != nil {
		w.logger.Error("Error persisting actual input: %v", err)
		return "", err
	}
	return dataId, nil
}

func (w *_autobindTest3) handleActivityRetryLoop(ctx workflow.Context, instance *WorkflowInstance, activityName, taskName string, execute func() (interface{}, error), initialError error) (interface{}, error) {
	// Log the initial failure
	w.logger.Error("Activity failed",
		zap.String("activity", activityName),
		zap.String("task", taskName),
		zap.Error(initialError))

	// Create a channel for waiting on retry signals if it doesn't exist
	retryChan, exists := instance.activityWaitChan[taskName]
	if !exists {
		retryChan = workflow.NewChannel(ctx)
		instance.activityWaitChan[taskName] = retryChan
	}

	// Record failure details - this will activate the signal processing in the Execute method
	instance.failedActivities[taskName] = ActivityFailure{
		ActivityName: activityName,
		TaskName:     taskName,
		Error:        initialError.Error(),
		FailedAt:     workflow.Now(ctx),
		RetryCount:   0,
	}

	// Log workflow status for monitoring
	w.logger.Info("Workflow paused - waiting for retry signal",
		zap.String("activity", activityName),
		zap.String("task", taskName))

	w.logger.Error("WORKFLOW_WAITING_FOR_RETRY",
		zap.String("workflow_id", workflow.GetInfo(ctx).WorkflowExecution.ID),
		zap.String("activity", activityName),
		zap.String("error", initialError.Error()))

	// Produce a workflow event about activity failure if needed
	errorPayload := map[string]interface{}{
		"event_type":    "activity_failed",
		"workflow_id":   workflow.GetInfo(ctx).WorkflowExecution.ID,
		"run_id":        workflow.GetInfo(ctx).WorkflowExecution.RunID,
		"activity_name": activityName,
		"task_name":     taskName,
		"error":         initialError.Error(),
		"timestamp":     workflow.Now(ctx).Format(time.RFC3339),
		"tenant_id":     workflow.GetInfo(ctx).Namespace,
	}
	jsonBytes, _ := json.Marshal(errorPayload)

	laCtx := workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
		StartToCloseTimeout: 5 * time.Second,
	})

	var success bool
	workflow.ExecuteLocalActivity(
		laCtx,
		activities.ProduceWorkflowHistoryActivity,
		workflow.GetInfo(ctx).WorkflowExecution.ID,
		jsonBytes,
	).Get(laCtx, &success)

	// Wait for retry signal loop
	for {
		// Create a selector to wait for the retry signal
		//log something
		w.logger.Info("Waiting for retry signal for activity",
			zap.String("activity", activityName))
		selector := workflow.NewSelector(ctx)

		var signalReceived bool
		selector.AddReceive(retryChan, func(c workflow.ReceiveChannel, more bool) {
			c.Receive(ctx, &signalReceived)
			w.logger.Info("Received retry signal for activity",
				zap.String("activity", activityName))
		})

		// Add a very long timeout as a safety mechanism
		longTimer := workflow.NewTimer(ctx, 365*24*time.Hour)
		selector.AddFuture(longTimer, func(f workflow.Future) {
			w.logger.Info("Long timeout reached, auto-retrying activity",
				zap.String("activity", activityName))
		})

		// Wait for either signal or timeout
		selector.Select(ctx)

		// Update retry count
		failure := instance.failedActivities[taskName]
		failure.RetryCount++
		instance.failedActivities[taskName] = failure

		w.logger.Info("Retrying failed activity",
			zap.String("activity", activityName),
			zap.String("task", taskName),
			zap.Int("attempt", failure.RetryCount))

		// Try the execution again using the provided function
		result, err := execute()

		if err == nil {
			// Success! Remove from failed activities
			delete(instance.failedActivities, taskName)
			delete(instance.activityWaitChan, taskName) // Clean up the channel too

			w.logger.Info("Activity retry successful",
				zap.String("Task", taskName))

			// Produce a success event
			successPayload := map[string]interface{}{
				"event_type":    "activity_retry_succeeded",
				"workflow_id":   workflow.GetInfo(ctx).WorkflowExecution.ID,
				"activity_name": activityName,
				"task_name":     taskName,
				"attempts":      failure.RetryCount,
				"timestamp":     workflow.Now(ctx).Format(time.RFC3339),
				"tenant_id":     workflow.GetInfo(ctx).Namespace,
			}
			jsonBytes, _ := json.Marshal(successPayload)

			workflow.ExecuteLocalActivity(
				laCtx,
				activities.ProduceWorkflowHistoryActivity,
				workflow.GetInfo(ctx).WorkflowExecution.ID,
				jsonBytes)
			if resultMap, ok := result.(map[string]interface{}); ok {
				event := instance.EventMap[taskName]
				event.Result = resultMap
				instance.EventMap[taskName] = event
			}
			return result, nil
		}

		// Still failed - update error message
		failure = instance.failedActivities[taskName]
		failure.Error = err.Error()
		instance.failedActivities[taskName] = failure

		w.logger.Error("Activity retry failed",
			zap.String("Task", taskName),
			zap.Error(err),
			zap.Int("attempt", failure.RetryCount))

	}
}

func (w *_autobindTest3) produceWorkflowTaskStart(ctx workflow.Context, taskName string, inputMap map[string]interface{}, instance *WorkflowInstance) error {
	// Get workflow execution info
	info := workflow.GetInfo(ctx)
	workflowID := info.WorkflowExecution.ID
	runID := info.WorkflowExecution.RunID

	// Create the event payload
	statusEventPayload := map[string]interface{}{
		"event_type":    "workflow_task_start",
		"workflow_id":   workflowID,
		"run_id":        runID,
		"current_task":  taskName,
		"previous_task": instance.previousTask,
		"tenant_id":     info.Namespace,
		"workflow_type": info.WorkflowType.Name,
		"timestamp":     time.Now(),
		"input":         inputMap,
	}

	// Convert to JSON bytes
	jsonBytes, err := json.Marshal(statusEventPayload)
	if err != nil {
		w.logger.Error("failed to marshal status event: %w", err)
		return err
	}

	// Use SideEffect to produce the event non-deterministically
	localActivityOptions := workflow.LocalActivityOptions{
		StartToCloseTimeout: 5 * time.Second,
	}
	// Create a context with the local activity options
	laCtx := workflow.WithLocalActivityOptions(ctx, localActivityOptions)

	// Execute the local activity
	var success bool
	err = workflow.ExecuteLocalActivity(
		laCtx,
		activities.ProduceWorkflowHistoryActivity,
		workflowID,
		jsonBytes,
	).Get(laCtx, &success)

	if err != nil {
		w.logger.Warn("Failed to produce kafka event for task status update",
			zap.String("task", taskName),
			zap.String("workflow_id", workflowID),
			zap.Error(err))
		// We don't return an error to prevent failing the workflow just for a status update
		return nil
	}

	if !success {
		w.logger.Warn("Kafka event production returned false",
			zap.String("task", taskName),
			zap.String("workflow_id", workflowID))
	}

	return nil
}

func (w *_autobindTest3) produceWorkflowTaskCompletion(ctx workflow.Context, taskName string, outputMap map[string]interface{}) error {
	// Get workflow execution info
	info := workflow.GetInfo(ctx)
	workflowID := info.WorkflowExecution.ID
	runID := info.WorkflowExecution.RunID

	w.logger.Info("Output map for task",
		zap.String("task", taskName),
		zap.Any("output", outputMap))

	completionEventPayload := map[string]interface{}{
		"event_type":    "workflow_task_complete",
		"workflow_id":   workflowID,
		"run_id":        runID,
		"current_task":  taskName,
		"tenant_id":     info.Namespace,
		"workflow_type": info.WorkflowType.Name,
		"completed_at":  time.Now(),
		"output":        outputMap,
	}

	// Convert to JSON bytes
	jsonBytes, err := json.Marshal(completionEventPayload)
	if err != nil {
		w.logger.Error("failed to marshal task completion event: %w", err)
		return err
	}

	// Use LocalActivity to produce the event
	localActivityOptions := workflow.LocalActivityOptions{
		StartToCloseTimeout: 5 * time.Second,
	}

	// Create a context with the local activity options
	laCtx := workflow.WithLocalActivityOptions(ctx, localActivityOptions)

	// Execute the local activity
	var success bool
	err = workflow.ExecuteLocalActivity(
		laCtx,
		activities.ProduceWorkflowHistoryActivity,
		workflowID,
		jsonBytes,
	).Get(laCtx, &success)

	if err != nil {
		w.logger.Warn("Failed to produce kafka event for task completion",
			zap.String("task", taskName),
			zap.String("workflow_id", workflowID),
			zap.Error(err))
		// We don't return an error to prevent failing the workflow just for a status update
		return nil
	}

	if !success {
		w.logger.Warn("Kafka event production for task completion returned false",
			zap.String("task", taskName),
			zap.String("workflow_id", workflowID))
	} else {
		w.logger.Info("Successfully sent task completion event",
			zap.String("task", taskName),
			zap.String("workflow_id", workflowID))
	}

	return nil
}

func (w *_autobindTest3) produceWorkflowCompletion(ctx workflow.Context, outputMap map[string]interface{}) error {
	info := workflow.GetInfo(ctx)
	workflowID := info.WorkflowExecution.ID
	runID := info.WorkflowExecution.RunID

	completionEventPayload := map[string]interface{}{
		"event_type":   "workflow_complete",
		"workflow_id":  workflowID,
		"run_id":       runID,
		"tenant_id":    info.Namespace,
		"completed_at": time.Now(),
	}

	// Convert to JSON bytes
	jsonBytes, err := json.Marshal(completionEventPayload)
	if err != nil {
		w.logger.Error("failed to marshal task completion event: %w", err)
		return err
	}

	// Use LocalActivity to produce the event
	localActivityOptions := workflow.LocalActivityOptions{
		StartToCloseTimeout: 5 * time.Second,
	}

	// Create a context with the local activity options
	laCtx := workflow.WithLocalActivityOptions(ctx, localActivityOptions)

	// Execute the local activity
	var success bool
	err = workflow.ExecuteLocalActivity(
		laCtx,
		activities.ProduceWorkflowHistoryActivity,
		workflowID,
		jsonBytes,
	).Get(laCtx, &success)

	if err != nil {
		w.logger.Warn("Failed to produce kafka event for workflow completion",
			zap.String("workflow_id", workflowID),
			zap.Error(err))
		// We don't return an error to prevent failing the workflow just for a status update
		return nil
	}

	if !success {
		w.logger.Warn("Kafka event production for workflow completion returned false",
			zap.String("workflow_id", workflowID))
	} else {
		w.logger.Info("Successfully sent workflow completion event",
			zap.String("workflow_id", workflowID))
	}

	return nil
}

func (w *_autobindTest3) safeExecuteActivity(ctx workflow.Context, instance *WorkflowInstance, activityName string, input interface{}) (interface{}, error) {
	var result interface{}
	currentTask := instance.currentTask

	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ActivityID:             currentTask,
		ScheduleToCloseTimeout: 120 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second * 1,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Second * 100,
			MaximumAttempts:    2,
		},
	})

	inputMap, err := common.ConvertStructToMap(input)
	if err != nil {
		w.logger.Error("error converting input to map: %v", err)
		return nil, err
	}
	w.logger.Info("Input map for activity", zap.Any("input", inputMap))

	// Execute activity
	activityError := workflow.ExecuteActivity(activityCtx, activityName, inputMap).Get(activityCtx, &result)

	// If no error, return the result
	if activityError == nil {
		return result, nil
	}

	// Activity failed - log the error
	w.logger.Error("Activity failed",
		zap.String("activity", activityName),
		zap.String("task", instance.currentTask),
		zap.Error(activityError))

	// Create a channel for waiting on retry signals if it doesn't exist
	retryChan, exists := instance.activityWaitChan[instance.currentTask]
	if !exists {
		retryChan = workflow.NewChannel(ctx)
		instance.activityWaitChan[instance.currentTask] = retryChan
	}

	// Record failure details - this will activate the signal processing in the Execute method
	instance.failedActivities[instance.currentTask] = ActivityFailure{
		ActivityName: activityName,
		TaskName:     instance.currentTask,
		Error:        activityError.Error(),
		FailedAt:     workflow.Now(ctx),
		RetryCount:   0,
	}

	// Log workflow status for monitoring
	w.logger.Info("Workflow paused - waiting for retry signal",
		zap.String("activity", activityName),
		zap.String("task", instance.currentTask))

	w.logger.Error("WORKFLOW_WAITING_FOR_RETRY",
		zap.String("workflow_id", workflow.GetInfo(ctx).WorkflowExecution.ID),
		zap.String("activity", activityName),
		zap.String("task", instance.currentTask),
		zap.String("error", activityError.Error()))

	// Produce a workflow event about activity failure if needed
	errorPayload := map[string]interface{}{
		"event_type":    "activity_failed",
		"workflow_id":   workflow.GetInfo(ctx).WorkflowExecution.ID,
		"run_id":        workflow.GetInfo(ctx).WorkflowExecution.RunID,
		"activity_name": activityName,
		"task_name":     instance.currentTask,
		"error":         activityError.Error(),
		"timestamp":     workflow.Now(ctx).Format(time.RFC3339),
	}
	jsonBytes, _ := json.Marshal(errorPayload)

	laCtx := workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
		StartToCloseTimeout: 5 * time.Second,
	})

	var success bool
	workflow.ExecuteLocalActivity(
		laCtx,
		activities.ProduceWorkflowHistoryActivity,
		workflow.GetInfo(ctx).WorkflowExecution.ID,
		jsonBytes,
	).Get(laCtx, &success)

	// Wait for retry signal loop
	for i := 1; i <= 10; i++ {
		// Create a selector to wait for the retry signal
		selector := workflow.NewSelector(ctx)

		var signalReceived bool
		selector.AddReceive(retryChan, func(c workflow.ReceiveChannel, more bool) {
			c.Receive(ctx, &signalReceived)
			w.logger.Info("Received retry signal for activity in safeExecuteActivity",
				zap.String("activity", activityName))
		})

		// Add a very long timeout as a safety mechanism
		longTimer := workflow.NewTimer(ctx, 365*24*time.Hour)
		selector.AddFuture(longTimer, func(f workflow.Future) {
			w.logger.Info("Long timeout reached, auto-retrying activity",
				zap.String("activity", activityName))
		})

		// Wait for either signal or timeout
		selector.Select(ctx)

		// Update retry count
		failure := instance.failedActivities[instance.currentTask]
		failure.RetryCount++
		instance.failedActivities[instance.currentTask] = failure

		w.logger.Info("Retrying failed activity in SafeExecuteActivity",
			zap.String("activity", activityName),
			zap.String("task", instance.currentTask),
			zap.Int("attempt", failure.RetryCount))

		// Try the activity again
		activityCtx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ActivityID:             fmt.Sprintf("%s_%d", currentTask, i),
			ScheduleToCloseTimeout: 120 * time.Second,
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval:    time.Second * 1,
				BackoffCoefficient: 2.0,
				MaximumInterval:    time.Second * 100,
				MaximumAttempts:    1,
			},
		})

		activityError = workflow.ExecuteActivity(activityCtx, activityName, inputMap).Get(ctx, &result)

		if activityError == nil {
			// Success! Remove from failed activities
			delete(instance.failedActivities, instance.currentTask)
			delete(instance.activityWaitChan, instance.currentTask) // Clean up the channel too

			w.logger.Info("Activity retry successful",
				zap.String("Task", instance.currentTask))

			// Produce a success event
			successPayload := map[string]interface{}{
				"event_type":    "activity_retry_succeeded",
				"workflow_id":   workflow.GetInfo(ctx).WorkflowExecution.ID,
				"activity_name": activityName,
				"task_name":     instance.currentTask,
				"attempts":      failure.RetryCount,
				"timestamp":     workflow.Now(ctx).Format(time.RFC3339),
			}
			jsonBytes, _ := json.Marshal(successPayload)

			workflow.ExecuteLocalActivity(
				laCtx,
				activities.ProduceWorkflowHistoryActivity,
				workflow.GetInfo(ctx).WorkflowExecution.ID,
				jsonBytes)

			if resultMap, ok := result.(map[string]interface{}); ok {
				event := instance.EventMap[instance.currentTask]
				event.Result = resultMap
				instance.EventMap[instance.currentTask] = event
			}
			return result, nil
		}

		// Still failed - update error message
		failure = instance.failedActivities[instance.currentTask]
		failure.Error = activityError.Error()
		instance.failedActivities[instance.currentTask] = failure

		w.logger.Error("Activity retry failed",
			zap.String("activity", activityName),
			zap.String("task", instance.currentTask),
			zap.Error(activityError),
			zap.Int("attempt", failure.RetryCount))

		// Continue loop to wait for another signal
	}

	return nil, fmt.Errorf("activity %s failed", instance.currentTask)
}

func (w *_autobindTest3) File_Copy1(ctx workflow.Context, instance *WorkflowInstance, input interface{}) error {
	instance.previousTask = instance.currentTask
	instance.currentTask = "File_Copy1"
	instance.dataMap["current_task"] = instance.currentTask
	w.logger.Info("Executing task: File_Copy1\n")

	dataMap, err := common.ConvertStructToMap(input)
	if err != nil {
		w.logger.Error("error converting input to map: %v", err)
		return err
	}
	w.logger.Info("dataMap: %s", dataMap)

	inputMap := make(map[string]interface{})
	inputMap["filecopy_in_file_names"] = instance.EventMap["Trigger"].Result["file_listener.out.file_names"]
	inputMap["filecopy_in_file_path"] = instance.EventMap["Trigger"].Result["file_listener.out.file_path"]
	inputMap["activity-id"] = "63c61207-238c-4511-aa60-ca26edb15f84"

	inputMap["workflow_id"] = workflow.GetInfo(ctx).WorkflowExecution.ID
	instance.dataMap["input"] = inputMap

	dataId, err := w.PersistActualInput(ctx, instance.dataMap)
	if err != nil {
		w.logger.Error("Error persisting actual input: %%s", err)
		return err
	}

	err = w.produceWorkflowTaskStart(ctx, instance.currentTask, inputMap, instance)
	if err != nil {
		w.logger.Error("Error producing task status event: %v", err)
	}

	_, err = w.safeExecuteActivity(ctx, instance, "FileCopyProxy", map[string]interface{}{"data_id": dataId})
	if err != nil {
		w.logger.Error("Workflow terminated during activity execution", zap.Error(err))
		return err
	}
	err = w.CustomUpdateHandler(ctx, instance, "FileCopyProxy")
	if err != nil {
		return err
	}
	err = w.waitEvent(ctx, instance, input, "File_Copy1", "file_copy.com")
	if err != nil {
		return err
	}

	outputMap := instance.EventMap[instance.currentTask].Result
	output, err := common.ConvertStructToMap(outputMap)
	if err != nil {
		w.logger.Error("error converting input to map: %v", err)
		return err
	}

	err = w.produceWorkflowTaskCompletion(ctx, instance.currentTask, output)
	if err != nil {
		w.logger.Error("Error producing task completion event: %v", err)
	}
	w.Pgp_Decrypter1(ctx, instance, input)
	return nil
}

func (w *_autobindTest3) waitEvent(ctx workflow.Context, instance *WorkflowInstance, input interface{}, taskName string, EventType string) error {
	w.logger.Info("Waiting for event: " + taskName + "\n")
	err := workflow.Await(ctx, func() bool {
		if result, exists := instance.EventMap[taskName]; exists {
			if result.EventType == EventType {
				return true
			}
		}
		return false
	})
	if err != nil {
		return err
	}

	if event, exists := instance.EventMap[taskName]; exists {
		instance.dataMap[taskName] = event.Result
		// Check if activity failed
		isSuccessStr, hasSuccessField := event.Result["is_success"].(string)
		isSuccessBool, hasSuccessBoolField := event.Result["is_success"].(bool)

		if (hasSuccessField && isSuccessStr == "false") ||
			(hasSuccessBoolField && !isSuccessBool) {
			// Extract error message
			var errorMsg string
			if errors, hasErrors := event.Result["errors"].([]interface{}); hasErrors && len(errors) > 0 {
				errorMsg = fmt.Sprintf("Activity reported errors: %v", errors)
			} else {
				errorMsg = "Activity failed without specific error details"
			}

			initialError := fmt.Errorf("%s", errorMsg)

			// Create an event checking function that will be used for retries
			// This function will check if a new successful event has arrived
			checkEventSuccess := func() (interface{}, error) {
				// Check if we have a new updated event with success=true
				if updatedEvent, exists := instance.EventMap[taskName]; exists {
					isSuccessStr, hasSuccessField := updatedEvent.Result["is_success"].(string)
					isSuccessBool, hasSuccessBoolField := updatedEvent.Result["is_success"].(bool)

					// Check if the event now indicates success
					if (hasSuccessField && isSuccessStr == "true") ||
						(hasSuccessBoolField && isSuccessBool) {
						return updatedEvent.Result, nil
					}
				}
				return nil, initialError
			}

			// Handle the retry loop for this failed event
			result, err := w.handleActivityRetryLoop(ctx, instance, taskName, taskName, checkEventSuccess, initialError)
			if err != nil {
				return err
			}

			// Update the data map with the successful result
			if resultMap, ok := result.(map[string]interface{}); ok {
				event := instance.EventMap[taskName]
				event.Result = resultMap
				instance.EventMap[taskName] = event
			}
		}
	}
	w.logger.Info("\nEventMap: \n" + fmt.Sprintf("%v", instance.EventMap))
	return nil
}

func (w *_autobindTest3) CustomUpdateHandler(ctx workflow.Context, instance *WorkflowInstance, updateName string) error {
	w.logger.Info("Entered CustomUpdateHandler for : " + updateName + "\n")
	workflowRunState := "InProgress"
	err := workflow.SetUpdateHandler(ctx, updateName, func(ctx workflow.Context, updateRequest interface{}) (interface{}, error) {
		if workflowRunState == "Completed" {
			w.logger.Error("workflow cannot accept updates in state: %%s", workflowRunState)
			return "", nil
		}
		requestMap, ok := updateRequest.(map[string]interface{})
		if !ok {
			w.logger.Error("invalid update request format")
			return nil, nil
		}
		resultValues, err := w.workflowIOManager.GetActualResult(workflow.GetInfo(ctx).Namespace, requestMap)
		if err != nil {
			return nil, err
		}
		var request Event
		common.ConvertToStruct(resultValues[0], &request)
		instance.EventMap[request.Name] = request
		//return map[string]interface{}{"Status": "Success"}, nil
		return request.Result, nil
	})
	if err != nil {
		w.logger.Error("workflow failed while setting up updatehandlers")
		return nil
	}
	return nil
}
func (w *_autobindTest3) Pgp_Decrypter1(ctx workflow.Context, instance *WorkflowInstance, input interface{}) error {
	instance.previousTask = instance.currentTask
	instance.currentTask = "Pgp_Decrypter1"
	instance.dataMap["current_task"] = instance.currentTask
	w.logger.Info("Executing task: Pgp_Decrypter1\n")

	dataMap, err := common.ConvertStructToMap(input)
	if err != nil {
		w.logger.Error("error converting input to map: %v", err)
		return err
	}
	w.logger.Info("dataMap: %s", dataMap)

	inputMap := make(map[string]interface{})
	inputMap["activity-id"] = "7ea983a2-b8be-4dad-8378-a0e0f1e126d5"
	inputMap["pgpdecryptor_in_file_names"] = instance.EventMap["File_Copy1"].Result["file_copy.out.file_names"]
	inputMap["pgpdecryptor_in_file_path"] = instance.EventMap["File_Copy1"].Result["file_copy.out.file_path"]

	inputMap["workflow_id"] = workflow.GetInfo(ctx).WorkflowExecution.ID
	instance.dataMap["input"] = inputMap

	dataId, err := w.PersistActualInput(ctx, instance.dataMap)
	if err != nil {
		w.logger.Error("Error persisting actual input: %%s", err)
		return err
	}

	err = w.produceWorkflowTaskStart(ctx, instance.currentTask, inputMap, instance)
	if err != nil {
		w.logger.Error("Error producing task status event: %v", err)
	}

	_, err = w.safeExecuteActivity(ctx, instance, "PGPDecryptorProxy", map[string]interface{}{"data_id": dataId})
	if err != nil {
		w.logger.Error("Workflow terminated during activity execution", zap.Error(err))
		return err
	}
	err = w.CustomUpdateHandler(ctx, instance, "PGPDecryptorProxy")
	if err != nil {
		return err
	}
	err = w.waitEvent(ctx, instance, input, "Pgp_Decrypter1", "PGP.com")
	if err != nil {
		return err
	}

	outputMap := instance.EventMap[instance.currentTask].Result
	output, err := common.ConvertStructToMap(outputMap)
	if err != nil {
		w.logger.Error("error converting input to map: %v", err)
		return err
	}

	err = w.produceWorkflowTaskCompletion(ctx, instance.currentTask, output)
	if err != nil {
		w.logger.Error("Error producing task completion event: %v", err)
	}
	w.Transformer_M1(ctx, instance, input)
	return nil
}

func (w *_autobindTest3) Transformer_M1(ctx workflow.Context, instance *WorkflowInstance, input interface{}) error {
	instance.previousTask = instance.currentTask
	instance.currentTask = "Transformer_M1"
	instance.dataMap["current_task"] = instance.currentTask
	w.logger.Info("Executing task: Transformer_M1\n")

	dataMap, err := common.ConvertStructToMap(input)
	if err != nil {
		w.logger.Error("error converting input to map: %v", err)
		return err
	}
	w.logger.Info("dataMap: %s", dataMap)

	inputMap := make(map[string]interface{})
	inputMap["transformerm_in_file_path"] = instance.EventMap["Pgp_Decrypter1"].Result["pgp_decryption.out.file_path"]
	inputMap["activity-id"] = "3d80d9fb-75cc-4a82-96c4-68d63ffae663"
	inputMap["transformerm_in_file_names"] = instance.EventMap["Pgp_Decrypter1"].Result["pgp_decryption.out.file_names"]

	inputMap["workflow_id"] = workflow.GetInfo(ctx).WorkflowExecution.ID
	instance.dataMap["input"] = inputMap

	dataId, err := w.PersistActualInput(ctx, instance.dataMap)
	if err != nil {
		w.logger.Error("Error persisting actual input: %%s", err)
		return err
	}

	err = w.produceWorkflowTaskStart(ctx, instance.currentTask, inputMap, instance)
	if err != nil {
		w.logger.Error("Error producing task status event: %v", err)
	}

	_, err = w.safeExecuteActivity(ctx, instance, "TxRunManualProcessActivity", map[string]interface{}{"data_id": dataId})
	if err != nil {
		w.logger.Error("Workflow terminated during activity execution", zap.Error(err))
		return err
	}
	err = w.CustomUpdateHandler(ctx, instance, "TxRunManualProcessActivity")
	if err != nil {
		return err
	}
	err = w.waitEvent(ctx, instance, input, "Transformer_M1", "TX.com")
	if err != nil {
		return err
	}

	outputMap := instance.EventMap[instance.currentTask].Result
	output, err := common.ConvertStructToMap(outputMap)
	if err != nil {
		w.logger.Error("error converting input to map: %v", err)
		return err
	}

	err = w.produceWorkflowTaskCompletion(ctx, instance.currentTask, output)
	if err != nil {
		w.logger.Error("Error producing task completion event: %v", err)
	}
	w.exit(ctx, instance)
	return nil
}

func (w *_autobindTest3) exit(ctx workflow.Context, instance *WorkflowInstance) {
	instance.currentTask = "All tasks Executed"
	w.logger.Info("Workflow completed\n")
	err := w.produceWorkflowCompletion(ctx, instance.dataMap)
	if err != nil {
		w.logger.Error("Error producing workflow completion event: %%w", err)
	}
}
