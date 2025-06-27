package workflows

import (
	"time"
	"gitlab/rFlow/workflow-worker/activities"
	"encoding/json"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"gitlab/rFlow/workflow-worker/common"
	"gitlab/rFlow/workflow-worker/dependencies"
	"go.uber.org/zap"
	"fmt"
)

// Document-Name: Dashboardtesting3
// Version: 1
// Type or Namespace: _Dashboardtesting3
// Dsl: 1

type _Dashboardtesting3 struct {
	dataMap            map[string]interface{}
	configMap          map[string]interface{}
	EventMap           map[string]Event
	workflowIOManager  dependencies.WorkflowIOManager
	currentTask       string
	previousTask      string
	taskHistory       map[string]interface{}
	logger            log.Logger
	failedActivities  map[string]ActivityFailure
	activityWaitChan  map[string]workflow.Channel
	signalChan        workflow.ReceiveChannel
}

type ActivityFailure struct {
	ActivityName string    `json:"activity_name"`
	TaskName     string    `json:"task_name"`
	Error        string    `json:"error"`
	FailedAt     time.Time `json:"failed_at"`
	RetryCount   int       `json:"retry_count"`
}

func New_Dashboardtesting3(workflowIOManager dependencies.WorkflowIOManager) *_Dashboardtesting3 {
	return &_Dashboardtesting3{
		dataMap:           map[string]interface{}{},
		EventMap:          map[string]Event{},
		workflowIOManager: workflowIOManager,
		logger:            nil,
		currentTask:       "",
		previousTask:      "",
		taskHistory:       map[string]interface{}{},
		failedActivities:  make(map[string]ActivityFailure),
		activityWaitChan:  make(map[string]workflow.Channel),
		signalChan:        nil,
	}
}

type Event struct {
	EventID   string                 `json:"event_id"`
	EventType string                 `json:"event_type"`
	Name      string                 `json:"name"`
	Result    map[string]interface{} `json:"result"`
}

func (w *_Dashboardtesting3) trackTaskComplete(taskName string) {
	timestamp := time.Now().Format(time.RFC3339)
	w.taskHistory[taskName] = timestamp
	w.logger.Info("Task completed: %s", taskName)
}


func (w *_Dashboardtesting3) Execute(ctx workflow.Context, input interface{}) (string, error) {

	w.logger = workflow.GetLogger(ctx)
	w.logger.Info("Executing RdexDemo workflow with input: %v", input)
	retrypolicy := &temporal.RetryPolicy{
		InitialInterval:    time.Second,
		BackoffCoefficient: 2.0,
		MaximumAttempts:    1,
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		HeartbeatTimeout:    10 * time.Second,
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
			return w.failedActivities, nil
		}); err != nil {
			w.logger.Error("failed to register failed activities query handler", zap.Error(err))
			return
		}

		// Only start processing signals when there's at least one failed activity
		for {
			// Check if there are any failed activities to retry
			if len(w.failedActivities) == 0 {
				// No failed activities, sleep briefly and check again
				_ = workflow.Sleep(ctx, 500*time.Millisecond)
				continue
			}

			// Now we have failed activities, start processing signals
			var activityName string
			w.signalChan.Receive(ctx, &activityName)

			w.logger.Info("Received retry signal for activity",
				zap.String("activity", activityName))

			// Verify it's a known failed activity
			if _, exists := w.failedActivities[activityName]; !exists {
				w.logger.Warn("Received retry signal for unknown failed activity",
					zap.String("activity", activityName))
				continue
			}

			// Find the channel for this activity and signal it
			if ch, exists := w.activityWaitChan[activityName]; exists {
				// Forward the signal to the activity's channel
				ch.Send(ctx, true)
				w.logger.Info("Forwarded retry signal to activity",
					zap.String("activity", activityName))
			} else {
				w.logger.Warn("No waiting channel found for activity",
					zap.String("activity", activityName))
			}
		}
	})

	// Register query handler for tracking task execution
	if err := workflow.SetQueryHandler(ctx, "getCurrentTask", func() (string, error) {
		if w.currentTask == "" {
			return "No task currently executing", nil
		}
		return w.currentTask, nil
	}); err != nil {
		w.logger.Error("failed to register query handler : _Dashboardtesting3", err)
		return "", err
	}

	if err := workflow.SetQueryHandler(ctx, "getTaskHistory", func() (map[string]interface{}, error) {
		return w.taskHistory, nil
	}); err != nil {
		w.logger.Error("failed to register task history query handler: %v", err)
		return "", err
	}
	inputMap, err := common.ConvertStructToMap(input)
	if err != nil {
		w.logger.Error("error converting input to map: %v", err)
		return "", err
	}
	inputMap["workflow_id"] = workflow.GetInfo(ctx).WorkflowExecution.ID
	inputMap["run_id"] = workflow.GetInfo(ctx).WorkflowExecution.RunID
	inputMap["tenant_id"] = workflow.GetInfo(ctx).Namespace
	inputMap["workflow_name"] = workflow.GetInfo(ctx).WorkflowType.Name

	// Store input in workflow data map
	w.dataMap["input"] = input


	// Execute first task
	if err := w.File_Copy1(ctx, w.dataMap); err != nil {
		return "", err
	}


	dataId, err := w.PersistActualInput(ctx, w.dataMap)
	if err != nil {
		w.logger.Error("Error persisting actual input: %v",err)
		return "", err
	}

	return dataId, nil
}

func (w *_Dashboardtesting3) PersistActualInput(ctx workflow.Context, data interface{}) (string, error) {
	persister := dependencies.NewWorkflowDataPersister().
		WithTenantID(workflow.GetInfo(ctx).Namespace).
		WithData(data).
		WithPersister(w.workflowIOManager)

	var dataId string
	handle := workflow.ExecuteLocalActivity(ctx, persister.Persist)

	if err := handle.Get(ctx, &dataId); err != nil {
		w.logger.Error("Error persisting actual input: %v" ,err)
		return "", err
	}
	return dataId, nil
}

func (w *_Dashboardtesting3) handleActivityRetryLoop(ctx workflow.Context, activityName, taskName string, execute func() (interface{}, error), initialError error) (interface{}, error) {
	// Log the initial failure
	w.logger.Error("Activity failed",
		zap.String("activity", activityName),
		zap.String("task", taskName),
		zap.Error(initialError))

	// Create a channel for waiting on retry signals if it doesn't exist
	retryChan, exists := w.activityWaitChan[activityName]
	if !exists {
		retryChan = workflow.NewChannel(ctx)
		w.activityWaitChan[activityName] = retryChan
	}

	// Record failure details - this will activate the signal processing in the Execute method
	w.failedActivities[activityName] = ActivityFailure{
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
		failure := w.failedActivities[activityName]
		failure.RetryCount++
		w.failedActivities[activityName] = failure

		w.logger.Info("Retrying failed activity",
			zap.String("activity", activityName),
			zap.String("task", taskName),
			zap.Int("attempt", failure.RetryCount))

		// Try the execution again using the provided function
		result, err := execute()

		if err == nil {
			// Success! Remove from failed activities
			delete(w.failedActivities, activityName)
			delete(w.activityWaitChan, activityName) // Clean up the channel too

			w.logger.Info("Activity retry successful",
				zap.String("activity", activityName))

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

			return result, nil
		}

		// Still failed - update error message
		failure = w.failedActivities[activityName]
		failure.Error = err.Error()
		w.failedActivities[activityName] = failure

		w.logger.Error("Activity retry failed",
			zap.String("activity", activityName),
			zap.String("task", taskName),
			zap.Error(err),
			zap.Int("attempt", failure.RetryCount))

	}
}
	
func (w *_Dashboardtesting3) produceWorkflowTaskStart(ctx workflow.Context, taskName string, input map[string]interface{}) error {
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
		"previous_task": w.previousTask,
		"tenant_id":     info.Namespace,
		"workflow_type": info.WorkflowType.Name,
		"timestamp":     time.Now(),
		"input":         input,
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

func (w *_Dashboardtesting3) produceWorkflowTaskCompletion(ctx workflow.Context, taskName string, outputMap map[string]interface{}) error {
	// Get workflow execution info
	info := workflow.GetInfo(ctx)
	workflowID := info.WorkflowExecution.ID
	runID := info.WorkflowExecution.RunID

	// Create the event payload
	completionEventPayload := map[string]interface{}{
		"event_type":    "workflow_task_complete",
		"workflow_id":   workflowID,
		"run_id":        runID,
		"current_task":  taskName,
		"tenant_id":     info.Namespace,
		"workflow_type": info.WorkflowType.Name,
		"timestamp":     time.Now(),
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

	
func (w *_Dashboardtesting3) safeExecuteActivity(ctx workflow.Context, activityName string, input interface{}) (interface{}, error) {
	var result interface{}

	// Execute activity
	activityError := workflow.ExecuteActivity(ctx, activityName, input).Get(ctx, &result)

	// If no error, return the result
	if activityError == nil {
		return result, nil
	}

	// Activity failed - log the error
	w.logger.Error("Activity failed",
		zap.String("activity", activityName),
		zap.String("task", w.currentTask),
		zap.Error(activityError))

	// Create a channel for waiting on retry signals if it doesn't exist
	retryChan, exists := w.activityWaitChan[activityName]
	if !exists {
		retryChan = workflow.NewChannel(ctx)
		w.activityWaitChan[activityName] = retryChan
	}

	// Record failure details - this will activate the signal processing in the Execute method
	w.failedActivities[activityName] = ActivityFailure{
		ActivityName: activityName,
		TaskName:     w.currentTask,
		Error:        activityError.Error(),
		FailedAt:     workflow.Now(ctx),
		RetryCount:   0,
	}

	// Log workflow status for monitoring
	w.logger.Info("Workflow paused - waiting for retry signal",
		zap.String("activity", activityName),
		zap.String("task", w.currentTask))

	w.logger.Error("WORKFLOW_WAITING_FOR_RETRY",
		zap.String("workflow_id", workflow.GetInfo(ctx).WorkflowExecution.ID),
		zap.String("activity", activityName),
		zap.String("error", activityError.Error()))

	// Produce a workflow event about activity failure if needed
	errorPayload := map[string]interface{}{
		"event_type":    "activity_failed",
		"workflow_id":   workflow.GetInfo(ctx).WorkflowExecution.ID,
		"run_id":        workflow.GetInfo(ctx).WorkflowExecution.RunID,
		"activity_name": activityName,
		"task_name":     w.currentTask,
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
	for {
		// Create a selector to wait for the retry signal
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
		failure := w.failedActivities[activityName]
		failure.RetryCount++
		w.failedActivities[activityName] = failure

		w.logger.Info("Retrying failed activity",
			zap.String("activity", activityName),
			zap.String("task", w.currentTask),
			zap.Int("attempt", failure.RetryCount))

		// Try the activity again
		activityError = workflow.ExecuteActivity(ctx, activityName, input).Get(ctx, &result)

		if activityError == nil {
			// Success! Remove from failed activities
			delete(w.failedActivities, activityName)
			delete(w.activityWaitChan, activityName) // Clean up the channel too

			w.logger.Info("Activity retry successful",
				zap.String("activity", activityName))

			// Produce a success event
			successPayload := map[string]interface{}{
				"event_type":    "activity_retry_succeeded",
				"workflow_id":   workflow.GetInfo(ctx).WorkflowExecution.ID,
				"activity_name": activityName,
				"task_name":     w.currentTask,
				"attempts":      failure.RetryCount,
				"timestamp":     workflow.Now(ctx).Format(time.RFC3339),
			}
			jsonBytes, _ := json.Marshal(successPayload)

			workflow.ExecuteLocalActivity(
				laCtx,
				activities.ProduceWorkflowHistoryActivity,
				workflow.GetInfo(ctx).WorkflowExecution.ID,
				jsonBytes,)

			return result, nil
		}

		// Still failed - update error message
		failure = w.failedActivities[activityName]
		failure.Error = activityError.Error()
		w.failedActivities[activityName] = failure

		w.logger.Error("Activity retry failed",
			zap.String("activity", activityName),
			zap.String("task", w.currentTask),
			zap.Error(activityError),
			zap.Int("attempt", failure.RetryCount))

		// Continue loop to wait for another signal
	}
}

	
func (w *_Dashboardtesting3) File_Copy1(ctx workflow.Context, input interface{}) error {
	w.previousTask = w.currentTask
	w.currentTask = "File_Copy1"
	w.dataMap["current_task"] = w.currentTask
	w.logger.Info("Executing task: File_Copy1\n")


	inputMap, err := common.ConvertStructToMap(input)
	if err != nil {
		w.logger.Error("error converting input to map: %v", err)
		return err
	}
	w.logger.Info("inputMap: %s", inputMap)

	err = w.produceWorkflowTaskStart(ctx, w.currentTask,inputMap)
	if err != nil {
		w.logger.Error("Error producing task status event: %v", err)
	}


		w.dataMap["filecopy_in_file_path"] = w.EventMap["File_Listener1"].Result["file_listener.out.file_path"]
	w.dataMap["activity-id"] = "4240f3fe-211d-4f05-b217-48dde89c343a"
	w.dataMap["filecopy_in_file_names"] = w.EventMap["File_Listener1"].Result["file_listener.out.file_names"]
	dataId, err := w.PersistActualInput(ctx, w.dataMap)
	if err != nil {
		w.logger.Error("Error persisting actual input: %%s",err)
		return err
	}

_, err = w.safeExecuteActivity(ctx, "FileCopyProxy", map[string]interface{}{"data_id": dataId})
	if err != nil {
		w.logger.Error("Workflow terminated during activity execution", zap.Error(err))
		return err
	}
	err = w.CustomUpdateHandler(ctx, "FileCopyProxy")
	if err != nil {
		return err
	}
	err = w.waitEvent(ctx, input, "File_Copy1", "file_copy.com")
	if err != nil {
		return err
	}
    w.trackTaskComplete("File_Copy1")

	outputMap := w.dataMap[w.currentTask]
	output, err := common.ConvertStructToMap(outputMap)
	if err != nil {
		w.logger.Error("error converting input to map: %v", err)
		return err
	}

	err = w.produceWorkflowTaskCompletion(ctx, w.currentTask,output)
	if err != nil {
		w.logger.Error("Error producing task completion event: %v", err)
	}

		w.Pgp_Decrypter1(ctx, input)
	return nil
}



func (w *_Dashboardtesting3) waitEvent(ctx workflow.Context, input interface{}, taskName string, EventType string) error {
	w.logger.Info("Waiting for event: "+ taskName+"\n")
	err := workflow.Await(ctx, func() bool {
        if result, exists := w.EventMap[taskName]; exists {
            if result.EventType == EventType {
                return true
            }
        }
        return false
    })
    if err != nil {
        return err
    }

    if event, exists := w.EventMap[taskName]; exists {
        w.dataMap[taskName] = event.Result

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
				if updatedEvent, exists := w.EventMap[taskName]; exists {
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
			result, err := w.handleActivityRetryLoop(ctx, taskName, taskName, checkEventSuccess, initialError)
			if err != nil {
				return err
			}

			// Update the data map with the successful result
			if resultMap, ok := result.(map[string]interface{}); ok {
				w.dataMap[taskName] = resultMap
			}
		}
    }
	w.logger.Info("\nEventMap: \n" + fmt.Sprintf("%v", w.EventMap))
    return nil
}

func (w *_Dashboardtesting3) CustomUpdateHandler(ctx workflow.Context, updateName string) error {
	w.logger.Info("Entered CustomUpdateHandler for : "+ updateName+"\n")
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
		w.EventMap[request.Name] = request
		//return map[string]interface{}{"Status": "Success"}, nil
		return request.Result,nil
	})
	if err != nil {
		w.logger.Error("workflow failed while setting up updatehandlers")
		return nil
	}
	return nil
}
func (w *_Dashboardtesting3) Pgp_Decrypter1(ctx workflow.Context, input interface{}) error {
	w.previousTask = w.currentTask
	w.currentTask = "Pgp_Decrypter1"
	w.dataMap["current_task"] = w.currentTask
	w.logger.Info("Executing task: Pgp_Decrypter1\n")


	inputMap, err := common.ConvertStructToMap(input)
	if err != nil {
		w.logger.Error("error converting input to map: %v", err)
		return err
	}
	w.logger.Info("inputMap: %s", inputMap)

	err = w.produceWorkflowTaskStart(ctx, w.currentTask,inputMap)
	if err != nil {
		w.logger.Error("Error producing task status event: %v", err)
	}


		w.dataMap["pgpdecryptor_in_file_path"] = w.EventMap["File_Copy1"].Result["file_copy.out.file_path"]
	w.dataMap["activity-id"] = "627e9d97-9ef8-43e0-ac6d-e08210d25066"
	w.dataMap["pgpdecryptor_in_file_names"] = w.EventMap["File_Copy1"].Result["file_copy.out.file_names"]
	dataId, err := w.PersistActualInput(ctx, w.dataMap)
	if err != nil {
		w.logger.Error("Error persisting actual input: %%s",err)
		return err
	}

_, err = w.safeExecuteActivity(ctx, "PGPDecryptorProxy", map[string]interface{}{"data_id": dataId})
	if err != nil {
		w.logger.Error("Workflow terminated during activity execution", zap.Error(err))
		return err
	}
	err = w.CustomUpdateHandler(ctx, "PGPDecryptorProxy")
	if err != nil {
		return err
	}
	err = w.waitEvent(ctx, input, "Pgp_Decrypter1", "PGP.com")
	if err != nil {
		return err
	}
    w.trackTaskComplete("Pgp_Decrypter1")

	outputMap := w.dataMap[w.currentTask]
	output, err := common.ConvertStructToMap(outputMap)
	if err != nil {
		w.logger.Error("error converting input to map: %v", err)
		return err
	}

	err = w.produceWorkflowTaskCompletion(ctx, w.currentTask,output)
	if err != nil {
		w.logger.Error("Error producing task completion event: %v", err)
	}

		w.Transformer_M1(ctx, input)
	return nil
}

func (w *_Dashboardtesting3) Transformer_M1(ctx workflow.Context, input interface{}) error {
	w.previousTask = w.currentTask
	w.currentTask = "Transformer_M1"
	w.dataMap["current_task"] = w.currentTask
	w.logger.Info("Executing task: Transformer_M1\n")


	inputMap, err := common.ConvertStructToMap(input)
	if err != nil {
		w.logger.Error("error converting input to map: %v", err)
		return err
	}
	w.logger.Info("inputMap: %s", inputMap)

	err = w.produceWorkflowTaskStart(ctx, w.currentTask,inputMap)
	if err != nil {
		w.logger.Error("Error producing task status event: %v", err)
	}


		w.dataMap["transformerm_in_file_path"] = w.EventMap["Pgp_Decrypter1"].Result["pgp_decryption.out.file_path"]
	w.dataMap["activity-id"] = "2428a69c-fe67-4303-9ba8-d250d0c7c59b"
	w.dataMap["transformerm_in_file_names"] = w.EventMap["Pgp_Decrypter1"].Result["pgp_decryption.out.file_names"]
	dataId, err := w.PersistActualInput(ctx, w.dataMap)
	if err != nil {
		w.logger.Error("Error persisting actual input: %%s",err)
		return err
	}

_, err = w.safeExecuteActivity(ctx, "TxRunManualProcessActivity", map[string]interface{}{"data_id": dataId})
	if err != nil {
		w.logger.Error("Workflow terminated during activity execution", zap.Error(err))
		return err
	}
	err = w.CustomUpdateHandler(ctx, "TxRunManualProcessActivity")
	if err != nil {
		return err
	}
	err = w.waitEvent(ctx, input, "Transformer_M1", "TX.com")
	if err != nil {
		return err
	}
    w.trackTaskComplete("Transformer_M1")

	outputMap := w.dataMap[w.currentTask]
	output, err := common.ConvertStructToMap(outputMap)
	if err != nil {
		w.logger.Error("error converting input to map: %v", err)
		return err
	}

	err = w.produceWorkflowTaskCompletion(ctx, w.currentTask,output)
	if err != nil {
		w.logger.Error("Error producing task completion event: %v", err)
	}

		w.exit()
	return nil
}

func (w *_Dashboardtesting3) exit() {
	w.currentTask = "All tasks Executed"
	w.logger.Info("Workflow completed\n")
}