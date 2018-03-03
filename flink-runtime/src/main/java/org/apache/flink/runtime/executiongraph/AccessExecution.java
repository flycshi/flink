/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

/**
 * Common interface for the runtime {@link Execution and {@link ArchivedExecution}.
 * 运行时 Execution 和 ArchivedExecution 的公共接口
 */
public interface AccessExecution {
	/**
	 * Returns the {@link ExecutionAttemptID} for this Execution.
	 * 返回这个 Execution 的 ExecutionAttemptID
	 *
	 * @return ExecutionAttemptID for this execution
	 */
	ExecutionAttemptID getAttemptId();

	/**
	 * Returns the attempt number for this execution.
	 * 返回这个 Execution 的尝试次数
	 *
	 * @return attempt number for this execution.
	 */
	int getAttemptNumber();

	/**
	 * Returns the timestamps for every {@link ExecutionState}.
	 * 返回每个 ExecutionState 的时间戳
	 *
	 * @return timestamps for each state
	 */
	long[] getStateTimestamps();

	/**
	 * Returns the current {@link ExecutionState} for this execution.
	 * 返回该 Execution 的当前 ExecutionState
	 *
	 * @return execution state for this execution
	 */
	ExecutionState getState();

	/**
	 * Returns the {@link TaskManagerLocation} for this execution.
	 * 返回该 Execution 的 TaskManagerLocation
	 *
	 * @return taskmanager location for this execution.
	 */
	TaskManagerLocation getAssignedResourceLocation();

	/**
	 * Returns the exception that caused the job to fail. This is the first root exception
	 * that was not recoverable and triggered job failure.
	 * 返回导致job失败的异常。
	 * 这是第一个可恢复的根异常，触发job的失败
	 *
	 * @return failure exception as a string, or {@code "(null)"}
	 */
	String getFailureCauseAsString();

	/**
	 * Returns the timestamp for the given {@link ExecutionState}.
	 * 返回指定 ExecutionState 的时间戳
	 *
	 * @param state state for which the timestamp should be returned
	 * @return timestamp for the given state
	 */
	long getStateTimestamp(ExecutionState state);

	/**
	 * Returns the user-defined accumulators as strings.
	 * 返回用户自定义累加器的字符串
	 *
	 * @return user-defined accumulators as strings.
	 */
	StringifiedAccumulatorResult[] getUserAccumulatorsStringified();

	/**
	 * Returns the subtask index of this execution.
	 * 返回该 Execution 的子任务索引
	 *
	 * @return subtask index of this execution.
	 */
	int getParallelSubtaskIndex();

	IOMetrics getIOMetrics();
}
