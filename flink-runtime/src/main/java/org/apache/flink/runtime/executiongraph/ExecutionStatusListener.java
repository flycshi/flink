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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.JobVertexID;

/**
 * Interface for observers that monitor the status of individual task executions.
 * 监视个别任务执行状态的观察者接口
 */
public interface ExecutionStatusListener {

	/**
	 * Called whenever the execution status of a task changes.
	 * 当一个任务的状态变化时被调用
	 * 
	 * @param jobID                  The ID of the job job的id
	 * @param vertexID               The ID of the task vertex JobVertex的id
	 * @param taskName               The name of the task	task名称
	 * @param totalNumberOfSubTasks  The parallelism of the task	子任务的并行度
	 * @param subtaskIndex           The subtask's parallel index	子任务的并行索引
	 * @param executionID            The ID of the execution attempt	执行的尝试id
	 * @param newExecutionState      The status to which the task switched	新状态
	 * @param timestamp              The timestamp when the change occurred. Informational only.
	 * @param optionalMessage        An optional message attached to the status change, like an
	 *                               exception message.
	 */
	void executionStatusChanged(
			JobID jobID,
			JobVertexID vertexID,
			String taskName,
			int totalNumberOfSubTasks,
			int subtaskIndex,
			ExecutionAttemptID executionID,
			ExecutionState newExecutionState,
			long timestamp,
			String optionalMessage);
}
