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

package org.apache.flink.runtime.execution;

/**
 * An enumeration of all states that a task can be in during its execution.
 * Tasks usually start in the state {@code CREATED} and switch states according to
 * this diagram:
 * 在执行中，一个task可能的所有状态的枚举。
 * tasks一般开始于 CREATED 状态，状态转换依据下图：
 * <pre>{@code
 *
 *     CREATED  -> SCHEDULED -> DEPLOYING -> RUNNING -> FINISHED
 *        |            |            |          |
 *        |            |            |   +------+
 *        |            |            V   V
 *        |            |         CANCELLING -----+----> CANCELED
 *        |            |                         |
 *        |            +-------------------------+
 *        |
 *        |                                   ... -> FAILED
 *        V
 *    RECONCILING  -> RUNNING | FINISHED | CANCELED | FAILED
 *
 * }</pre>
 *
 * <p>It is possible to enter the {@code RECONCILING} state from {@code CREATED}
 * state if job manager fail over, and the {@code RECONCILING} state can switch into
 * any existing task state.
 * 如果 JobManager 故障重置了，则会导致job的状态从 CREATED 转变到 RECONCOLING，
 * RECONCILING 状态可以转换到任何以后的任务状态。
 *
 * <p>It is possible to enter the {@code FAILED} state from any other state.
 * 从任何状态都可能变成 FAILED 状态
 *
 * <p>The states {@code FINISHED}, {@code CANCELED}, and {@code FAILED} are
 * considered terminal states.
 * {@code FINISHED}, {@code CANCELED}, and {@code FAILED} 状态都是终止状态。
 */
public enum ExecutionState {

	CREATED,
	
	SCHEDULED,
	
	DEPLOYING,
	
	RUNNING,

	/**
	 * This state marks "successfully completed". It can only be reached when a
	 * program reaches the "end of its input". The "end of input" can be reached
	 * when consuming a bounded input (fix set of files, bounded query, etc) or
	 * when stopping a program (not cancelling!) which make the input look like
	 * it reached its end at a specific point.
	 * 这个状态标记为"成功完成"。
	 * 只有当一个程序达到"输入结束"时才能达到该状态。
	 * 当消费一个有界的输入(固定集合的文件、有界查询、等) 或者 当停止一个程序(不是取消)也会让输入看起来像在指定点达到了结束。
	 */
	FINISHED,
	
	CANCELING,
	
	CANCELED,
	
	FAILED,

	RECONCILING;

	public boolean isTerminal() {
		return this == FINISHED || this == CANCELED || this == FAILED;
	}
}
