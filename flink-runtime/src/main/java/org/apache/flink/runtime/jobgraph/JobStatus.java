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

package org.apache.flink.runtime.jobgraph;

/**
 * Possible states of a job once it has been accepted by the job manager.
 * job被job manager接受的可能状态
 */
public enum JobStatus {

	/**
	 * Job is newly created, no task has started to run.
	 * job是新建的, 还没有task被执行
	 */
	CREATED(TerminalState.NON_TERMINAL),

	/**
	 * Some tasks are scheduled or running, some may be pending, some may be finished.
	 * 一些task被调度、或执行, 一些可能被悬挂, 一些可能完成了。
	 */
	RUNNING(TerminalState.NON_TERMINAL),

	/**
	 * The job has failed and is currently waiting for the cleanup to complete
	 * job失败了,当前等待清理工作
	 */
	FAILING(TerminalState.NON_TERMINAL),
	
	/**
	 * The job has failed with a non-recoverable task failure
	 * job失败了,且不可恢复
	 */
	FAILED(TerminalState.GLOBALLY),

	/**
	 * Job is being cancelled
	 * job正在被取消
	 */
	CANCELLING(TerminalState.NON_TERMINAL),
	
	/**
	 * Job has been cancelled
	 * job已经被取消了
	 */
	CANCELED(TerminalState.GLOBALLY),

	/**
	 * All of the job's tasks have successfully finished.
	 * job的所有task都已经成功完成
	 */
	FINISHED(TerminalState.GLOBALLY),
	
	/**
	 * The job is currently undergoing a reset and total restart
	 * job当前在重置,进行完全重启
	 */
	RESTARTING(TerminalState.NON_TERMINAL),

	/**
	 * The job has been suspended which means that it has been stopped but not been removed from a
	 * potential HA job store.
	 * job被悬挂, 意味着它被停止了, 但是没有被从HA job存储中移除
	 */
	SUSPENDED(TerminalState.LOCALLY),

	/**
	 * The job is currently reconciling and waits for task execution report to recover state.
	 * job当前处于调解状态, 等待task执行报告恢复状态
	 */
	RECONCILING(TerminalState.NON_TERMINAL);
	
	// --------------------------------------------------------------------------------------------

	private enum TerminalState {
		NON_TERMINAL,
		LOCALLY,
		GLOBALLY
	}
	
	private final TerminalState terminalState;
	
	JobStatus(TerminalState terminalState) {
		this.terminalState = terminalState;
	}

	/**
	 * Checks whether this state is <i>globally terminal</i>. A globally terminal job
	 * is complete and cannot fail any more and will not be restarted or recovered by another
	 * standby master node.
	 * 检查状态是否是 globally terminal 。 一个 globally terminal job 是指完成了,且不能再被失败了,且不能被其他后备master节点重启或者恢复了。
	 * 
	 * <p>When a globally terminal state has been reached, all recovery data for the job is
	 * dropped from the high-availability services.
	 * 当变成 全局终止状态 时, 所有的job的恢复数据会被从HA服务中删除。
	 * 
	 * @return True, if this job status is globally terminal, false otherwise.
	 */
	public boolean isGloballyTerminalState() {
		return terminalState == TerminalState.GLOBALLY;
	}

	/**
	 * Checks whether this state is <i>locally terminal</i>. Locally terminal refers to the
	 * state of a job's execution graph within an executing JobManager. If the execution graph
	 * is locally terminal, the JobManager will not continue executing or recovering the job.
	 * 检查状态是否是 locally terminal 。
	 * locally terminal 引用的是在JobManager中执行的执行图的状态,
	 * 如果执行图是locally terminal, JobManager将不会继续执行或者恢复job。
	 *
	 * <p>The only state that is locally terminal, but not globally terminal is {@link #SUSPENDED},
	 * which is typically entered when the executing JobManager looses its leader status.
	 * 
	 * @return True, if this job status is terminal, false otherwise.
	 */
	public boolean isTerminalState() {
		return terminalState != TerminalState.NON_TERMINAL;
	}
}


