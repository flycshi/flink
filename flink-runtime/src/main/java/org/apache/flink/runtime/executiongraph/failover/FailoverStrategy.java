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

package org.apache.flink.runtime.executiongraph.failover;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;

import java.util.List;

/**
 * A {@code FailoverStrategy} describes how the job computation recovers from task
 * failures.
 * {@code FailoverStrategy}描述了如果从task失败中恢复job计算。
 * 
 * <p>Failover strategies implement recovery logic for failures of tasks. The execution
 * graph still implements "global failure / recovery" (which restarts all tasks) as
 * a fallback plan or safety net in cases where it deems that the state of the graph
 * may have become inconsistent.
 * 容灾策略为任务的失败实现了恢复逻辑。
 * 执行图仍需实现"global failure / recovery"(通过重启所有任务), 作为一个备用计划或者安全网, 在出现图的状态看上去已经不一致的情况下。
 */
public abstract class FailoverStrategy {


	// ------------------------------------------------------------------------
	//  failover implementation
	//  容灾实现
	// ------------------------------------------------------------------------ 

	/**
	 * Called by the execution graph when a task failure occurs.
	 * 当一个task失败发生时, 执行图会调用该方法
	 * 
	 * @param taskExecution The execution attempt of the failed task. 失败任务的执行尝试
	 * @param cause The exception that caused the task failure.	导致task失败的异常
	 */
	public abstract void onTaskFailure(Execution taskExecution, Throwable cause);

	/**
	 * Called whenever new vertices are added to the ExecutionGraph.
	 * 当新的{@link ExecutionJobVertex}添加到{@link ExecutionGraph}
	 * 
	 * @param newJobVerticesTopological The newly added vertices, in topological order.
	 */
	public abstract void notifyNewVertices(List<ExecutionJobVertex> newJobVerticesTopological);

	/**
	 * Gets the name of the failover strategy, for logging purposes.
	 * 获取容灾策略的名称, 用于log
	 */
	public abstract String getStrategyName();

	/**
	 * Tells the FailoverStrategy to register its metrics.
	 * 告诉{@code FailoverStrategy}去注册它的度量
	 * 
	 * <p>The default implementation does nothing
	 * 		默认实现不做任何事
	 * 
	 * @param metricGroup The metric group to register the metrics at
	 */
	public void registerMetrics(MetricGroup metricGroup) {}

	// ------------------------------------------------------------------------
	//  factory
	//  工厂
	// ------------------------------------------------------------------------

	/**
	 * This factory is a necessary indirection when creating the FailoverStrategy to that
	 * we can have both the FailoverStrategy final in the ExecutionGraph, and the
	 * ExecutionGraph final in the FailOverStrategy.
	 */
	public interface Factory {

		/**
		 * Instantiates the {@code FailoverStrategy}.
		 * 
		 * @param executionGraph The execution graph for which the strategy implements failover.
		 * @return The instantiated failover strategy.
		 */
		FailoverStrategy create(ExecutionGraph executionGraph);
	}
}
