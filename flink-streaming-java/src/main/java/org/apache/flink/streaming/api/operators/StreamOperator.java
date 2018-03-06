/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import java.io.Serializable;

/**
 * Basic interface for stream operators. Implementers would implement one of
 * {@link org.apache.flink.streaming.api.operators.OneInputStreamOperator} or
 * {@link org.apache.flink.streaming.api.operators.TwoInputStreamOperator} to create operators
 * that process elements.
 * 流操作符的基础接口。
 * 可以实现{@link OneInputStreamOperator}或者{@link TwoInputStreamOperator}中的一个, 来创建一个操作符, 进行处理数据。
 *
 * <p>The class {@link org.apache.flink.streaming.api.operators.AbstractStreamOperator}
 * offers default implementation for the lifecycle and properties methods.
 * {@link AbstractStreamOperator}这个抽象类提供了生命周期与属性相关方法的默认实现。
 *
 * <p>Methods of {@code StreamOperator} are guaranteed not to be called concurrently. Also, if using
 * the timer service, timer callbacks are also guaranteed not to be called concurrently with
 * methods on {@code StreamOperator}.
 * {@link StreamOperator}的方法需要保证不会并发调用。
 * 另外，如果使用定时器服务，也需要保证在{@code StreamOperator}的方法中不会并发调用定时器
 *
 * @param <OUT> The output type of the operator	操作符的输入数据类型
 */
@PublicEvolving
public interface StreamOperator<OUT> extends Serializable {

	// ------------------------------------------------------------------------
	//  life cycle
	//  生命周期
	// ------------------------------------------------------------------------

	/**
	 * Initializes the operator. Sets access to the context and the output.
	 * 初始化操作符。
	 * 设置对上下文和输出的访问。
	 */
	void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<OUT>> output);

	/**
	 * This method is called immediately before any elements are processed, it should contain the
	 * operator's initialization logic.
	 * 该方法在任何元素被处理之前就会被立即调用，它应该包含操作符的初始化逻辑。
	 *
	 * @throws java.lang.Exception An exception in this method causes the operator to fail.
	 */
	void open() throws Exception;

	/**
	 * This method is called after all records have been added to the operators via the methods
	 * {@link org.apache.flink.streaming.api.operators.OneInputStreamOperator#processElement(StreamRecord)}, or
	 * {@link org.apache.flink.streaming.api.operators.TwoInputStreamOperator#processElement1(StreamRecord)} and
	 * {@link org.apache.flink.streaming.api.operators.TwoInputStreamOperator#processElement2(StreamRecord)}.
	 * 通过这些方法将所有记录都添加到操作符后，该方法会被调用。
	 *
	 * <p>The method is expected to flush all remaining buffered data. Exceptions during this
	 * flushing of buffered should be propagated, in order to cause the operation to be recognized
	 * as failed, because the last data items are not processed properly.
	 * 该方法需要flush所有还在缓存中的数据。
	 * 缓存flush过程中的异常需要被传播，以便操作能感知到失败，因为最新的数据项没有被合适处理。
	 *
	 * @throws java.lang.Exception An exception in this method causes the operator to fail.
	 */
	void close() throws Exception;

	/**
	 * This method is called at the very end of the operator's life, both in the case of a successful
	 * completion of the operation, and in the case of a failure and canceling.
	 * 这个方法在操作符生命周期的最后时刻调用，操作的成功完成的场景，以及失败和取消的场景。
	 *
	 * <p>This method is expected to make a thorough effort to release all resources
	 * that the operator has acquired.
	 * 这个方法期望对操作符已经申请的所有资源做一个彻底的释放。
	 */
	void dispose() throws Exception;

	// ------------------------------------------------------------------------
	//  state snapshots
	//  状态快照
	// ------------------------------------------------------------------------

	/**
	 * Called to draw a state snapshot from the operator.
	 *
	 * @return a runnable future to the state handle that points to the snapshotted state. For synchronous implementations,
	 * the runnable might already be finished.
	 *
	 * @throws Exception exception that happened during snapshotting.
	 */
	OperatorSnapshotResult snapshotState(
		long checkpointId,
		long timestamp,
		CheckpointOptions checkpointOptions) throws Exception;

	/**
	 * Provides state handles to restore the operator state.
	 *
	 * @param stateHandles state handles to the operator state.
	 */
	void initializeState(OperatorSubtaskState stateHandles) throws Exception;

	/**
	 * Called when the checkpoint with the given ID is completed and acknowledged on the JobManager.
	 *
	 * @param checkpointId The ID of the checkpoint that has been completed.
	 *
	 * @throws Exception Exceptions during checkpoint acknowledgement may be forwarded and will cause
	 *                   the program to fail and enter recovery.
	 */
	void notifyOfCompletedCheckpoint(long checkpointId) throws Exception;

	// ------------------------------------------------------------------------
	//  miscellaneous
	// ------------------------------------------------------------------------

	void setKeyContextElement1(StreamRecord<?> record) throws Exception;

	void setKeyContextElement2(StreamRecord<?> record) throws Exception;

	ChainingStrategy getChainingStrategy();

	void setChainingStrategy(ChainingStrategy strategy);

	MetricGroup getMetricGroup();

	OperatorID getOperatorID();
}
