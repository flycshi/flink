/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.runtime.checkpoint.CheckpointOptions;

import java.util.Collection;
import java.util.concurrent.RunnableFuture;

/**
 * Interface for operators that can perform snapshots of their state.
 * 操作符可以对其状态执行快照的接口
 *
 * @param <S> Generic type of the state object that is created as handle to snapshots.
 *            快照的句柄，状态对象的一般类型
 */
public interface Snapshotable<S extends StateObject> {

	/**
	 * Operation that writes a snapshot into a stream that is provided by the given {@link CheckpointStreamFactory} and
	 * returns a {@link RunnableFuture} that gives a state handle to the snapshot. It is up to the implementation if
	 * the operation is performed synchronous or asynchronous. In the later case, the returned Runnable must be executed
	 * first before obtaining the handle.
	 * 将快照写入到由给定的{@code CheckpointStreamFactory}提供的流中，并返回一个{@code RunnableFuture}对象，其包含到快照的状态句柄。
	 * 由实现者来决定是同步还是异步操作。
	 * 在后一种情况下(也就是异步的情况下)，返回的Runnable必须首先执行，然后才能获得该句柄。
	 *
	 * @param checkpointId  The ID of the checkpoint.
	 * @param timestamp     The timestamp of the checkpoint.
	 * @param streamFactory The factory that we can use for writing our state to streams.
	 * @param checkpointOptions Options for how to perform this checkpoint.
	 * @return A runnable future that will yield a {@link StateObject}.
	 */
	RunnableFuture<S> snapshot(
			long checkpointId,
			long timestamp,
			CheckpointStreamFactory streamFactory,
			CheckpointOptions checkpointOptions) throws Exception;

	/**
	 * Restores state that was previously snapshotted from the provided parameters. Typically the parameters are state
	 * handles from which the old state is read.
	 * 恢复之前的快照数据。
	 * 一般入参就是需要读取旧状态的句柄
	 *
	 * @param state the old state to restore.
	 */
	void restore(Collection<S> state) throws Exception;
}
