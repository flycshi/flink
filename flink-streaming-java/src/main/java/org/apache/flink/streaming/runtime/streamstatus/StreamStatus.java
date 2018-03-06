/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.streamstatus;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.tasks.SourceStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

/**
 * A Stream Status element informs stream tasks whether or not they should continue to expect records and watermarks
 * 一个{@code StreamStatus}元素通知流任务是否还能从上游获取record或者watermarks。
 * from the input stream that sent them. There are 2 kinds of status, namely {@link StreamStatus#IDLE} and
 * 有两类,IDLE、ACTIVE
 * {@link StreamStatus#ACTIVE}. Stream Status elements are generated at the sources, and may be propagated through
 * 流状态元素在数据源处产生,可能会在topology的任务间传输。
 * the tasks of the topology.
 * They directly infer the current status of the emitting task; a {@link SourceStreamTask} or
 * {@link StreamTask} emits a {@link StreamStatus#IDLE} if it will temporarily halt to emit any records or watermarks
 * (i.e. is idle), and emits a {@link StreamStatus#ACTIVE} once it resumes to do so (i.e. is active). Tasks are
 * responsible for propagating their status further downstream once they toggle between being idle and active. The cases
 * that source tasks and downstream tasks are considered either idle or active is explained below:
 * 它们直接推断出发射任务的当前状态；
 * {@link SourceStreamTask} or {@link StreamTask}，
 * 如果临时停止发送任何{@link org.apache.flink.streaming.runtime.streamrecord.StreamRecord}或者{@code Watermarks}，则会发送一个{@link StreamStatus#IDLE}
 * 如果重新发送数据，则返回发射一个@link StreamStatus#ACTIVE}
 * tasks一旦在idle和active间变化时，有责任去将它们的状态传送到下游。
 * 源任务和下游任务被认为是空闲或活动的情况如下:
 *
 * <ul>
 *     <li>Source tasks: A source task is considered to be idle if its head operator, i.e. a {@link StreamSource}, will
 *         not emit records for an indefinite amount of time. This is the case, for example, for Flink's Kafka Consumer,
 *         where sources might initially have no assigned partitions to read from, or no records can be read from the
 *         assigned partitions. Once the head {@link StreamSource} operator detects that it will resume emitting data,
 *         the source task is considered to be active. {@link StreamSource}s are responsible for toggling the status
 *         of the containing source task and ensuring that no records (and possibly watermarks, in the case of Flink's
 *         Kafka Consumer which can generate watermarks directly within the source) will be emitted while the task is
 *         idle. This guarantee should be enforced on sources through
 *         {@link org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext} implementations.</li>
 *         数据源任务: 一个数据源任务,如果在一段不确定的时间内,不会发送record,就任务其处于IDLE状态了。
 *         比如: kafka消费者,如果没有可分配的分区可以读取, 或者从分配的分区中没有record可以读取。
 *         一旦操作符监测到要重新开始发送数据了,就会被重新设置为active。
 *         {@link StreamSource}负责对包含源任务的状态进行切换，并确保在任务空闲时不会发出任何记录(可能在Flink的Kafka客户端可以直接生成水印的情况下)。
 *
 *     <li>Downstream tasks: a downstream task is considered to be idle if all its input streams are idle, i.e. the last
 *         received Stream Status element from all input streams is a {@link StreamStatus#IDLE}. As long as one of its
 *         input streams is active, i.e. the last received Stream Status element from the input stream is
 *         {@link StreamStatus#ACTIVE}, the task is active.</li>
 *         下游任务：如果其所有的输入流都是idle的，则其被任务是idle的，也就是从所有输入流收到的最后的状态元素是idle。
 *         只要有一个输入流active，也就是从输入流接收到底 状态元素是active，则任务就是active
 * </ul>
 *
 * <p>Stream Status elements received at downstream tasks also affect and control how their operators process and advance
 * their watermarks. The below describes the effects (the logic is implemented as a {@link StatusWatermarkValve} which
 * downstream tasks should use for such purposes):
 * 下游任务接收的流状态元素也会影响和控制它们的操作程序如何处理和推进它们的watermarks。
 * 以下描述了影响(逻辑被实现为{@link StatusWatermarkValve}，下游任务用于以下目的)
 *
 * <ul>
 *     <li>Since source tasks guarantee that no records will be emitted between a {@link StreamStatus#IDLE} and
 *         {@link StreamStatus#ACTIVE}, downstream tasks can always safely process and propagate records through their
 *         operator chain when they receive them, without the need to check whether or not the task is currently idle or
 *         active. However, for watermarks, since there may be watermark generators that might produce watermarks
 *         anywhere in the middle of topologies regardless of whether there are input data at the operator, the current
 *         status of the task must be checked before forwarding watermarks emitted from
 *         an operator. If the status is actually idle, the watermark must be blocked.
 *         由于源任务保障在idle和active之间，不会发射任何record，下游任务可以总是安全的处理，并在操作链中传输它们收到的records，而不需要检查task当前是idle，还是active。
 *         无论如何，对于watermarks，由于可能存在watermarks产生器，其可能在拓扑的任何位置产生watermarks，而不会去考虑操作符中是否有输入数据，
 *         所有在将一个操作符发射出的watermarks下传前，必须要检查task的状态。
 *         如果状态是idle，watermark必须被阻塞住。
 *
 *     <li>For downstream tasks with multiple input streams, the watermarks of input streams that are temporarily idle,
 *         or has resumed to be active but its watermark is behind the overall min watermark of the operator, should not
 *         be accounted for when deciding whether or not to advance the watermark and propagated through the operator
 *         chain.
 *         对于有多个输入源的下游任务，输入源的watermarks暂时idle，或者重新开始active，但是其watermark落后于操作符的最小watermark，
 *         当决定是否要将watermark在操作链中传输时，不应该考虑下传。
 * </ul>
 *
 * <p>Note that to notify downstream tasks that a source task is permanently closed and will no longer send any more
 * elements, the source should still send a {@link Watermark#MAX_WATERMARK} instead of {@link StreamStatus#IDLE}.
 * Stream Status elements only serve as markers for temporary status.
 * 需要注意的是，通知下游任务，一个源任务将永久关闭，并将不再发送任何元素时，源应该仍然发送一个{@link Watermark#MAX_WATERMARK}，而不是{@link StreamStatus#IDLE}。
 * {@link StreamStatus}元素只是用来标记临时状态
 */
@Internal
public final class StreamStatus extends StreamElement {

	public static final int IDLE_STATUS = -1;
	public static final int ACTIVE_STATUS = 0;

	public static final StreamStatus IDLE = new StreamStatus(IDLE_STATUS);
	public static final StreamStatus ACTIVE = new StreamStatus(ACTIVE_STATUS);

	public final int status;

	public StreamStatus(int status) {
		if (status != IDLE_STATUS && status != ACTIVE_STATUS) {
			throw new IllegalArgumentException("Invalid status value for StreamStatus; " +
				"allowed values are " + ACTIVE_STATUS + " (for ACTIVE) and " + IDLE_STATUS + " (for IDLE).");
		}

		this.status = status;
	}

	public boolean isIdle() {
		return this.status == IDLE_STATUS;
	}

	public boolean isActive() {
		return !isIdle();
	}

	public int getStatus() {
		return status;
	}

	@Override
	public boolean equals(Object o) {
		return this == o ||
			o != null && o.getClass() == StreamStatus.class && ((StreamStatus) o).status == this.status;
	}

	@Override
	public int hashCode() {
		return status;
	}

	@Override
	public String toString() {
		String statusStr = (status == ACTIVE_STATUS) ? "ACTIVE" : "IDLE";
		return "StreamStatus(" + statusStr + ")";
	}
}
