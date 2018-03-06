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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * A {@link org.apache.flink.streaming.api.operators.StreamOperator} is supplied with an object
 * of this interface that can be used to emit elements and other messages, such as barriers
 * and watermarks, from an operator.
 * 给{@link StreamOperator}提供一个该接口的对象，用来从一个操作符中发射元素，以及其他消息，比如barriers、watermarks
 *
 * @param <T> The type of the elements that can be emitted.
 *            可以发射的元素的类型
 */
@PublicEvolving
public interface Output<T> extends Collector<T> {

	/**
	 * Emits a {@link Watermark} from an operator. This watermark is broadcast to all downstream
	 * operators.
	 * 从一个操作符发射一个{@link Watermark}。
	 * 这个{@link Watermark}被广播到所有下游操作符。
	 *
	 * <p>A watermark specifies that no element with a timestamp lower or equal to the watermark
	 * timestamp will be emitted in the future.
	 * {@link Watermark}表示不大于这个水位中的时间戳的元素在将来都不会被发射了。
	 */
	void emitWatermark(Watermark mark);

	/**
	 * Emits a record the side output identified by the given {@link OutputTag}.
	 * 将一个record发送到由{@link OutputTag}标识的输出边
	 *
	 * @param record The record to collect.
	 */
	<X> void collect(OutputTag<X> outputTag, StreamRecord<X> record);

	void emitLatencyMarker(LatencyMarker latencyMarker);
}
