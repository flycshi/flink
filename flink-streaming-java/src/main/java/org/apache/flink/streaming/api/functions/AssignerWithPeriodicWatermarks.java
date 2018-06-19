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

package org.apache.flink.streaming.api.functions;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * The {@code AssignerWithPeriodicWatermarks} assigns event time timestamps to elements,
 * and generates low watermarks that signal event time progress within the stream.
 * These timestamps and watermarks are used by functions and operators that operate
 * on event time, for example event time windows.
 * {@code AssignerWithPeriodicWatermarks}将event的时间戳分配给元素，并生成low watermark，以在流中显示事件时间进程。
 * 这些时间戳和水印被在事件时间上操作的函数和操作符使用，例如事件时间窗口。
 *
 * <p>Use this class to generate watermarks in a periodical interval.
 * At most every {@code i} milliseconds (configured via
 * {@link ExecutionConfig#getAutoWatermarkInterval()}), the system will call the
 * {@link #getCurrentWatermark()} method to probe for the next watermark value.
 * The system will generate a new watermark, if the probed value is non-null
 * and has a timestamp larger than that of the previous watermark (to preserve
 * the contract of ascending watermarks).
 * 使用这个类在周期间隔中生成水印。
 * 在每一个{@code i}毫秒(通过{@link ExecutionConfig#getAutoWatermarkInterval()}配置)，
 * 系统将调用{@link #getCurrentWatermark()}方法探测下一个水印值。
 * 系统将生成一个新的水印，如果被探测的值是非空的，并且具有比以前的水印更大的时间戳(以保持升序水印的契约)。
 *
 * <p>The system may call the {@link #getCurrentWatermark()} method less often than every
 * {@code i} milliseconds, if no new elements arrived since the last call to the
 * method.
 * 如果自上次调用该方法以来没有新元素到达，那么系统调用{@link #getCurrentWatermark()}方法的频率可能比每次{@code i}毫秒要低
 *
 * <p>Timestamps and watermarks are defined as {@code longs} that represent the
 * milliseconds since the Epoch (midnight, January 1, 1970 UTC).
 * A watermark with a certain value {@code t} indicates that no elements with event
 * timestamps {@code x}, where {@code x} is lower or equal to {@code t}, will occur any more.
 * 时间戳和水印被定义为{@code longs}，表示自纪元以来的毫秒数(零时，1970年1月1日)。
 * 带有特定值{@code t}的水印表明，不再出现带有事件时间戳{@code x}(其中{@code x}较低或等于{@code t})的元素。
 *
 * @param <T> The type of the elements to which this assigner assigns timestamps.
 *
 * @see org.apache.flink.streaming.api.watermark.Watermark
 */
public interface AssignerWithPeriodicWatermarks<T> extends TimestampAssigner<T> {

	/**
	 * Returns the current watermark. This method is periodically called by the
	 * system to retrieve the current watermark. The method may return {@code null} to
	 * indicate that no new Watermark is available.
	 * 返回当前水印。
	 * 系统周期性地调用此方法来检索当前的水印。
	 * 该方法可以返回{@code null}，以表明没有新的水印可用。
	 *
	 * <p>The returned watermark will be emitted only if it is non-null and its timestamp
	 * is larger than that of the previously emitted watermark (to preserve the contract of
	 * ascending watermarks). If the current watermark is still
	 * identical to the previous one, no progress in event time has happened since
	 * the previous call to this method. If a null value is returned, or the timestamp
	 * of the returned watermark is smaller than that of the last emitted one, then no
	 * new watermark will be generated.
	 * 返回的水印只有在非null且其时间戳大于先前发出的水印时才会被发出(以保持升序水印的契约)。
	 * 如果当前的水印仍然与前一个水印相同，则自上次调用该方法以来，事件时间没有发生任何进展。
	 * 如果返回空值，或返回的水印的时间戳小于上次发出的水印的时间戳，则不会生成新的水印。
	 *
	 * <p>The interval in which this method is called and Watermarks are generated
	 * depends on {@link ExecutionConfig#getAutoWatermarkInterval()}.
	 *
	 * @see org.apache.flink.streaming.api.watermark.Watermark
	 * @see ExecutionConfig#getAutoWatermarkInterval()
	 *
	 * @return {@code Null}, if no watermark should be emitted, or the next watermark to emit.
	 */
	@Nullable
	Watermark getCurrentWatermark();
}
