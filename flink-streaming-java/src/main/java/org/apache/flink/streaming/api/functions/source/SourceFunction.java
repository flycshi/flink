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

package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.TimestampAssigner;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.Serializable;

/**
 * Base interface for all stream data sources in Flink. The contract of a stream source
 * is the following: When the source should start emitting elements, the {@link #run} method
 * is called with a {@link SourceContext} that can be used for emitting elements.
 * The run method can run for as long as necessary. The source must, however, react to an
 * invocation of {@link #cancel()} by breaking out of its main loop.
 * flink中所有流数据源的基础接口。
 * 一个流源头的约束:当源头开始发送元素,run方法被调用来进行发送元素。
 * run方法可以一致运行。
 * 源头必须通过cancel方法的调用来结束循环。
 *
 * <h3>Checkpointed Sources</h3>
 *
 * <p>Sources that also implement the {@link org.apache.flink.streaming.api.checkpoint.Checkpointed}
 * interface must ensure that state checkpointing, updating of internal state and emission of
 * elements are not done concurrently. This is achieved by using the provided checkpointing lock
 * object to protect update of state and emission of elements in a synchronized block.
 * 实现了 Checkpointed 接口的源,必须保证 状态检查、内部状态的更新、元素发射 不能并发执行。这可以通过使用 检查锁对象 来保证 状态更新 和 元素发射 在一个同步锁中。
 *
 * <p>This is the basic pattern one should follow when implementing a (checkpointed) source:
 * 实现具有检查功能数据源的基本模式。
 *
 * <pre>{@code
 *  public class ExampleSource<T> implements SourceFunction<T>, Checkpointed<Long> {
 *      private long count = 0L;
 *      private volatile boolean isRunning = true;
 *
 *      public void run(SourceContext<T> ctx) {
 *          while (isRunning && count < 1000) {
 *              synchronized (ctx.getCheckpointLock()) {
 *                  ctx.collect(count);
 *                  count++;
 *              }
 *          }
 *      }
 *
 *      public void cancel() {
 *          isRunning = false;
 *      }
 *
 *      public Long snapshotState(long checkpointId, long checkpointTimestamp) { return count; }
 *
 *      public void restoreState(Long state) { this.count = state; }
 * }
 * }</pre>
 *
 *
 * <h3>Timestamps and watermarks:</h3>
 * 时间戳和水位:
 * Sources may assign timestamps to elements and may manually emit watermarks.
 * 数据源可能会给元素指定时间戳,也可能会手动发送水位。
 * However, these are only interpreted if the streaming program runs on
 * {@link TimeCharacteristic#EventTime}. On other time characteristics
 * ({@link TimeCharacteristic#IngestionTime} and {@link TimeCharacteristic#ProcessingTime}),
 * the watermarks from the source function are ignored.
 * 只有在 EventTime 下才会处理这个,其他模式则直接忽略。
 *
 * <h3>Gracefully Stopping Functions</h3>
 * Functions may additionally implement the {@link org.apache.flink.api.common.functions.StoppableFunction}
 * interface. "Stopping" a function, in contrast to "canceling" means a graceful exit that leaves the
 * state and the emitted elements in a consistent state.
 * 函数可能会额外实现 StoppableFunction 接口。
 * 停止一个函数,相比较与取消, 意味着优雅的退出,状态和发射的元素是一致的。
 *
 * <p>When a source is stopped, the executing thread is not interrupted, but expected to leave the
 * {@link #run(SourceContext)} method in reasonable time on its own, preserving the atomicity
 * of state updates and element emission.
 * 当一个源被停掉的时候,执行现场不会被中断,而是希望在一个合理的时间内,结束run方法,保证状态更新和元素发射的原子性。
 *
 * @param <T> The type of the elements produced by this source.
 *
 * @see org.apache.flink.api.common.functions.StoppableFunction
 * @see org.apache.flink.streaming.api.TimeCharacteristic
 */
@Public
public interface SourceFunction<T> extends Function, Serializable {

	/**
	 * Starts the source. Implementations can use the {@link SourceContext} emit
	 * elements.
	 * 开始数据源。实现者可以使用 SourceContext 发送元素。
	 *
	 * <p>Sources that implement {@link org.apache.flink.streaming.api.checkpoint.Checkpointed}
	 * must lock on the checkpoint lock (using a synchronized block) before updating internal
	 * state and emitting elements, to make both an atomic operation:
	 * 实现了Checkpointed的源必须在更新内部状态和发射元素前上锁,让两个都是原子操作。
	 *
	 * <pre>{@code
	 *  public class ExampleSource<T> implements SourceFunction<T>, Checkpointed<Long> {
	 *      private long count = 0L;
	 *      private volatile boolean isRunning = true;
	 *
	 *      public void run(SourceContext<T> ctx) {
	 *          while (isRunning && count < 1000) {
	 *              synchronized (ctx.getCheckpointLock()) {
	 *                  ctx.collect(count);
	 *                  count++;
	 *              }
	 *          }
	 *      }
	 *
	 *      public void cancel() {
	 *          isRunning = false;
	 *      }
	 *
	 *      public Long snapshotState(long checkpointId, long checkpointTimestamp) { return count; }
	 *
	 *      public void restoreState(Long state) { this.count = state; }
	 * }
	 * }</pre>
	 *
	 * @param ctx The context to emit elements to and for accessing locks.
	 */
	void run(SourceContext<T> ctx) throws Exception;

	/**
	 * Cancels the source. Most sources will have a while loop inside the
	 * {@link #run(SourceContext)} method. The implementation needs to ensure that the
	 * source will break out of that loop after this method is called.
	 * 取消源。
	 * 大多数的源在run方法内部都有一个循环。
	 * 具体的实现需要确保在该方法被调用后,循环能够终止。
	 *
	 * <p>A typical pattern is to have an {@code "volatile boolean isRunning"} flag that is set to
	 * {@code false} in this method. That flag is checked in the loop condition.
	 * 一个典型的模式就是在配置一个volatile的boolean值
	 * 在循环的条件判断中加入这个fla的判断。
	 *
	 * <p>When a source is canceled, the executing thread will also be interrupted
	 * (via {@link Thread#interrupt()}). The interruption happens strictly after this
	 * method has been called, so any interruption handler can rely on the fact that
	 * this method has completed. It is good practice to make any flags altered by
	 * this method "volatile", in order to guarantee the visibility of the effects of
	 * this method to any interruption handler.
	 * 当源被取消的时候,执行线程将被中断。
	 * 该方法调用后,中断必须发生,所以任何中断句柄可以基于这个方法被完成的事实。
	 * 比较好的实践是通过volatile进行flag修改,以便确保这个方法的效果可以被任何中断句柄可见。
	 */
	void cancel();

	// ------------------------------------------------------------------------
	//  source context
	// ------------------------------------------------------------------------

	/**
	 * Interface that source functions use to emit elements, and possibly watermarks.
	 * 源函数用来发送元素,以及可能的watermarks的接口
	 *
	 * @param <T> The type of the elements produced by the source.
	 */
	@Public // Interface might be extended in the future with additional methods.
	interface SourceContext<T> {

		/**
		 * Emits one element from the source, without attaching a timestamp. In most cases,
		 * this is the default way of emitting elements.
		 * 从数据源发射一个元素,不带时间戳。
		 * 大多数情况下,这是默认的发射元素的方式。
		 *
		 * <p>The timestamp that the element will get assigned depends on the time characteristic of
		 * the streaming program:
		 * <ul>
		 *     <li>On {@link TimeCharacteristic#ProcessingTime}, the element has no timestamp.</li>
		 *     <li>On {@link TimeCharacteristic#IngestionTime}, the element gets the system's
		 *         current time as the timestamp.</li>
		 *     <li>On {@link TimeCharacteristic#EventTime}, the element will have no timestamp initially.
		 *         It needs to get a timestamp (via a {@link TimestampAssigner}) before any time-dependent
		 *         operation (like time windows).</li>
		 * </ul>
		 *
		 * @param element The element to emit
		 */
		void collect(T element);

		/**
		 * Emits one element from the source, and attaches the given timestamp. This method
		 * is relevant for programs using {@link TimeCharacteristic#EventTime}, where the
		 * sources assign timestamps themselves, rather than relying on a {@link TimestampAssigner}
		 * on the stream.
		 * 从数据源发射一个元素,并且附带一个给定的时间戳。
		 * 这个方法对于采用EventTime编程是有意义的,数据源自己指定时间戳,而不是基于数据流中的TimestampAssigner来指定。
		 *
		 * <p>On certain time characteristics, this timestamp may be ignored or overwritten.
		 * This allows programs to switch between the different time characteristics and behaviors
		 * without changing the code of the source functions.
		 * <ul>
		 *     <li>On {@link TimeCharacteristic#ProcessingTime}, the timestamp will be ignored,
		 *         because processing time never works with element timestamps.</li>
		 *     <li>On {@link TimeCharacteristic#IngestionTime}, the timestamp is overwritten with the
		 *         system's current time, to realize proper ingestion time semantics.</li>
		 *     <li>On {@link TimeCharacteristic#EventTime}, the timestamp will be used.</li>
		 * </ul>
		 *
		 * @param element The element to emit
		 * @param timestamp The timestamp in milliseconds since the Epoch
		 */
		@PublicEvolving
		void collectWithTimestamp(T element, long timestamp);

		/**
		 * Emits the given {@link Watermark}. A Watermark of value {@code t} declares that no
		 * elements with a timestamp {@code t' <= t} will occur any more. If further such
		 * elements will be emitted, those elements are considered <i>late</i>.
		 *
		 * <p>This method is only relevant when running on {@link TimeCharacteristic#EventTime}.
		 * On {@link TimeCharacteristic#ProcessingTime},Watermarks will be ignored. On
		 * {@link TimeCharacteristic#IngestionTime}, the Watermarks will be replaced by the
		 * automatic ingestion time watermarks.
		 *
		 * @param mark The Watermark to emit
		 */
		@PublicEvolving
		void emitWatermark(Watermark mark);

		/**
		 * Marks the source to be temporarily idle. This tells the system that this source will
		 * temporarily stop emitting records and watermarks for an indefinite amount of time. This
		 * is only relevant when running on {@link TimeCharacteristic#IngestionTime} and
		 * {@link TimeCharacteristic#EventTime}, allowing downstream tasks to advance their
		 * watermarks without the need to wait for watermarks from this source while it is idle.
		 *
		 * <p>Source functions should make a best effort to call this method as soon as they
		 * acknowledge themselves to be idle. The system will consider the source to resume activity
		 * again once {@link SourceContext#collect(T)}, {@link SourceContext#collectWithTimestamp(T, long)},
		 * or {@link SourceContext#emitWatermark(Watermark)} is called to emit elements or watermarks from the source.
		 */
		@PublicEvolving
		void markAsTemporarilyIdle();

		/**
		 * Returns the checkpoint lock. Please refer to the class-level comment in
		 * {@link SourceFunction} for details about how to write a consistent checkpointed
		 * source.
		 *
		 * @return The object to use as the lock
		 */
		Object getCheckpointLock();

		/**
		 * This method is called by the system to shut down the context.
		 */
		void close();
	}
}
