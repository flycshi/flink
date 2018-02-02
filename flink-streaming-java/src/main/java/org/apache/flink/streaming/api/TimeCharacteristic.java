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

package org.apache.flink.streaming.api;

import org.apache.flink.annotation.PublicEvolving;

/**
 * The time characteristic defines how the system determines time for time-dependent
 * order and operations that depend on time (such as time windows).
 * 时间特征 定义了 系统如何为基于事件的顺序和基于事件的操作(比如时间窗口)来确定时间。
 */
@PublicEvolving
public enum TimeCharacteristic {

	/**
	 * Processing time for operators means that the operator uses the system clock of the machine
	 * to determine the current time of the data stream. Processing-time windows trigger based
	 * on wall-clock time and include whatever elements happen to have arrived at the operator at
	 * that point in time.
	 * 处理时间,对于操作符来说,是指操作符使用系统机器时钟来确定数据流的当前时间。
	 * 基于挂钟的处理时间窗口触发器,以及正好在那个时间点到达的任何元素。
	 *
	 * <p>Using processing time for window operations results in general in quite non-deterministic
	 * results, because the contents of the windows depends on the speed in which elements arrive.
	 * It is, however, the cheapest method of forming windows and the method that introduces the
	 * least latency.
	 * 窗口操作使用处理时间 一般会导致相当不确定性的结果, 因为窗口的内容依赖元素到达的速度。
	 * 然而,它是形成窗口的最便宜的方法和引入最小延迟的方法。
	 */
	ProcessingTime,

	/**
	 * Ingestion time means that the time of each individual element in the stream is determined
	 * when the element enters the Flink streaming data flow. Operations like windows group the
	 * elements based on that time, meaning that processing speed within the streaming dataflow
	 * does not affect windowing, but only the speed at which sources receive elements.
	 * IngestionTime 意味着流中的每个独立元素的时间,是有元素进入flink的时间。
	 *像基于这个时间的窗口聚合操作, 意味着流的处理速度不会影响窗口,但是会受元素到达数据源的速度的影响。
	 *
	 * <p>Ingestion time is often a good compromise between processing time and event time.
	 * It does not need and special manual form of watermark generation, and events are typically
	 * not too much out-or-order when they arrive at operators; in fact, out-of-orderness can
	 * only be introduced by streaming shuffles or split/join/union operations. The fact that
	 * elements are not very much out-of-order means that the latency increase is moderate,
	 * compared to event
	 * time.
	 * 摄入时间是处理时间和事件时间之间的一个很好的折中方案。
	 * 它不需要特定形式的水位产生。当元素到达操作符时,也会不有太多的乱序;事实上,只有shuffle或者split/join/union操作
	 * 才会引入少量的乱序。
	 * 与事件时间相比,元素很少乱序意味着延迟是适度的。
	 */
	IngestionTime,

	/**
	 * Event time means that the time of each individual element in the stream (also called event)
	 * is determined by the event's individual custom timestamp. These timestamps either exist in
	 * the elements from before they entered the Flink streaming dataflow, or are user-assigned at
	 * the sources. The big implication of this is that it allows for elements to arrive in the
	 * sources and in all operators out of order, meaning that elements with earlier timestamps may
	 * arrive after elements with later timestamps.
	 * 事件时间是指流中的每个元素(也叫做事件)的时间是由事件时间确定。
	 * 这个时间戳要么在元素进入flink数据流之前就已经存在,要么是用户在数据源中指定。
	 * 这其中的重要含义是,它允许元素到达数据源和所有的操作符是乱序的,也就是说较早时间的元素有可能比较晚时间的元素到达晚。
	 *
	 * <p>Operators that window or order data with respect to event time must buffer data until they
	 * can be sure that all timestamps for a certain time interval have been received. This is
	 * handled by the so called "time watermarks".
	 * 依赖事件时间的窗口或者有序数据,必须通过缓存数据来确认针对某个时间间隔的所有时间戳都已经到达了。
	 * 这是由所谓的"时间水位"来处理的。
	 *
	 * <p>Operations based on event time are very predictable - the result of windowing operations
	 * is typically identical no matter when the window is executed and how fast the streams
	 * operate. At the same time, the buffering and tracking of event time is also costlier than
	 * operating with processing time, and typically also introduces more latency. The amount of
	 * extra cost depends mostly on how much out of order the elements arrive, i.e., how long the
	 * time span between the arrival of early and late elements is. With respect to the
	 * "time watermarks", this means that the cost typically depends on how early or late the
	 * watermarks can be generated for their timestamp.
	 * 基于事件时间的操作是可预测的,无论窗口何时执行,以及流操作处理多快,窗口操作的结果通常都是相同的。
	 * 与此同时,事件时间的缓存和跟踪也比处理时间的操作更昂贵,通常也会引入更多的延迟。
	 * 额外的消耗很大程度取决于元素到达的乱序程度,即,较早元素和较晚元素到达的时间间隔有多大。
	 * 对于"时间水位"来说,这个意味着消耗取决于时间水位的产生有多早或多晚。
	 *
	 *
	 * <p>In relation to {@link #IngestionTime}, the event time is similar, but refers the the
	 * event's original time, rather than the time assigned at the data source. Practically, that
	 * means that event time has generally more meaning, but also that it takes longer to determine
	 * that all elements for a certain time have arrived.
	 * 与IngestionTime相比,EventTime是相似的,但是表示的是事件的原始时间,而IngestionTime是在数据源中指定的。
	 * 实际上,EventTime更有意义,但是对于某个时间的元素是否都达到,需要更长的时间才能确定。
	 */
	EventTime
}
