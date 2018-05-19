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

package org.apache.flink.metrics;

/**
 * A MeterView provides an average rate of events per second over a given time period.
 * 在给定的时间段内，MeterView 提供每秒事件的平均速率
 *
 * <p>The primary advantage of this class is that the rate is neither updated by the computing thread nor for every event.
 * Instead, a history of counts is maintained that is updated in regular intervals by a background thread. From this
 * history a rate is derived on demand, which represents the average rate of events over the given time span.
 *
 * <p>这个类的主要优点是，该速率既不是由计算线程更新的，也不是每个事件都更新的。
 * 相反，通过后台线程定期更新，维护了一份记录的历史数据。
 * 从这段历史中，一个比率是根据需求得出的，它代表了给定时间跨度内事件的平均速率。
 *
 * <p>Setting the time span to a low value reduces memory-consumption and will more accurately report short-term changes.
 * The minimum value possible is {@link View#UPDATE_INTERVAL_SECONDS}.
 * A high value in turn increases memory-consumption, since a longer history has to be maintained, but will result in
 * smoother transitions between rates.
 *
 * <p>将时间跨度设置为较小的值可以减少内存消耗，并更准确地报告短期更改。
 * 最小值可能是{@link View#UPDATE_INTERVAL_SECONDS}。
 * 一个较大的值反过来会增加内存消耗，因为要保持较长的历史，但是会使得速率的变化更平滑。
 *
 * <p>The events are counted by a {@link Counter}.
 *    事件使用{@link Counter}来计算
 */
public class MeterView implements Meter, View {
	/**
	 * The underlying counter maintaining the count.
	 * 底层使用的计算器
	 */
	private final Counter counter;
	/**
	 * The time-span over which the average is calculated.
	 * 计算平均值的事件跨度
	 */
	private final int timeSpanInSeconds;
	/**
	 * Circular array containing the history of values.
	 * 包含历史数据的循环数组
	 */
	private final long[] values;
	/**
	 * The index in the array for the current time.
	 * 当前时间在数组中的索引
	 */
	private int time = 0;
	/**
	 * The last rate we computed.
	 * 最新计算的rate
	 */
	private double currentRate = 0;

	public MeterView(int timeSpanInSeconds) {
		this(new SimpleCounter(), timeSpanInSeconds);
	}

	public MeterView(Counter counter, int timeSpanInSeconds) {
		this.counter = counter;
		/** 这里的操作是为了让时间跨度刚好是{@link UPDATE_INTERVAL_SECONDS}的整数倍 */
		this.timeSpanInSeconds = timeSpanInSeconds - (timeSpanInSeconds % UPDATE_INTERVAL_SECONDS);
		this.values = new long[this.timeSpanInSeconds / UPDATE_INTERVAL_SECONDS + 1];
	}

	@Override
	public void markEvent() {
		this.counter.inc();
	}

	@Override
	public void markEvent(long n) {
		this.counter.inc(n);
	}

	@Override
	public long getCount() {
		return counter.getCount();
	}

	@Override
	public double getRate() {
		return currentRate;
	}

	@Override
	public void update() {
		time = (time + 1) % values.length;
		values[time] = counter.getCount();
		/**
		 * values 是一个循环数组，每隔{@link UPDATE_INTERVAL_SECONDS}就会在这个数组的下一个索引位置更新当前的总的统计值
		 * 由于是循环数组，所以 i 和 i+1，两个位置处的统计值之间的事件间隔是{@link timeSpanInSeconds}
		 * 比如对于索引为2处的值是在时间1处产生的，那经过{@link timeSpanInSeconds}后，循环数组的最新索引就编程了1，
		 * 索引拿 索引1处的值 - 索引2处的值，就等于{@link timeSpanInSeconds}这段事件的新增变化量
		 */
		currentRate =  ((double) (values[time] - values[(time + 1) % values.length]) / timeSpanInSeconds);
	}
}
