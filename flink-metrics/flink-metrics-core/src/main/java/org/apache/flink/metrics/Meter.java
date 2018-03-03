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
 * Metric for measuring throughput.
 * 测量吞吐量的度量
 */
public interface Meter extends Metric {

	/**
	 * Mark occurrence of an event.
	 * 标记一个事件的发生
	 */
	void markEvent();

	/**
	 * Mark occurrence of multiple events.
	 * 标记多个事件的发生
	 *
	 * @param n number of events occurred	发生的事件的数量
	 */
	void markEvent(long n);

	/**
	 * Returns the current rate of events per second.
	 * 返回当前每秒事件的速率
	 *
	 * @return current rate of events per second
	 */
	double getRate();

	/**
	 * Get number of events marked on the meter.
	 * 获取仪表上标记的事件数量
	 *
	 * @return number of events marked on the meter
	 */
	long getCount();
}
