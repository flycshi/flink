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

package org.apache.flink.metrics.reporter;

import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;

/**
 * Reporters are used to export {@link Metric Metrics} to an external backend.
 * {@code MetricReporter}是用来将{@code Metric}导出到外部的
 *
 * <p>Reporters are instantiated via reflection and must be public, non-abstract, and have a
 * public no-argument constructor.
 * Reporters的实例化都是通过反射, 必须public, 非抽象, 并且有一个公共的无参构造器
 */
public interface MetricReporter {

	// ------------------------------------------------------------------------
	//  life cycle
	//  生命周期
	// ------------------------------------------------------------------------

	/**
	 * Configures this reporter. Since reporters are instantiated generically and hence parameter-less,
	 * this method is the place where the reporters set their basic fields based on configuration values.
	 * 配置这个reporter。
	 * 因为reporters都是通用无参实例化, 这个方法就是基于配置进行基础字段设置的地方。
	 *
	 * <p>This method is always called first on a newly instantiated reporter.
	 * 这个方法总是在一个新的实例化后, 首先调用的方法
	 *
	 * @param config A properties object that contains all parameters set for this reporter.
	 *               包含了这个reporter的所有参赛的属性对象
	 */
	void open(MetricConfig config);

	/**
	 * Closes this reporter. Should be used to close channels, streams and release resources.
	 * 关闭reporter。应该用来关闭通道, 流, 释放资源
	 */
	void close();

	// ------------------------------------------------------------------------
	//  adding / removing metrics
	//  添加/移除 metrics
	// ------------------------------------------------------------------------

	/**
	 * Called when a new {@link Metric} was added.
	 * 当一个新的{@code Metric}被添加时调用该方法
	 *
	 * @param metric      the metric that was added
	 * @param metricName  the name of the metric
	 * @param group       the group that contains the metric
	 */
	void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group);

	/**
	 * Called when a {@link Metric} was should be removed.
	 * 当一个{@code Metric}被移除时调用该方法
	 *
	 * @param metric      the metric that should be removed
	 * @param metricName  the name of the metric
	 * @param group       the group that contains the metric
	 */
	void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group);
}
