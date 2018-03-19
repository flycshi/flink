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
 * An interface for metrics which should be updated in regular intervals by a background thread.
 * 应该通过后台线程定期更新的metrics的一个接口
 */
public interface View {
	/**
	 * The interval in which metrics are updated.
	 * metrics更新的间隔
	 */
	int UPDATE_INTERVAL_SECONDS = 5;

	/**
	 * This method will be called regularly to update the metric.
	 * 被定期调用进行metric更新的方法
	 */
	void update();
}
