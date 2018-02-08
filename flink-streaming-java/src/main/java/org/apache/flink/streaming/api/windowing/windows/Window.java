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

package org.apache.flink.streaming.api.windowing.windows;

import org.apache.flink.annotation.PublicEvolving;

/**
 * A {@code Window} is a grouping of elements into finite buckets. Windows have a maximum timestamp
 * which means that, at some point, all elements that go into one window will have arrived.
 * 一个window是一组元素的有限集合。窗口有一个最大的时间戳,即在这个时间点,属于该窗口的所有元素都需要到达。
 *
 * <p>Subclasses should implement {@code equals()} and {@code hashCode()} so that logically
 * same windows are treated the same.
 * 子类需要实现equals和hashCode方法,这样逻辑一样的窗口就可以被相同对待。
 */
@PublicEvolving
public abstract class Window {

	/**
	 * Gets the largest timestamp that still belongs to this window.
	 * 获取该窗口的最大时间戳
	 *
	 * @return The largest timestamp that still belongs to this window.
	 */
	public abstract long maxTimestamp();
}
