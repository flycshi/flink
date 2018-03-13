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

package org.apache.flink.runtime.instance;

/**
 * Classes implementing the InstanceListener interface can be notified about
 * the availability disappearance of instances.
 * 当出现有效的{@link Instance}时, 该接口的实现可以被通知到。
 */
public interface InstanceListener {

	/**
	 * Called when a new instance becomes available.
	 * 当一个新的{@code Instance}变为有效时, 被调用
	 * 
	 * @param instance The instance that became available.
	 */
	void newInstanceAvailable(Instance instance);
	
	/**
	 * Called when an instance died.
	 * 当一个{@link Instance}挂掉时, 被调用
	 * 
	 * @param instance The instance that died.
	 */
	void instanceDied(Instance instance);
}
