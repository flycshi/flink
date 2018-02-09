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

package org.apache.flink.runtime.leaderretrieval;

/**
 * This interface has to be implemented by a service which retrieves the current leader and notifies
 * a listener about it.
 * 这个接口被用来提取当前leader,并通知listener的服务来实现。
 *
 * <p>Prior to using this service it has to be started by calling the start method. The start method
 * also takes the {@link LeaderRetrievalListener} as an argument. The service can only be started
 * once.
 * 使用这个服务前,需要先调用start方法。start方法需要传入一个 LeaderRetrievalListener 作为参数。
 * 这个服务只能被start一次
 *
 * <p>The service should be stopped by calling the stop method.
 * 这个服务需要通过调用stop方法来停止
 */
public interface LeaderRetrievalService {

	/**
	 * Starts the leader retrieval service with the given listener to listen for new leaders. This
	 * method can only be called once.
	 * 启动leader提取服务,并指定listener来监听新的leader。
	 * 这个方法只能被调用一次。
	 *
	 * @param listener The leader retrieval listener which will be notified about new leaders.
	 * @throws Exception
	 */
	void start(LeaderRetrievalListener listener) throws Exception;

	/**
	 * Stops the leader retrieval service.
	 *
	 * @throws Exception
	 */
	void stop() throws Exception;
}
