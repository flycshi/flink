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

package org.apache.flink.runtime.leaderelection;

import java.util.UUID;

/**
 * Interface for a service which allows to elect a leader among a group of contenders.
 * 在一组竞选者中选择一个leader的服务的接口
 *
 * Prior to using this service, it has to be started calling the start method. The start method
 * takes the contender as a parameter. If there are multiple contenders, then each contender has
 * to instantiate its own leader election service.
 * 在使用服务前,必须调用start方法来启动。
 * start方法的入参是 LeaderContender。
 * 如果有多个竞选者,每个竞选者需要实例化它自己的leader选举服务。
 *
 * Once a contender has been granted leadership he has to confirm the received leader session ID
 * by calling the method confirmLeaderSessionID. This will notify the leader election service, that
 * the contender has received the new leader session ID and that it can now be published for
 * leader retrieval services.
 */
public interface LeaderElectionService {

	/**
	 * Starts the leader election service. This method can only be called once.
	 *
	 * @param contender LeaderContender which applies for the leadership
	 * @throws Exception
	 */
	void start(LeaderContender contender) throws Exception;

	/**
	 * Stops the leader election service.
	 * @throws Exception
	 */
	void stop() throws Exception;

	/**
	 * Confirms that the new leader session ID has been successfully received by the new leader.
	 * This method is usually called by the newly appointed {@link LeaderContender}.
	 * 确认新leader的会话id已经被新leader成功接收到了。
	 * 这个方法一般被新的 LeaderContender 调用。
	 *
	 * The rational behind this method is to establish an order between setting the new leader
	 * session ID in the {@link LeaderContender} and publishing the new leader session ID to the
	 * leader retrieval services.
	 * 背后的逻辑是,在 LeaderContender 中设置新leader的会话id, 和 发布新leader的会话id到leader提取服务之间,建立顺序。
	 *
	 * @param leaderSessionID The new leader session ID
	 */
	void confirmLeaderSessionID(UUID leaderSessionID);

	/**
	 * Returns true if the {@link LeaderContender} with which the service has been started owns
	 * currently the leadership.
	 *
	 * @return true if the associated {@link LeaderContender} is the leader, otherwise false
	 */
	boolean hasLeadership();

}
