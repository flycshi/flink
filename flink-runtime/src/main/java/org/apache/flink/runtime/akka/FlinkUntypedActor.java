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

package org.apache.flink.runtime.akka;

import org.apache.flink.runtime.messages.JobManagerMessages.LeaderSessionMessage;
import org.apache.flink.runtime.messages.RequiresLeaderSessionID;

import akka.actor.UntypedActor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * Base class for Flink's actors implemented with Java. Actors inheriting from this class
 * automatically log received messages when the debug log level is activated. Furthermore,
 * they filter out {@link LeaderSessionMessage} with the wrong leader session ID. If a message
 * of type {@link RequiresLeaderSessionID} without being wrapped in a LeaderSessionMessage is
 * detected, then an Exception is thrown.
 * flink的用java实现的actor的基类。
 * 继承自这个类的actor, 在debug日志等级激活时, 会自动打印接收到的消息。
 * 另外, 他们会过滤出携带错误leader session id的LeaderSessionMessage。
 * 如果一个 RequiresLeaderSessionID 类型的消息, 没有被包裹在一个 LeaderSessionMessage 中的消息被检测到, 会抛出一个异常。
 *
 * <p>In order to implement the actor behavior, an implementing subclass has to override the method
 * handleMessage, which defines how messages are processed. Furthermore, the subclass has to provide
 * a leader session ID option which is returned by getLeaderSessionID.
 * 为了实现actor行为, 一个实现子类必须覆写 handleMessage 方法, 该方法定义了消息是如何被处理的。
 * 另外, 子类必须提供一个leader session id选项, 可以通过 getLeaderSessionID 方法获取。
 */
public abstract class FlinkUntypedActor extends UntypedActor {

	//CHECKSTYLE.OFF: MemberNameCheck - re-enable after JobManager/TaskManager refactoring in FLIP-6?
	protected final Logger LOG = LoggerFactory.getLogger(getClass());
	//CHECKSTYLE.ON: MemberNameCheck

	/**
	 * This method is called by Akka if a new message has arrived for the actor. It logs the
	 * processing time of the incoming message if the logging level is set to debug. After logging
	 * the handleLeaderSessionID method is called.
	 * 如果一个新的消息到达该actor, 该方法会被akka调用。
	 * 如果log等级被设置为debug, 它会打印消息的处理时间。
	 * 在打印消息后, 会调用 handleLeaderSessionID 方法
	 *
	 * <p>Important: This method cannot be overriden. The actor specific message handling logic is
	 * implemented by the method handleMessage.
	 * 重要注意: 该方法不能被覆写。actor的消息处理逻辑通过 handleMessage 方法实现。
	 *
	 * @param message Incoming message
	 * @throws Exception
	 */
	@Override
	public final void onReceive(Object message) throws Exception {
		if (LOG.isTraceEnabled()) {
			LOG.trace("Received message {} at {} from {}.", message, getSelf().path(), getSender());

			long start = System.nanoTime();

			handleLeaderSessionID(message);

			long duration = (System.nanoTime() - start) / 1_000_000;

			LOG.trace("Handled message {} in {} ms from {}.", message, duration, getSender());
		} else {
			handleLeaderSessionID(message);
		}
	}

	/**
	 * This method filters out {@link LeaderSessionMessage} whose leader session ID is not equal
	 * to the actors leader session ID. If a message of type {@link RequiresLeaderSessionID}
	 * arrives, then an Exception is thrown, because these messages have to be wrapped in a
	 * {@link LeaderSessionMessage}.
	 * 该方法过滤出那些 leader session id 与 actor的 leader session id 不相等的 LeaderSessionMessage。
	 * 如果一个 RequiresLeaderSessionID 类型的消息达到, 那就抛出一个异常, 因为这些消息需要被包裹在一个 LeaderSessionMessage 中。
	 *
	 * @param message Incoming message
	 * @throws Exception
	 */
	private void handleLeaderSessionID(Object message) throws Exception {
		if (message instanceof LeaderSessionMessage) {
			LeaderSessionMessage msg = (LeaderSessionMessage) message;
			UUID expectedID = getLeaderSessionID();
			UUID actualID = msg.leaderSessionID();

			if (expectedID != null) {
				if (expectedID.equals(actualID)) {
					handleMessage(msg.message());
				} else {
					handleDiscardedMessage(expectedID, msg);
				}
			} else {
				handleNoLeaderId(msg);
			}
		} else if (message instanceof RequiresLeaderSessionID) {
			throw new Exception("Received a message " + message + " without a leader session " +
					"ID, even though the message requires a leader session ID.");
		} else {
			// call method to handle message
			handleMessage(message);
		}
	}

	private void handleDiscardedMessage(UUID expectedLeaderSessionID, LeaderSessionMessage msg) {
		LOG.warn("Discard message {} because the expected leader session ID {} did not " +
				"equal the received leader session ID {}.", msg, expectedLeaderSessionID,
				msg.leaderSessionID());
	}

	private void handleNoLeaderId(LeaderSessionMessage msg) {
		LOG.warn("Discard message {} because there is currently no valid leader id known.", msg);
	}

	/**
	 * This method contains the actor logic which defines how to react to incoming messages.
	 * 该方法包含了actor的如何处理消息的逻辑。
	 *
	 * @param message Incoming message
	 * @throws Exception
	 */
	protected abstract void handleMessage(Object message) throws Exception;

	/**
	 * Returns the current leader session ID associcated with this actor.
	 * 返回与该actor相关联的当前leader session id
	 *
	 * @return
	 */
	protected abstract UUID getLeaderSessionID();

	/**
	 * This method should be called for every outgoing message. It wraps messages which require
	 * a leader session ID (indicated by {@link RequiresLeaderSessionID}) in a
	 * {@link LeaderSessionMessage} with the actor's leader session ID.
	 * 这个方法应该在每一个输出消息时调用。
	 * 它将需要一个leader session id的消息(用RequiresLeaderSessionID标识)包裹一层放到一个LeaderSessionMessage中,附带上leader session id。
	 *
	 * <p>This method can be overriden to implement a different decoration behavior.
	 *
	 * @param message Message to be decorated
	 * @return The deocrated message
	 */
	protected Object decorateMessage(Object message) {
		if (message instanceof RequiresLeaderSessionID) {
			return new LeaderSessionMessage(getLeaderSessionID(), message);
		} else {
			return message;
		}
	}

}
