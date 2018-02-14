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

import akka.actor.ActorRef;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.io.Serializable;
import java.util.UUID;

/**
 * Interface to abstract the communication with an actor.
 * 与一个actor交互的抽象接口
 *
 * It allows to avoid direct interaction with an ActorRef.
 * 它允许避免与ActorRef的直接交互
 */
public interface ActorGateway extends Serializable {

	/**
	 * Sends a message asynchronously and returns its response. The response to the message is
	 * returned as a future.
	 * 异步发送一个消息,并返回发送结果。
	 * 消息的回复结果是一个future
	 *
	 * @param message Message to be sent
	 * @param timeout Timeout until the Future is completed with an AskTimeoutException
	 * @return Future which contains the response to the sent message
	 */
	Future<Object> ask(Object message, FiniteDuration timeout);

	/**
	 * Sends a message asynchronously without a result.
	 * 异步发送一个没有返回结果的消息
	 *
	 * @param message Message to be sent
	 */
	void tell(Object message);

	/**
	 * Sends a message asynchronously without a result with sender being the sender.
	 * 异步发送一个没有返回结果的消息, 发送时会带上发送者信息
	 *
	 * @param message Message to be sent
	 * @param sender Sender of the message
	 */
	void tell(Object message, ActorGateway sender);

	/**
	 * Forwards a message. For the receiver of this message it looks as if sender has sent the
	 * message.
	 * 对于消息的接收者, 看起来,消息发送者已经发送了这个消息。
	 *
	 * @param message Message to be sent
	 * @param sender Sender of the forwarded message
	 */
	void forward(Object message, ActorGateway sender);

	/**
	 * Retries to send asynchronously a message up to numberRetries times. The response to this
	 * message is returned as a future. The message is re-sent if the number of retries is not yet
	 * exceeded and if an exception occurred while sending it.
	 * 异步尝试多次发送一个消息。这个消息的回复是一个future。如果重试次数没到上限,或者发送时发生异常,消息会被重新发送。
	 *
	 * @param message Message to be sent
	 * @param numberRetries Number of times to retry sending the message
	 * @param timeout Timeout for each sending attempt
	 * @param executionContext ExecutionContext which is used to send the message multiple times
	 * @return Future of the response to the sent message
	 */
	Future<Object> retry(
			Object message,
			int numberRetries,
			FiniteDuration timeout,
			ExecutionContext executionContext);

	/**
	 * Returns the path of the remote instance.
	 * 返回远程实例的路径
	 *
	 * @return Path of the remote instance.
	 */
	String path();

	/**
	 * Returns the underlying actor with which is communicated
	 * 返回底层通信的 ActorRef 实例
	 *
	 * @return ActorRef of the target actor
	 */
	ActorRef actor();

	/**
	 * Returns the leaderSessionID associated with the remote actor or null.
	 * 返回远程actor的 leaderSessionID , 或者返回 null
	 *
	 * @return Leader session ID if its associated with this gateway, otherwise null
	 */
	UUID leaderSessionID();
}
