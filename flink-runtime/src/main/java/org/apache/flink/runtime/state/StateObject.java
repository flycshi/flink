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

package org.apache.flink.runtime.state;

import java.io.Serializable;

/**
 * Base of all handles that represent checkpointed state in some form. The object may hold
 * the (small) state directly, or contain a file path (state is in the file), or contain the
 * metadata to access the state stored in some external database.
 * 以某种形式表示检查点状态的所有句柄的基础。对象可以直接保存(数据量小的)状态, 或者包含一个文件路径(状态在文件中)。
 * 或者包含去获取存储在外部db中的状态的元数据。
 *
 * <p>State objects define how to {@link #discardState() discard state} and how to access the
 * {@link #getStateSize() size of the state}.
 * state objects 定义了如何丢弃状态, 以及如何获取状态的size
 * 
 * <p>State Objects are transported via RPC between <i>JobManager</i> and
 * <i>TaskManager</i> and must be {@link java.io.Serializable serializable} to support that.
 * State Objects 通过RPC在JobManager和TaskManager之间传输, 必须继承Serializable接口
 * 
 * <p>Some State Objects are stored in the checkpoint/savepoint metadata. For long-term
 * compatibility, they are not stored via {@link java.io.Serializable Java Serialization},
 * but through custom serializers.
 * 有些State Objects 被存储在检查点/保存点元数据中。
 * 为了长期兼容, 他们不是通过Serializable序列化存储, 而是通过用户序列化器。
 */
public interface StateObject extends Serializable {

	/**
	 * Discards the state referred to and solemnly owned by this handle, to free up resources in
	 * the persistent storage. This method is called when the state represented by this
	 * object will not be used any more.
	 * 丢弃该句柄拥有的状态, 释放持久化存储中的资源。
	 * 当该对象表示的状态不在被使用时, 该方法会被调用。
	 */
	void discardState() throws Exception;

	/**
	 * Returns the size of the state in bytes. If the size is not known, this
	 * method should return {@code 0}.
	 * 返回状态的字节大小, 如果大小不知道, 该方法返回0
	 * 
	 * <p>The values produced by this method are only used for informational purposes and
	 * for metrics/monitoring. If this method returns wrong values, the checkpoints and recovery
	 * will still behave correctly. However, efficiency may be impacted (wrong space pre-allocation)
	 * and functionality that depends on metrics (like monitoring) will be impacted.
	 * 该方法提供的数据只是提供信息,以及进行监控。
	 * 如果该方法返回了错误的值, 检查点和恢复将继续保持正常。
	 * 但是, 效率会受影响(预分配了错误的space), 以及基于度量(如监控)的函数会受影响。
	 * 
	 * <p>Note for implementors: This method should not perform any I/O operations
	 * while obtaining the state size (hence it does not declare throwing an {@code IOException}).
	 * Instead, the state size should be stored in the state object, or should be computable from
	 * the state stored in this object.
	 * The reason is that this method is called frequently by several parts of the checkpointing
	 * and issuing I/O requests from this method accumulates a heavy I/O load on the storage
	 * system at higher scale.
	 * 实现者需要注意: 该方法在获取状态大小时不应该进行任何I/O操作(因为它没有定义抛出IOException异常)。
	 * 替代的, 状态大小应该被存储在状态对象中, 或者从状态对象中存储的状态进行计算而来。
	 * 这样做的原因是, 该方法会被checkpointing的多个部分频繁调用, 如果有I/O请求, 会累加到一个比较重的I/O负载。
	 *
	 * @return Size of the state in bytes. 状态的字节大小
	 */
	long getStateSize();
}
