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

package org.apache.flink.api.common.state;

import org.apache.flink.annotation.PublicEvolving;

import java.io.IOException;

/**
 * Base interface for partitioned state that supports adding elements and inspecting the current
 * state. Elements can either be kept in a buffer (list-like) or aggregated into one value.
 * 支持添加元素和检查当前状态的分区状态的基接口。
 * 元素可以保存在缓冲区中(类似列表)，也可以聚合为一个值。
 *
 * <p>The state is accessed and modified by user functions, and checkpointed consistently
 * by the system as part of the distributed snapshots.
 * 状态由用户函数访问和修改，系统作为分布式快照的一部分始终对其进行检查。
 * 
 * <p>The state is only accessible by functions applied on a {@code KeyedStream}. The key is
 * automatically supplied by the system, so the function always sees the value mapped to the
 * key of the current element. That way, the system can handle stream and state partitioning
 * consistently together.
 * 状态只能通过在{@code KeyedStream}上应用的函数访问。
 * 该键是由系统自动提供的，因此函数总是看到映射到当前元素的键的值。
 * 这样，系统就可以一致地同时处理流和状态分区。
 * 
 * @param <IN> Type of the value that can be added to the state.
 * @param <OUT> Type of the value that can be retrieved from the state.
 */
@PublicEvolving
public interface AppendingState<IN, OUT> extends State {

	/**
	 * Returns the current value for the state. When the state is not
	 * partitioned the returned value is the same for all inputs in a given
	 * operator instance. If state partitioning is applied, the value returned
	 * depends on the current operator input, as the operator maintains an
	 * independent state for each partition.
	 * 返回状态的当前值。
	 * 当状态没有分区时，返回值对于给定操作符实例中的所有输入都是相同的。
	 * 如果应用状态分区，则返回的值取决于当前操作符输入，因为操作符为每个分区维护独立的状态。
	 *
	 * <p>
	 *     <b>NOTE TO IMPLEMENTERS:</b> if the state is empty, then this method
	 *     should return {@code null}.
	 * </p>
	 *
	 * @return The operator state value corresponding to the current input or {@code null}
	 * if the state is empty.
	 * 
	 * @throws Exception Thrown if the system cannot access the state.
	 */
	OUT get() throws Exception ;

	/**
	 * Updates the operator state accessible by {@link #get()} by adding the given value
	 * to the list of values. The next time {@link #get()} is called (for the same state
	 * partition) the returned state will represent the updated list.
	 * 通过将给定值添加到值列表中，更新{@link #get()}可以访问的操作符状态。
	 * 下一次调用{@link #get()}(对于相同的状态分区)时，返回的状态将表示更新后的列表。
	 * 
	 * @param value
	 *            The new value for the state.
	 *            
	 * @throws IOException Thrown if the system cannot access the state.
	 */
	void add(IN value) throws Exception;
	
}
