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

package org.apache.flink.streaming.api;

import org.apache.flink.annotation.Public;

/**
 * The checkpointing mode defines what consistency guarantees the system gives in the presence of
 * failures.
 * checkpoint模式定义了在系统故障时的一致性保证策略。
 *
 * <p>When checkpointing is activated, the data streams are replayed such that lost parts of the
 * processing are repeated. For stateful operations and functions, the checkpointing mode defines
 * whether the system draws checkpoints such that a recovery behaves as if the operators/functions
 * see each record "exactly once" ({@link #EXACTLY_ONCE}), or whether the checkpoints are drawn
 * in a simpler fashion that typically encounteres some duplicates upon recovery
 * ({@link #AT_LEAST_ONCE})</p>
 * 当checkpoint被激活时,数据流会被充放,以便丢失的处理部分再重来一次。
 * 对于有状态的操作和函数,checkpoint模式定义了系统是否备份checkpoint,以便一个恢复行为就好像操作符/函数只会看到记录一次,
 * 或者checkpoint比较简单,但是在恢复时,一般会出现记录重复。
 */
@Public
public enum CheckpointingMode {

	/**
	 * Sets the checkpointing mode to "exactly once". This mode means that the system will
	 * checkpoint the operator and user function state in such a way that, upon recovery,
	 * every record will be reflected exactly once in the operator state.
	 * 检查模式设置为"当且仅当一次"。
	 * 该模式指,系统将以这样的方式来检查操作符和用户函数,在恢复时,每个记录在操作符的状态中只会出现一次。
	 *
	 * <p>For example, if a user function counts the number of elements in a stream,
	 * this number will consistently be equal to the number of actual elements in the stream,
	 * regardless of failures and recovery.</p>
	 * 举个例子,如果一个用户函数来统计流中元素的个数,不管是否存在失败和恢复操作,统计的数值应该始终是和流中真实元素个数是一致的。
	 *
	 * <p>Note that this does not mean that each record flows through the streaming data flow
	 * only once. It means that upon recovery, the state of operators/functions is restored such
	 * that the resumed data streams pick up exactly at after the last modification to the state.</p>
	 * 需要注意的事,并不说每个记录只会在数据流中流过一次。
	 * 也就是说,在恢复时,操作符/函数的状态是被存储的,这样就可以从回放的数据流中从最后一次状态修改的地方开始提取数据。
	 *
	 * <p>Note that this mode does not guarantee exactly-once behavior in the interaction with
	 * external systems (only state in Flink's operators and user functions). The reason for that
	 * is that a certain level of "collaboration" is required between two systems to achieve
	 * exactly-once guarantees. However, for certain systems, connectors can be written that facilitate
	 * this collaboration.</p>
	 * 需要注意的时,该模式不能保障在与外部系统交互中保障exactly-once(只能在flink的操作符和用户函数的状态中保障)。
	 * 原因是需要在两个系统之间合作才能达到。
	 * 但是,对于特定的系统,连接器是可以促进这种合作的。
	 *
	 * <p>This mode sustains high throughput. Depending on the data flow graph and operations,
	 * this mode may increase the record latency, because operators need to align their input
	 * streams, in order to create a consistent snapshot point. The latency increase for simple
	 * dataflows (no repartitioning) is negligible. For simple dataflows with repartitioning, the average
	 * latency remains small, but the slowest records typically have an increased latency.</p>
	 * 该模式支持高吞吐。
	 * 依赖数据流图和操作,该模式可能会增加记录延迟,因为操作者需要把他们的输入排成一行,这样才可以创建一个一致的snapshot点。
	 * 简单的数据流(不会重新分区)的延迟可以忽略。
	 * 对于重新分区的简单数据流,平均延迟仍然很小,但是最慢的记录通常延迟会增加。
	 */
	EXACTLY_ONCE,

	/**
	 * Sets the checkpointing mode to "at least once". This mode means that the system will
	 * checkpoint the operator and user function state in a simpler way. Upon failure and recovery,
	 * some records may be reflected multiple times in the operator state.
	 * 设置检查模式为"至少一次"。
	 * 该模式表示系统会以简单的方式来检查操作符和用户函数的状态。
	 * 在失败恢复时,某些记录会在操作符状态中出现多次。
	 *
	 * <p>For example, if a user function counts the number of elements in a stream,
	 * this number will equal to, or larger, than the actual number of elements in the stream,
	 * in the presence of failure and recovery.</p>
	 * 比如,在统计流中元素个数的用户函数中,在出现故障和恢复的清下,统计的个数会大于等于数据流中实际的元素个数。
	 *
	 * <p>This mode has minimal impact on latency and may be preferable in very-low latency
	 * scenarios, where a sustained very-low latency (such as few milliseconds) is needed,
	 * and where occasional duplicate messages (on recovery) do not matter.</p>
	 * 这种模式具有很小的延迟,比较适合需要低延迟场景,比如需要支撑非常小的延迟的场景(比如几十毫秒),并且在恢复中出现重复消息是可以接受的。
	 */
	AT_LEAST_ONCE
}
