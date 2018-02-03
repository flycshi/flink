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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Defines the chaining scheme for the operator. When an operator is chained to the
 * predecessor, it means that they run in the same thread. They become one operator
 * consisting of multiple steps.
 * 定义了操作符的链接模式。
 * 当一个操作符被链接到其前一个操作符时，表示他们在一个线程中运行。
 * 他们变成包含了多步操作的一个整体的操作符。
 *
 * <p>The default value used by the {@link StreamOperator} is {@link #HEAD}, which means that
 * the operator is not chained to its predecessor. Most operators override this with
 * {@link #ALWAYS}, meaning they will be chained to predecessors whenever possible.
 * 操作符的默认值时HEAD，指操作符不与其前一个操作符链接在一起。
 * 大多数操作符会覆盖成ALWAYS，指他们尽可能的链接在一起。
 */
@PublicEvolving
public enum ChainingStrategy {

	/**
	 * Operators will be eagerly chained whenever possible.
	 * 操作符会尽可能的连接
	 *
	 * <p>To optimize performance, it is generally a good practice to allow maximal
	 * chaining and increase operator parallelism.
	 * 为了优化性能，比较好的方式是运行最大的连接，以及增加操作符并行度。
	 */
	ALWAYS,

	/**
	 * The operator will not be chained to the preceding or succeeding operators.
	 * 操作符将不会与前或者后的操作符进行连接。
	 */
	NEVER,

	/**
	 * The operator will not be chained to the predecessor, but successors may chain to this
	 * operator.
	 * 操作符不会与前操作符连接，按时后面的操作符可以连接到它。
	 */
	HEAD
}
