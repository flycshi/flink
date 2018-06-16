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

package org.apache.flink.runtime.state;

import org.apache.flink.util.MathUtils;
import org.apache.flink.util.Preconditions;

public final class KeyGroupRangeAssignment {

	/**
	 * The default lower bound for max parallelism if nothing was configured by the user. We have this so allow users
	 * some degree of scale-up in case they forgot to configure maximum parallelism explicitly.
	 * 如果用户没有配置任何内容，则最大并行度的默认下界。
	 * 我们有了这个功能，用户可以在忘记显式地配置最大并行性的情况下进行一定程度的扩展。
	 */
	public static final int DEFAULT_LOWER_BOUND_MAX_PARALLELISM = 1 << 7;

	/** The (inclusive) upper bound for max parallelism */
	public static final int UPPER_BOUND_MAX_PARALLELISM = 1 << 15;

	private KeyGroupRangeAssignment() {
		throw new AssertionError();
	}

	/**
	 * Assigns the given key to a parallel operator index.
	 * 将给定键分配给一个并行操作符的索引
	 *
	 * @param key the key to assign
	 * @param maxParallelism the maximum supported parallelism, aka the number of key-groups.
	 * @param parallelism the current parallelism of the operator
	 * @return the index of the parallel operator to which the given key should be routed.
	 */
	public static int assignKeyToParallelOperator(Object key, int maxParallelism, int parallelism) {
		return computeOperatorIndexForKeyGroup(maxParallelism, parallelism, assignToKeyGroup(key, maxParallelism));
	}

	/**
	 * Assigns the given key to a key-group index.
	 *
	 * @param key the key to assign
	 * @param maxParallelism the maximum supported parallelism, aka the number of key-groups.
	 * @return the key-group to which the given key is assigned
	 */
	public static int assignToKeyGroup(Object key, int maxParallelism) {
		return computeKeyGroupForKeyHash(key.hashCode(), maxParallelism);
	}

	/**
	 * Assigns the given key to a key-group index.
	 *
	 * @param keyHash the hash of the key to assign
	 * @param maxParallelism the maximum supported parallelism, aka the number of key-groups.
	 * @return the key-group to which the given key is assigned
	 */
	public static int computeKeyGroupForKeyHash(int keyHash, int maxParallelism) {
		return MathUtils.murmurHash(keyHash) % maxParallelism;
	}

	/**
	 * Computes the range of key-groups that are assigned to a given operator under the given parallelism and maximum
	 * parallelism.
	 *
	 * IMPORTANT: maxParallelism must be <= Short.MAX_VALUE to avoid rounding problems in this method. If we ever want
	 * to go beyond this boundary, this method must perform arithmetic on long values.
	 *
	 * @param maxParallelism Maximal parallelism that the job was initially created with.
	 * @param parallelism    The current parallelism under which the job runs. Must be <= maxParallelism.
	 * @param operatorIndex  Id of a key-group. 0 <= keyGroupID < maxParallelism.
	 * @return
	 */
	public static KeyGroupRange computeKeyGroupRangeForOperatorIndex(
			int maxParallelism,
			int parallelism,
			int operatorIndex) {

		checkParallelismPreconditions(parallelism);
		checkParallelismPreconditions(maxParallelism);

		Preconditions.checkArgument(maxParallelism >= parallelism,
				"Maximum parallelism must not be smaller than parallelism.");

		int start = operatorIndex == 0 ? 0 : ((operatorIndex * maxParallelism - 1) / parallelism) + 1;
		int end = ((operatorIndex + 1) * maxParallelism - 1) / parallelism;
		return new KeyGroupRange(start, end);
	}

	/**
	 * Computes the index of the operator to which a key-group belongs under the given parallelism and maximum
	 * parallelism.
	 * 计算一个<b>key-group</b>在给定并行度和最大并行度下归属的操作符的索引
	 *
	 * IMPORTANT: maxParallelism must be <= Short.MAX_VALUE to avoid rounding problems in this method. If we ever want
	 * to go beyond this boundary, this method must perform arithmetic on long values.
	 *
	 * @param maxParallelism Maximal parallelism that the job was initially created with.
	 *                       0 < parallelism <= maxParallelism <= Short.MAX_VALUE must hold.
	 * @param parallelism    The current parallelism under which the job runs. Must be <= maxParallelism.
	 * @param keyGroupId     Id of a key-group. 0 <= keyGroupID < maxParallelism.
	 * @return The index of the operator to which elements from the given key-group should be routed under the given
	 * parallelism and maxParallelism.
	 *
	 * 由于{@code keyGroupId}是通过{@link #computeKeyGroupForKeyHash(int, int)},
	 * 所以{@code keyGroupId}的取值是在[0, maxParallelism]之间,
	 * 所以{@code keyGroupId * parallelism / maxParallelism}的结果为[0, parallelism)之间的整数
	 *
	 */
	public static int computeOperatorIndexForKeyGroup(int maxParallelism, int parallelism, int keyGroupId) {
		return keyGroupId * parallelism / maxParallelism;
	}

	/**
	 * Computes a default maximum parallelism from the operator parallelism. This is used in case the user has not
	 * explicitly configured a maximum parallelism to still allow a certain degree of scale-up.
	 *
	 * @param operatorParallelism the operator parallelism as basis for computation.
	 * @return the computed default maximum parallelism.
	 */
	public static int computeDefaultMaxParallelism(int operatorParallelism) {

		checkParallelismPreconditions(operatorParallelism);

		return Math.min(
				Math.max(
						MathUtils.roundUpToPowerOfTwo(operatorParallelism + (operatorParallelism / 2)),
						DEFAULT_LOWER_BOUND_MAX_PARALLELISM),
				UPPER_BOUND_MAX_PARALLELISM);
	}

	public static void checkParallelismPreconditions(int parallelism) {
		Preconditions.checkArgument(parallelism > 0
						&& parallelism <= UPPER_BOUND_MAX_PARALLELISM,
				"Operator parallelism not within bounds: " + parallelism);
	}
}
