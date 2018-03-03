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

package org.apache.flink.api.common.accumulators;

import org.apache.flink.annotation.Public;

import java.io.Serializable;

/**
 * Accumulators collect distributed statistics or aggregates in a from user functions
 * and operators. Each parallel instance creates and updates its own accumulator object,
 * and the different parallel instances of the accumulator are later merged.
 * merged by the system at the end of the job. The result can be obtained from the
 * result of a job execution, or from the web runtime monitor.
 * 累加器从用户函数或操作符中收集分布式的统计数据或者进行聚合。
 * 每一个并行实例构建和更新它自己的累加对象，然后累加器的不同并行实例后续进行合并。
 * 在job结束时有系统合并。
 * 结果可以从一个job执行结果，或者运行时web监控中获取。
 *
 * The accumulators are inspired by the Hadoop/MapReduce counters.
 * 累加器是参考的 Hadoop/MapReduce 的计数器
 * 
 * The type added to the accumulator might differ from the type returned. This
 * is the case e.g. for a set-accumulator: We add single objects, but the result
 * is a set of objects.
 * 加到累加器上的类型可能与返回的类型不同。
 * 比如集合累加器：我们添加独立对象，但是结果是一个对象集合。
 * 
 * @param <V>
 *            Type of values that are added to the accumulator
 *            添加到累加器中的值的类型
 * @param <R>
 *            Type of the accumulator result as it will be reported to the
 *            client
 *            累加器结果的类型，它会被报告给客户端
 */
@Public
public interface Accumulator<V, R extends Serializable> extends Serializable, Cloneable {
	/**
	 * @param value
	 *            The value to add to the accumulator object
	 *            添加到累加器对象的值
	 */
	void add(V value);

	/**
	 * @return local The local value from the current UDF context
	 * 			当前UDF上下文的本地值
	 */
	R getLocalValue();

	/**
	 * Reset the local value. This only affects the current UDF context.
	 * 重置本地值。这个仅仅影响当前UDF上下文
	 */
	void resetLocal();

	/**
	 * Used by system internally to merge the collected parts of an accumulator
	 * at the end of the job.
	 * 在job结束时，被系统内部用来合并一个累加器
	 * 
	 * @param other Reference to accumulator to merge in.
	 */
	void merge(Accumulator<V, R> other);

	/**
	 * Duplicates the accumulator. All subclasses need to properly implement
	 * cloning and cannot throw a {@link java.lang.CloneNotSupportedException}
	 * 克隆这个累加器。
	 * 所有子类需要恰当的实现克隆方法，不能抛出 CloneNotSupportedException
	 *
	 * @return The duplicated accumulator.
	 */
	Accumulator<V, R> clone();
}
