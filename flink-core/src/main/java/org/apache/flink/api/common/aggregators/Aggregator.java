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


package org.apache.flink.api.common.aggregators;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.types.Value;

import java.io.Serializable;

/**
 * Aggregators are a means of aggregating values across parallel instances of a function. Aggregators  
 * collect simple statistics (such as the number of processed elements) about the actual work performed in a function.
 * Aggregators are specific to iterations and are commonly used to check the convergence of an iteration by using a
 * {@link ConvergenceCriterion}. In contrast to the {@link org.apache.flink.api.common.accumulators.Accumulator}
 * (whose result is available at the end of a job,
 * the aggregators are computed once per iteration superstep. Their value can be used to check for convergence (at the end
 * of the iteration superstep) and it can be accessed in the next iteration superstep.
 * 聚合器是在一个函数的并行实例间聚合value的一种方法。
 * 聚合器手机关于在一个函数中执行的实际工作的简单统计信息(比如处理过的元素的数量)。
 * 聚合器是特定于迭代的, 通常通过使用{@link ConvergenceCriterion}来检查迭代的收敛。
 * 与{@link org.apache.flink.api.common.accumulators.Accumulator}不同(其结果在job结束时才有效), 聚合器在每次迭代开始时都会计算一次。
 * 他们的值可以被用来检查收敛(在每次迭代终点), 在下次迭代中还可以获取。
 *
 * <p>
 * Aggregators must be registered at the iteration inside which they are used via the function. In the Java API, the
 * method is "IterativeDataSet.registerAggregator(...)" or "IterativeDataSet.registerAggregationConvergenceCriterion(...)"
 * when using the aggregator together with a convergence criterion. Aggregators are always registered under a name. That
 * name can be used to access the aggregator at runtime from within a function. The following code snippet shows a typical
 * case. Here, it count across all parallel instances how many elements are filtered out by a function.
 * 聚合器必须在其通过函数使用的迭代中注册。
 * 在java API中, 当使用聚合器为收敛准则时, 方法是{@code IterativeDataSet.registerAggregator(...)}
 * 或者{@code IterativeDataSet.registerAggregationConvergenceCriterion(...)}。
 * 聚合器总是注册在一个名称下。
 * 如下示例, 统计在所有并行实例下, 一个函数过滤了多少元素。
 * 
 * <pre>
 * // the user-defined function
 * // 用户自定义函数
 * public class MyFilter extends FilterFunction&lt;Double&gt; {
 *     private LongSumAggregator agg;
 *     
 *     public void open(Configuration parameters) {
 *         agg = getIterationRuntimeContext().getIterationAggregator("numFiltered");
 *     }
 *     
 *     public boolean filter (Double value) {
 *         if (value &gt; 1000000.0) {
 *             agg.aggregate(1);
 *             return false
 *         }
 *         
 *         return true;
 *     }
 * }
 * 
 * // the iteration where the aggregator is registered
 * // 注册聚合器的迭代器
 * IterativeDataSet&lt;Double&gt; iteration = input.iterate(100).registerAggregator("numFiltered", LongSumAggregator.class);
 * ...
 * DataSet&lt;Double&gt; filtered = someIntermediateResult.filter(new MyFilter);
 * ...
 * DataSet&lt;Double&gt; result = iteration.closeWith(filtered);
 * ...
 * </pre>
 * 
 * <p>
 * Aggregators must be <i>distributive</i>: An aggregator must be able to pre-aggregate values and it must be able
 * to aggregate these pre-aggregated values to form the final aggregate. Many aggregation functions fulfill this
 * condition (sum, min, max) and others can be brought into that form: One can expressing <i>count</i> as a sum over
 * values of one, and one can express <i>average</i> through a sum and a count.
 * 聚合器必须是分布式的: 一个聚合器必须能够预聚合值, 它必须能够聚合这些预聚合的值, 从而形成最终的聚合。
 * 很多聚合函数满足这个条件(sum, min, max), 其他的可以被引入这个形式: 一个可以将count表示为值的和, 并且通过sum和count来表示average
 *
 * @param <T> The type of the aggregated value.
 */
@PublicEvolving
public interface Aggregator<T extends Value> extends Serializable {

	/**
	 * Gets the aggregator's current aggregate.
	 * 获取聚合器的当前聚合值
	 * 
	 * @return The aggregator's current aggregate.
	 */
	T getAggregate();

	/**
	 * Aggregates the given element. In the case of a <i>sum</i> aggregator, this method adds the given
	 * value to the sum.
	 * 聚合给定元素。
	 * 如果是<i>sum</i>聚合器, 该方法将给定值累加上去
	 * 
	 * @param element The element to aggregate.
	 */
	void aggregate(T element);

	/**
	 * Resets the internal state of the aggregator. This must bring the aggregator into the same
	 * state as if it was newly initialized.
	 * 重置聚合器的内部状态。
	 * 这个方法必须将聚合器的状态设置为初始状态。
	 */
	void reset();
}
