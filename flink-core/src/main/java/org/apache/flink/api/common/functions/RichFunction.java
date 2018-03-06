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

package org.apache.flink.api.common.functions;

import org.apache.flink.annotation.Public;
import org.apache.flink.configuration.Configuration;

/**
 * An base interface for all rich user-defined functions. This class defines methods for
 * the life cycle of the functions, as well as methods to access the context in which the functions
 * are executed.
 * 所有丰富的用户自定义函数的一个基本接口。
 * 这个类定义了函数的生命周期的方法, 以及获取函数执行所在的上下文的方法
 */
@Public
public interface RichFunction extends Function {
	
	/**
	 * Initialization method for the function. It is called before the actual working methods 
	 * (like <i>map</i> or <i>join</i>) and thus suitable for one time setup work. For functions that
	 * are part of an iteration, this method will be invoked at the beginning of each iteration superstep.
	 * 函数的初始化方法。
	 * 该方法在实际的工作方法之前被调用(比如 map、join), 因此适合一次配置工作。
	 * 比如对于迭代过程中的一部分的函数, 这个方法会在每次迭代的开始时被调用。
	 *
	 * <p>
	 * The configuration object passed to the function can be used for configuration and initialization.
	 * The configuration contains all parameters that were configured on the function in the program
	 * composition.
	 * 传入的配置对象可以用来配置和初始化。
	 * 配置中包含了构成程序的函数所需的所有配置参数。
	 * 
	 * <pre>{@code
	 * public class MyMapper extends FilterFunction<String> {
	 * 
	 *     private String searchString;
	 *     
	 *     public void open(Configuration parameters) {
	 *         this.searchString = parameters.getString("foo");
	 *     }
	 *     
	 *     public boolean filter(String value) {
	 *         return value.equals(searchString);
	 *     }
	 * }
	 * }</pre>
	 * <p>
	 * By default, this method does nothing.
	 * 默认的, 该方法不做任何处理。
	 * 
	 * @param parameters The configuration containing the parameters attached to the contract.
	 *                   包含了与约定相关参数的配置
	 * 
	 * @throws Exception Implementations may forward exceptions, which are caught by the runtime. When the
	 *                   runtime catches an exception, it aborts the task and lets the fail-over logic
	 *                   decide whether to retry the task execution.
	 *                   具体的实现可能会出现异常, 异常会被runtime捕获。
	 *                   当runtime捕获异常时, 它会终止任务, 并让容灾逻辑决定是否重试任务执行。
	 * 
	 * @see org.apache.flink.configuration.Configuration
	 */
	void open(Configuration parameters) throws Exception;

	/**
	 * Tear-down method for the user code. It is called after the last call to the main working methods
	 * (e.g. <i>map</i> or <i>join</i>). For functions that are part of an iteration, this method will
	 * be invoked after each iteration superstep.
	 * 用户代码的关闭方法。
	 * 在主要的工作方法(比如 map、join)被最后一次调用后, 会调用该方法。
	 * 作为迭代一部分的函数, 该方法在每次迭代后都会被调用。
	 *
	 * <p>
	 * This method can be used for clean up work.
	 * 这个方法可以用于清理工作。
	 * 
	 * @throws Exception Implementations may forward exceptions, which are caught by the runtime. When the
	 *                   runtime catches an exception, it aborts the task and lets the fail-over logic
	 *                   decide whether to retry the task execution.
	 */
	void close() throws Exception;
	
	// ------------------------------------------------------------------------
	//  Runtime context
	//  运行时上下文
	// ------------------------------------------------------------------------
	
	/**
	 * Gets the context that contains information about the UDF's runtime, such as the 
	 * parallelism of the function, the subtask index of the function, or the name of
	 * the of the task that executes the function.
	 * 获取包含UDF的运行时的信息的上下文, 比如函数的并行度、函数的子任务的索引, 或者执行这个函数的任务的名称。
	 * 
	 * <p>The RuntimeContext also gives access to the
	 * {@link org.apache.flink.api.common.accumulators.Accumulator}s and the
	 * {@link org.apache.flink.api.common.cache.DistributedCache}.
	 * 
	 * @return The UDF's runtime context.
	 */
	RuntimeContext getRuntimeContext();

	/**
	 * Gets a specialized version of the {@link RuntimeContext}, which has additional information
	 * about the iteration in which the function is executed. This IterationRuntimeContext is only
	 * available if the function is part of an iteration. Otherwise, this method throws an exception.
	 * 获取{@link RuntimeContext}的一个特定版本, 其拥有函数执行所在的迭代的额外信息。
	 * 只有该方法是一个迭代的一部分时, 这个{@link IterationRuntimeContext}才是有效的。
	 * 否则, 该犯法会抛出一个异常。
	 * 
	 * @return The IterationRuntimeContext.
	 * @throws java.lang.IllegalStateException Thrown, if the function is not executed as part of an iteration.
	 */
	IterationRuntimeContext getIterationRuntimeContext();
	
	/**
	 * Sets the function's runtime context. Called by the framework when creating a parallel instance of the function.
	 * 这个函数的运行时上下文。
	 * 当创建函数的一个并行实例时, 由框架调用该方法。
	 *  
	 * @param t The runtime context.
	 */
	void setRuntimeContext(RuntimeContext t);
}
