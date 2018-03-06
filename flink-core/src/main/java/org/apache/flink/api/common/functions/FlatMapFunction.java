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
import org.apache.flink.util.Collector;

import java.io.Serializable;

/**
 * Base interface for flatMap functions. FlatMap functions take elements and transform them,
 * into zero, one, or more elements. Typical applications can be splitting elements, or unnesting lists
 * and arrays. Operations that produce multiple strictly one result element per input element can also
 * use the {@link MapFunction}.
 * flatMap函数的基础接口。
 * faltmap函数拿到元素, 并转换他们, 可能是0、1、或者更多各元素。
 * 常见应用场景可能是分割元素, 或者解套列表和数组。
 * 每个输入元素严格产生一个结果元素的操作可以使用{@link MapFunction}来实现。
 *
 * <p>
 * The basic syntax for using a FlatMapFunction is as follows:
 * 使用{@code FlatMapFunction}的基本语法如下:
 * <pre>{@code
 * DataSet<X> input = ...;
 * 
 * DataSet<Y> result = input.flatMap(new MyFlatMapFunction());
 * }</pre>
 * 
 * @param <T> Type of the input elements.
 * @param <O> Type of the returned elements.
 */
@Public
public interface FlatMapFunction<T, O> extends Function, Serializable {

	/**
	 * The core method of the FlatMapFunction. Takes an element from the input data set and transforms
	 * it into zero, one, or more elements.
	 *
	 * @param value The input value.
	 * @param out The collector for returning result values.
	 *
	 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
	 *                   to fail and may trigger recovery.
	 */
	void flatMap(T value, Collector<O> out) throws Exception;
}
