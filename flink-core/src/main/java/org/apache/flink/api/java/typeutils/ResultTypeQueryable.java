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

package org.apache.flink.api.java.typeutils;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * This interface can be implemented by functions and input formats to tell the framework
 * about their produced data type. This method acts as an alternative to the reflection analysis
 * that is otherwise performed and is useful in situations where the produced data type may vary
 * depending on parametrization.
 * 这个接口可以被函数和输入格式实现，来告诉框架，他们的生产数据类型。
 * 该方法可以作为反射分析的一种选择，在生产数据类型依赖参数时，是比较有用的。
 */
@Public
public interface ResultTypeQueryable<T> {

	/**
	 * Gets the data type (as a {@link TypeInformation}) produced by this function or input format.
	 * 
	 * @return The data type produced by this function or input format.
	 */
	TypeInformation<T> getProducedType();
}
