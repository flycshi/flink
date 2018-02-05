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

package org.apache.flink.api.common;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Specifies to which extent user-defined functions are analyzed in order
 * to give the Flink optimizer an insight of UDF internals and inform
 * the user about common implementation mistakes.
 * 指定用户自定义函数被分析的程度，以便让flink优化器能够看到udf的内部，并指出常见的实现错误。
 *
 * The analyzer gives hints about:
 * 分析器给出的提升有：
 *  - ForwardedFields semantic properties
 *  	语法特征
 *  - Warnings if static fields are modified by a Function
 *  	如果静态字段被一个函数修改，会给出warning
 *  - Warnings if a FilterFunction modifies its input objects
 *  	如果一个filter函数修改了它的输入对象，会给出warning
 *  - Warnings if a Function returns null
 *  	如果一个函数返回null，会给出warning
 *  - Warnings if a tuple access uses a wrong index
 *  	如果访问一个tuple时，使用了错误的索引，会给出warning
 *  - Information about the number of object creations (for manual optimization)
 *  	创建对象的个数的信息（用于手动优化）
 */
@PublicEvolving
public enum CodeAnalysisMode {

	/**
	 * Code analysis does not take place.
	 * 不进行代码分析
	 */
	DISABLE,

	/**
	 * Hints for improvement of the program are printed to the log.
	 * 程序提升的示意被打印到日志中。
	 */
	HINT,

	/**
	 * The program will be automatically optimized with knowledge from code
	 * analysis.
	 * 程序会基于代码分析自动优化。
	 */
	OPTIMIZE;

}
