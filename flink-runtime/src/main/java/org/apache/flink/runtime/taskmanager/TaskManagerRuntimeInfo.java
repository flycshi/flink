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

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.configuration.Configuration;

/**
 * Interface to access {@link TaskManager} information.
 * 获取{@code TaskManager}信息的接口
 */
public interface TaskManagerRuntimeInfo {

	/**
	 * Gets the configuration that the TaskManager was started with.
	 * 获取{@code TaskManager}启动所用的配置
	 *
	 * @return The configuration that the TaskManager was started with.
	 */
	Configuration getConfiguration();

	/**
	 * Gets the list of temporary file directories.
	 * 获取临时文件目录列表
	 * 
	 * @return The list of temporary file directories.
	 */
	String[] getTmpDirectories();

	/**
	 * Checks whether the TaskManager should exit the JVM when the task thread throws
	 * an OutOfMemoryError.
	 * 检查当任务线程抛出一个OOM错误时，{@code TaskManager}是否应该退出JVM。
	 * 
	 * @return True to terminate the JVM on an OutOfMemoryError, false otherwise.
	 */
	boolean shouldExitJvmOnOutOfMemoryError();
}
