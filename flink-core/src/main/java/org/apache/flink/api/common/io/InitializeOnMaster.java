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

package org.apache.flink.api.common.io;

import org.apache.flink.annotation.Public;

import java.io.IOException;

/**
 * This interface may be implemented by {@link OutputFormat}s to have the master initialize them globally.
 * 这个接口可以被{@link OutputFormat}实现, 用来让master进行全局的初始化
 * 
 * For example, the {@link FileOutputFormat} implements this behavior for distributed file systems and
 * creates/deletes target directories if necessary.
 * 比如, {@link FileOutputFormat}为分布式文件系统实现这个行为, 如果有必要, 创建/删除目标地址。
 */
@Public
public interface InitializeOnMaster {

	/**
	 * The method is invoked on the master (JobManager) before the distributed program execution starts.
	 * 在分布式程序执行开始之前, 这个方法被master(JobManager)调用
	 * 
	 * @param parallelism The parallelism with which the format or functions will be run.	格式或者函数将运行的并行度
	 * @throws IOException The initialization may throw exceptions, which may cause the job to abort.
	 */
	void initializeGlobal(int parallelism) throws IOException;
}
