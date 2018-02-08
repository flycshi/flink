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

package org.apache.flink.core.fs;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.net.URI;

/**
 * A factory to create file systems.
 * 创建文件系统的工厂
 *
 * <p>The factory is typically configured via {@link #configure(Configuration)} before
 * creating file systems via {@link #create(URI)}.
 * 在通过 create(URI) 创建文件系统前，这些工厂一般都会通过 configure(Configuration) 进行配置
 */
@PublicEvolving
public interface FileSystemFactory {

	/**
	 * Gets the scheme of the file system created by this factory.
	 * 获取该工厂创建的文件系统的模式
	 */
	String getScheme();

	/**
	 * Applies the given configuration to this factory. All future file system
	 * instantiations via {@link #create(URI)} should take the configuration into
	 * account.
	 * 将制定的配置应用到这个工厂。
	 * 所有将通过 create(URI) 创建的文件系统实例，都需要考虑这个配置。
	 *
	 * @param config The configuration to apply.
	 */
	void configure(Configuration config);

	/**
	 * Creates a new file system for the given file system URI.
	 * The URI describes the type of file system (via its scheme) and optionally the
	 * authority (for example the host) of the file system.
	 * 为制定的文件系统URI创建一个新的文件系统。
	 * URI描述了文件系统的类型（通过它的模式），以及文件系统的主机（比如host）
	 *
	 * @param fsUri The URI that describes the file system.	描述文件系统的URI
	 * @return A new instance of the specified file system.	一个指定文件系统的实例
	 *
	 * @throws IOException Thrown if the file system could not be instantiated.
	 * 如果文件系统不能实例化，则抛出异常
	 */
	FileSystem create(URI fsUri) throws IOException;
}
