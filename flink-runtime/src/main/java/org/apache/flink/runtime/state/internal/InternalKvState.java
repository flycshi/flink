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

package org.apache.flink.runtime.state.internal;

import org.apache.flink.api.common.state.State;

/**
 * The {@code InternalKvState} is the root of the internal state type hierarchy, similar to the
 * {@link State} being the root of the public API state hierarchy.
 * {@code InternalKvState}是内部state类型层级的根，就好像{@link State}是公用API state层级的根一样。
 * 
 * <p>The internal state classes give access to the namespace getters and setters and access to
 * additional functionality, like raw value access or state merging.
 * 内部状态类提供namespace访问的get和set方法，以及额外的访问函数，比如原生value的访问，state的合并
 * 
 * <p>The public API state hierarchy is intended to be programmed against by Flink applications.
 * The internal state hierarchy holds all the auxiliary methods that are used by the runtime and not
 * intended to be used by user applications. These internal methods are considered of limited use to users and
 * only confusing, and are usually not regarded as stable across releases.
 * 公共API状态层次结构旨在通过Flink应用程序进行编程。
 * 内部状态层次结构包含运行时使用的所有辅助方法，而用户应用程序不打算使用这些辅助方法。
 * 这些内部方法被认为对用户的使用是有限的，并且只会让人感到困惑，并且通常不会被认为是跨版本稳定的。
 * 
 * <p>Each specific type in the internal state hierarchy extends the type from the public
 * state hierarchy:
 * 内部状态层次结构中的每个特定类型都从公共状态层次结构扩展类型
 * 
 * <pre>
 *             State
 *               |
 *               +-------------------InternalKvState
 *               |                         |
 *          MergingState                   |
 *               |                         |
 *               +-----------------InternalMergingState
 *               |                         |
 *      +--------+------+                  |
 *      |               |                  |
 * ReducingState    ListState        +-----+-----------------+
 *      |               |            |                       |
 *      +-----------+   +-----------   -----------------InternalListState
 *                  |                |
 *                  +---------InternalReducingState
 * </pre>
 * 
 * @param <N> The type of the namespace.
 */
public interface InternalKvState<N> extends State {

	/**
	 * Sets the current namespace, which will be used when using the state access methods.
	 * 设置当前namespace，在使用state访问方法时，会用到
	 *
	 * @param namespace The namespace.
	 */
	void setCurrentNamespace(N namespace);

	/**
	 * Returns the serialized value for the given key and namespace.
	 * 获取给定key和namespace对应的序列化value
	 *
	 * <p>If no value is associated with key and namespace, <code>null</code>
	 * is returned.
	 * 如果没有关联的value，则返回<code>null</code>
	 *
	 * @param serializedKeyAndNamespace Serialized key and namespace
	 * @return Serialized value or <code>null</code> if no value is associated with the key and namespace.
	 * 
	 * @throws Exception Exceptions during serialization are forwarded
	 */
	byte[] getSerializedValue(byte[] serializedKeyAndNamespace) throws Exception;
}
