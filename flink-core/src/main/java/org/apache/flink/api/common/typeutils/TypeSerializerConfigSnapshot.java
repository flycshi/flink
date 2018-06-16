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

package org.apache.flink.api.common.typeutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.io.VersionedIOReadableWritable;
import org.apache.flink.util.Preconditions;

/**
 * A {@code TypeSerializerConfigSnapshot} is a point-in-time view of a {@link TypeSerializer's} configuration.
 * The configuration snapshot of a serializer is persisted along with checkpoints of the managed state that the
 * serializer is registered to.
 * {@code TypeSerializerConfigSnapshot}是{@link TypeSerializer}配置的时间点视图。
 * 序列化器的配置快照与注册了序列化器的托管状态的检查点一起保存。
 *
 * <p>The persisted configuration may later on be used by new serializers to ensure serialization compatibility
 * for the same managed state. In order for new serializers to be able to ensure this, the configuration snapshot
 * should encode sufficient information about:
 * 持久化配置稍后可能会被新的序列化器使用，以确保对相同托管状态的序列化兼容性。
 * 为了使新的序列化器能够确保这一点，配置快照应该对以下信息进行编码:
 *
 * <ul>
 *   <li><strong>Parameter settings of the serializer:</strong> parameters of the serializer include settings
 *   required to setup the serializer, or the state of the serializer if it is stateful. If the serializer
 *   has nested serializers, then the configuration snapshot should also contain the parameters of the nested
 *   serializers.</li>
 *   序列化器的参数包括设置序列化器所需的设置，或者有状态的序列化器的状态。
 *   如果序列化器有嵌套的序列化器，那么配置快照也应该包含嵌套序列化器的参数。
 *
 *   <li><strong>Serialization schema of the serializer:</strong> the data format used by the serializer.</li>
 * </ul>
 *
 * <p>NOTE: Implementations must contain the default empty nullary constructor. This is required to be able to
 * deserialize the configuration snapshot from its binary form.
 */
@PublicEvolving
public abstract class TypeSerializerConfigSnapshot extends VersionedIOReadableWritable {

	/** The user code class loader; only relevant if this configuration instance was deserialized from binary form. */
	private ClassLoader userCodeClassLoader;

	/**
	 * Set the user code class loader.
	 * Only relevant if this configuration instance was deserialized from binary form.
	 *
	 * <p>This method is not part of the public user-facing API, and cannot be overriden.
	 *
	 * @param userCodeClassLoader user code class loader.
	 */
	@Internal
	public final void setUserCodeClassLoader(ClassLoader userCodeClassLoader) {
		this.userCodeClassLoader = Preconditions.checkNotNull(userCodeClassLoader);
	}

	/**
	 * Returns the user code class loader.
	 * Only relevant if this configuration instance was deserialized from binary form.
	 *
	 * @return the user code class loader
	 */
	@Internal
	public final ClassLoader getUserCodeClassLoader() {
		return userCodeClassLoader;
	}

	public abstract boolean equals(Object obj);

	public abstract int hashCode();
}
