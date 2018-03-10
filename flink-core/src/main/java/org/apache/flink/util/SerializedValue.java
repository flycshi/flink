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

package org.apache.flink.util;

import org.apache.flink.annotation.Internal;

import java.io.IOException;
import java.util.Arrays;

/**
 * This class is used to transfer (via serialization) objects whose classes are not available
 * in the system class loader. When those objects are deserialized without access to their
 * special class loader, the deserialization fails with a {@code ClassNotFoundException}.
 * 这个类是用来传输(通过序列化)那些在系统类加载器中不存在的类的对象的。
 * 当这些对象没有使用它们特定的类加载器进行反序列化时，反序列化会失败，并抛出 ClassNotFoundException。
 *
 * To work around that issue, the SerializedValue serialized data immediately into a byte array.
 * When send through RPC or another service that uses serialization, only the byte array is
 * transferred. The object is deserialized later (upon access) and requires the accessor to
 * provide the corresponding class loader.
 * 为了解决这个问题，SerializedValue 会立即将数据序列化成一个字节数组。
 * 当通过rpc或者其他使用序列化的服务发送时，只有字节数组会被传输。
 * 对象在被取用时，才会进行反序列化，并且需要取用者提供相应的类加载器。
 *
 * @param <T> The type of the value held.
 *            持有的值的类型
 */
@Internal
public class SerializedValue<T> implements java.io.Serializable {

	private static final long serialVersionUID = -3564011643393683761L;

	/**
	 * The serialized data
	 * 序列化后的字节数组
	 */
	private final byte[] serializedData;

	private SerializedValue(byte[] serializedData) {
		Preconditions.checkNotNull(serializedData, "Serialized data");
		this.serializedData = serializedData;
	}

	public SerializedValue(T value) throws IOException {
		this.serializedData = value == null ? null : InstantiationUtil.serializeObject(value);
	}

	@SuppressWarnings("unchecked")
	public T deserializeValue(ClassLoader loader) throws IOException, ClassNotFoundException {
		Preconditions.checkNotNull(loader, "No classloader has been passed");
		return serializedData == null ? null : (T) InstantiationUtil.deserializeObject(serializedData, loader);
	}

	/**
	 * Returns the serialized value or <code>null</code> if no value is set.
	 *
	 * @return Serialized data.
	 */
	public byte[] getByteArray() {
		return serializedData;
	}

	public static <T> SerializedValue<T> fromBytes(byte[] serializedData) {
		return new SerializedValue<T>(serializedData);
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public int hashCode() {
		return serializedData == null ? 0 : Arrays.hashCode(serializedData);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof SerializedValue) {
			SerializedValue<?> other = (SerializedValue<?>) obj;
			return this.serializedData == null ? other.serializedData == null :
					(other.serializedData != null && Arrays.equals(this.serializedData, other.serializedData));
		}
		else {
			return false;
		}
	}

	@Override
	public String toString() {
		return "SerializedValue";
	}
}
