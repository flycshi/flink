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

package org.apache.flink.core.io;

import org.apache.flink.annotation.Public;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * This interface must be implemented by every class whose objects have to be serialized to their binary representation
 * and vice-versa. In particular, records have to implement this interface in order to specify how their data can be
 * transferred to a binary representation.
 * 对象需要被序列化成二进制形式的每个类都需要实现这个接口, 反之亦然。
 * 一般的, 记录需要实现这个接口, 以便指定他们的数据如何转换成一个二进制形式。
 * 
 * <p>When implementing this Interface make sure that the implementing class has a default
 * (zero-argument) constructor!
 * 当实现这个接口的时候, 确保实现类有一个无参构造函数。
 */
@Public
public interface IOReadableWritable {

	/**
	 * Writes the object's internal data to the given data output view.
	 * 将对象的内部数据写到指定的数据输出视图中,
	 * 
	 * @param out
	 *        the output view to receive the data.
	 * @throws IOException
	 *         thrown if any error occurs while writing to the output stream
	 */
	void write(DataOutputView out) throws IOException;

	/**
	 * Reads the object's internal data from the given data input view.
	 * 从给定的数据输入视图中读取对象的内部数据
	 * 
	 * @param in
	 *        the input view to read the data from
	 * @throws IOException
	 *         thrown if any error occurs while reading from the input stream
	 */
	void read(DataInputView in) throws IOException;

}
