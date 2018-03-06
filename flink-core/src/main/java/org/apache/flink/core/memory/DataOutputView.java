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

package org.apache.flink.core.memory;

import org.apache.flink.annotation.Public;

import java.io.DataOutput;
import java.io.IOException;


/**
 * This interface defines a view over some memory that can be used to sequentially write contents to the memory.
 * The view is typically backed by one or more {@link org.apache.flink.core.memory.MemorySegment}.
 * 这个接口定义了一个在内存之上的视图, 从而可以向内存写数据。
 * 这个视图一般以一个或多个{@link org.apache.flink.core.memory.MemorySegment}作为backup
 */
@Public
public interface DataOutputView extends DataOutput {

	/**
	 * Skips {@code numBytes} bytes memory. If some program reads the memory that was skipped over, the
	 * results are undefined.
	 * 跳过的内存字节数。
	 * 如果一些程序读取跳过的内存字节, 结果是不可知的。
	 *
	 * @param numBytes The number of bytes to skip.
	 *
	 * @throws IOException Thrown, if any I/O related problem occurred such that the view could not
	 *                     be advanced to the desired position.
	 */
	void skipBytesToWrite(int numBytes) throws IOException;

	/**
	 * Copies {@code numBytes} bytes from the source to this view.
	 * 从数据源中赋值到view
	 *
	 * @param source The source to copy the bytes from.
	 * @param numBytes The number of bytes to copy.
	 *
	 * @throws IOException Thrown, if any I/O related problem occurred, such that either the input view
	 *                     could not be read, or the output could not be written.
	 */
	void write(DataInputView source, int numBytes) throws IOException;
}
