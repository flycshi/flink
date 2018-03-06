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


package org.apache.flink.types;

import org.apache.flink.annotation.Public;
import org.apache.flink.core.io.IOReadableWritable;

import java.io.Serializable;

/**
 * Basic value interface for types that act as serializable values.
 * 作为可序列化值的类型的基本值接口。
 *
 * <p>
 * This interface extends {@link IOReadableWritable} and requires to implement
 * the serialization of its value.
 * 这个继承自{@link IOReadableWritable}, 并且需要实现它的value的序列化
 * 
 * @see org.apache.flink.core.io.IOReadableWritable
 */
@Public
public interface Value extends IOReadableWritable, Serializable {
}
