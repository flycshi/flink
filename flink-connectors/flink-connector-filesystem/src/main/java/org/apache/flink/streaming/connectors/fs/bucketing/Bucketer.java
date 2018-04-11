/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.fs.bucketing;

import org.apache.flink.streaming.connectors.fs.Clock;

import org.apache.hadoop.fs.Path;

import java.io.Serializable;

/**
 * A bucketer is used with a {@link BucketingSink}
 * to put emitted elements into rolling files.
 * {@code BucketingSink}用来将一个发射元素放入滚动文件中。
 *
 *
 * <p>The {@code BucketingSink} can be writing to many buckets at a time, and it is responsible for managing
 * a set of active buckets. Whenever a new element arrives it will ask the {@code Bucketer} for the bucket
 * path the element should fall in. The {@code Bucketer} can, for example, determine buckets based on
 * system time.
 * {@code BucketingSink}可以同时写入多个buckets，并且它负责管理一个活跃buckets的集合。
 * 当一个新的元素到达时，它将询问{@code Bucketer}，这个元素应该落入的bucket路径。
 * {@code Bucketer}，比如，可以基于系统时间决定buckets。
 */
public interface Bucketer<T> extends Serializable {
	/**
	 * Returns the {@link Path} of a bucket file.
	 * 返回一个bucket文件的路径。
	 *
	 * @param basePath The base path containing all the buckets.	包含所有buckets的父路径
	 * @param element The current element being processed.			当前需要被处理的元素
	 *
	 * @return The complete {@code Path} of the bucket which the provided element should fall in. This
	 * should include the {@code basePath} and also the {@code subtaskIndex} to avoid clashes with
	 * parallel sinks.
	 * 提供的元素需要落入的bucket的完整路径。
	 * 路径中需要包含{@code basePath}，以及{@code subtaskIndex}，以此来避免并行sink子任务之间的冲突。
	 */
	Path getBucketPath(Clock clock, Path basePath, T element);
}
