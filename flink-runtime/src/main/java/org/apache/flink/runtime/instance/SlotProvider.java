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

package org.apache.flink.runtime.instance;

import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * The slot provider is responsible for preparing slots for ready-to-run tasks.
 * 负责为{@code ready-to-run}的任务准备slots。
 * 
 * <p>It supports two allocating modes:
 * 		支持两个分配模式:
 * <ul>
 *     <li>Immediate allocating: A request for a task slot immediately gets satisfied, we can call
 *         {@link CompletableFuture#getNow(Object)} to get the allocated slot.</li>
 *         立即分配: 获取一个任务槽位的一个请求立即被满足, 我么可以调用{@link CompletableFuture#getNow(Object)}获取分配的槽位
 *     <li>Queued allocating: A request for a task slot is queued and returns a future that will be
 *         fulfilled as soon as a slot becomes available.</li>
 *         排队分配: 获取一个任务槽位的请求被入队列, 并返回future, 该future在一个槽位变为有效时就可以获取到分配的槽位。
 * </ul>
 */
public interface SlotProvider {

	/**
	 * Allocating slot with specific requirement.
	 * 按要求分配槽位
	 *
	 * @param task         The task to allocate the slot for
	 *                     需要分配槽位的task
	 * @param allowQueued  Whether allow the task be queued if we do not have enough resource
	 *                     如果没有足够的资源, 是否允许将task加入队列
	 * @param preferredLocations preferred locations for the slot allocation
	 *                           优先的位置分配
	 * @return The future of the allocation
	 */
	CompletableFuture<SimpleSlot> allocateSlot(
		ScheduledUnit task,
		boolean allowQueued,
		Collection<TaskManagerLocation> preferredLocations);
}
