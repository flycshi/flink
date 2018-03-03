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

package org.apache.flink.runtime.clusterframework.types;

import org.apache.flink.util.AbstractID;

/**
 * Unique identifier for a slot allocated by a JobManager from a TaskManager.
 * Also identifies a pending allocation request, and is constant across retries.
 * 由 JobManager 从 TaskManager 中分配的一个槽位的唯一标识。
 * 当然也标识一个挂起的分配请求，并且在重试中保持不变。
 * 
 * <p>This ID is used for all synchronization of the status of Slots from TaskManagers
 * that are not free (i.e., have been allocated by a job).
 * 此ID用于所有来着 TaskManager 的槽位的状态的同步
 */
public class AllocationID extends AbstractID {

	private static final long serialVersionUID = 1L;

	/**
	 * Constructs a new random AllocationID.
	 */
	public AllocationID() {
		super();
	}

	/**
	 * Constructs a new AllocationID with the given parts.
	 *
	 * @param lowerPart the lower bytes of the ID
	 * @param upperPart the higher bytes of the ID
	 */
	public AllocationID(long lowerPart, long upperPart) {
		super(lowerPart, upperPart);
	}
}
