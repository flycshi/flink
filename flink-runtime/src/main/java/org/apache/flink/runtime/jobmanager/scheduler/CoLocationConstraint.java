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

package org.apache.flink.runtime.jobmanager.scheduler;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.AbstractID;
import org.apache.flink.runtime.instance.Instance;

import org.apache.flink.util.Preconditions;
import org.apache.flink.runtime.instance.SharedSlot;

import static org.apache.flink.util.Preconditions.checkState;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A CoLocationConstraint manages the location of a set of tasks
 * (Execution Vertices). In co-location groups, the different subtasks of
 * different JobVertices need to be executed on the same {@link Instance}.
 * This is realized by creating a special shared slot that holds these tasks.
 * 一个 CoLocationConstraint 管理一个任务集合的位置。
 * 在位置联合组中, 不同 JobVertex 的不同子任务需要在相同的 Instance 上执行。
 * 是通过创建一个持有这些tasks的特殊SharedSlot来实现的。
 * 
 * <p>This class tracks the location and the shared slot for this set of tasks.
 * 		这个类为task集合跟踪位置和SharedSlot
 */
public class CoLocationConstraint {

	/** 当前实例归属的 CoLocationGroup 实例 */
	private final CoLocationGroup group;

	/** 当前实例跟踪的 SharedSlot 实例 */
	private volatile SharedSlot sharedSlot;

	/** 当前实例跟踪的位置 */
	private volatile ResourceID lockedLocation;


	CoLocationConstraint(CoLocationGroup group) {
		Preconditions.checkNotNull(group);
		this.group = group;
	}

	// ------------------------------------------------------------------------
	//  Status & Properties
	//  状态 & 属性
	// ------------------------------------------------------------------------

	/**
	 * Gets the shared slot into which this constraint's tasks are places.
	 * 获取该约束的tasks被放置的SharedSlot实例
	 * 
	 * @return The shared slot into which this constraint's tasks are places.
	 */
	public SharedSlot getSharedSlot() {
		return sharedSlot;
	}

	/**
	 * Gets the ID that identifies the co-location group.
	 * 获取标识位置联合组的ID
	 * 
	 * @return The ID that identifies the co-location group.
	 */
	public AbstractID getGroupId() {
		return this.group.getId();
	}

	/**
	 * Checks whether the location of this constraint has been assigned.
	 * The location is assigned once a slot has been set, via the
	 * {@link #setSharedSlot(org.apache.flink.runtime.instance.SharedSlot)} method,
	 * and the location is locked via the {@link #lockLocation()} method.
	 * 检查该约束的位置是否已经分配。
	 * 通过 {@link #setSharedSlot(org.apache.flink.runtime.instance.SharedSlot)} 方法, 一旦一个 slot 被设置, 位置就分配了
	 * 通过{@link #lockLocation()}方法锁定位置
	 * 
	 * @return True if the location has been assigned, false otherwise.
	 */
	public boolean isAssigned() {
		return lockedLocation != null;
	}

	/**
	 * Checks whether the location of this constraint has been assigned
	 * (as defined in the {@link #isAssigned()} method, and the current
	 * shared slot is alive.
	 * 检查当前约束的位置是否被分配,以及当前SharedSlot是否alive
	 *
	 * @return True if the location has been assigned and the shared slot is alive,
	 *         false otherwise.
	 */
	public boolean isAssignedAndAlive() {
		return lockedLocation != null && sharedSlot.isAlive();
	}

	/**
	 * Gets the location assigned to this slot. This method only succeeds after
	 * the location has been locked via the {@link #lockLocation()} method and
	 * {@link #isAssigned()} returns true.
	 * 获取分配 slot 的位置, 也就是 TaskManager 的位置
	 * 这个方法只有在通过{@link #lockLocation()}方法锁定了location,且{@link #isAssigned()}方法true后, 才能成功返回
	 *
	 * @return The instance describing the location for the tasks of this constraint.
	 * @throws IllegalStateException Thrown if the location has not been assigned, yet.
	 */
	public TaskManagerLocation getLocation() {
		if (lockedLocation != null) {
			return sharedSlot.getTaskManagerLocation();
		} else {
			throw new IllegalStateException("Location not yet locked");
		}
	}

	// ------------------------------------------------------------------------
	//  Assigning resources and location
	// ------------------------------------------------------------------------

	/**
	 * Assigns a new shared slot to this co-location constraint. The shared slot
	 * will hold the subtasks that are executed under this co-location constraint.
	 * If the constraint's location is assigned, then this slot needs to be from
	 * the same location (instance) as the one assigned to this constraint.
	 * 给该实例分配一个新的SharedSlot。
	 * 这个SharedSlot实例会持有在这个实例下执行的子任务。
	 * 如果位置已经被分配,那新的slot需要和之前分配的slot来自相同的TaskManager实例。
	 * 
	 * <p>If the constraint already has a slot, the current one will be released.</p>
	 *
	 * @param newSlot The new shared slot to assign to this constraint.
	 * @throws IllegalArgumentException If the constraint's location has been assigned and
	 *                                  the new slot is from a different location.
	 */
	public void setSharedSlot(SharedSlot newSlot) {
		checkNotNull(newSlot);

		if (this.sharedSlot == null) {
			this.sharedSlot = newSlot;
		}
		else if (newSlot != this.sharedSlot){
			if (lockedLocation != null && lockedLocation != newSlot.getTaskManagerID()) {
				throw new IllegalArgumentException(
						"Cannot assign different location to a constraint whose location is locked.");
			}
			if (this.sharedSlot.isAlive()) {
				this.sharedSlot.releaseSlot();
			}

			this.sharedSlot = newSlot;
		}
	}

	/**
	 * Locks the location of this slot. The location can be locked only once
	 * and only after a shared slot has been assigned.
	 * 锁定slot的位置
	 * 这个位置只能被锁定一次, 且只有在一个SharedSlot被分配后
	 * 
	 * @throws IllegalStateException Thrown, if the location is already locked,
	 *                               or is no slot has been set, yet.
	 */
	public void lockLocation() throws IllegalStateException {
		checkState(lockedLocation == null, "Location is already locked");
		checkState(sharedSlot != null, "Cannot lock location without a slot.");

		lockedLocation = sharedSlot.getTaskManagerID();
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------
	
	@Override
	public String toString() {
		return "CoLocation constraint id " + getGroupId() + " shared slot " + sharedSlot;
	}
}
