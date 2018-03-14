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

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmanager.slots.AllocatedSlot;
import org.apache.flink.runtime.jobmanager.slots.SlotOwner;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.AbstractID;

import javax.annotation.Nullable;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * A SimpleSlot represents a single slot on a TaskManager instance, or a slot within a shared slot.
 * 表示一个 TaskManager 实例上的一个单独的 slot ，或是拥有一个 SharedSlot 的 slot。
 *
 * <p>If this slot is part of a {@link SharedSlot}, then the parent attribute will point to that shared slot.
 * If not, then the parent attribute is null.
 * 如果这个slot是一个 SharedSlot 的部分，那 parent 属性将指向那个 SharedSlot。
 * 如果不是，那么 parent 属性为null。
 */
public class SimpleSlot extends Slot {

	/**
	 * The updater used to atomically swap in the execution
	 * 用来在 Execution 中进行原子交换的更新器
	 */
	private static final AtomicReferenceFieldUpdater<SimpleSlot, Execution> VERTEX_UPDATER =
			AtomicReferenceFieldUpdater.newUpdater(SimpleSlot.class, Execution.class, "executedTask");

	// ------------------------------------------------------------------------

	/**
	 * Task being executed in the slot. Volatile to force a memory barrier and allow for correct double-checking
	 * 在slot中执行的task。
	 * Volatile 用来强制一个内存栅栏，允许正确的双重校验。
	 */
	private volatile Execution executedTask;

	/**
	 * The locality attached to the slot, defining whether the slot was allocated at the desired location.
	 * 附加在slot上的地点配置，定义了slot是否被分配到要求的地点
	 */
	private volatile Locality locality = Locality.UNCONSTRAINED;

	// ------------------------------------------------------------------------
	//  Old Constructors (prior FLIP-6)
	// ------------------------------------------------------------------------

	/**
	 * Creates a new simple slot that stands alone and does not belong to shared slot.
	 * 
	 * @param jobID The ID of the job that the slot is allocated for.
	 * @param owner The component from which this slot is allocated.
	 * @param location The location info of the TaskManager where the slot was allocated from
	 * @param slotNumber The number of the task slot on the instance.
	 * @param taskManagerGateway The gateway to communicate with the TaskManager of this slot
	 */
	public SimpleSlot(
			JobID jobID, SlotOwner owner, TaskManagerLocation location, int slotNumber,
			TaskManagerGateway taskManagerGateway) {
		this(jobID, owner, location, slotNumber, taskManagerGateway, null, null);
	}

	/**
	 * Creates a new simple slot that belongs to the given shared slot and
	 * is identified by the given ID.
	 *
	 * @param jobID The ID of the job that the slot is allocated for.
	 * @param owner The component from which this slot is allocated.
	 * @param location The location info of the TaskManager where the slot was allocated from
	 * @param slotNumber The number of the simple slot in its parent shared slot.
	 * @param taskManagerGateway to communicate with the associated task manager.
	 * @param parent The parent shared slot.
	 * @param groupID The ID that identifies the group that the slot belongs to.
	 */
	public SimpleSlot(
			JobID jobID, SlotOwner owner, TaskManagerLocation location, int slotNumber,
			TaskManagerGateway taskManagerGateway,
			@Nullable SharedSlot parent, @Nullable AbstractID groupID) {

		super(parent != null ?
				parent.getAllocatedSlot() :
				new AllocatedSlot(NO_ALLOCATION_ID, jobID, location, slotNumber,
						ResourceProfile.UNKNOWN, taskManagerGateway),
				owner, slotNumber, parent, groupID);
	}

	// ------------------------------------------------------------------------
	//  Constructors
	// ------------------------------------------------------------------------

	/**
	 * Creates a new simple slot that stands alone and does not belong to shared slot.
	 *
	 * @param allocatedSlot The allocated slot that this slot represents.
	 * @param owner The component from which this slot is allocated.
	 * @param slotNumber The number of the task slot on the instance.
	 */
	public SimpleSlot(AllocatedSlot allocatedSlot, SlotOwner owner, int slotNumber) {
		this(allocatedSlot, owner, slotNumber, null, null);
	}

	/**
	 * Creates a new simple slot that belongs to the given shared slot and
	 * is identified by the given ID..
	 *
	 * @param parent The parent shared slot.
	 * @param owner The component from which this slot is allocated.
	 * @param slotNumber The number of the simple slot in its parent shared slot.
	 * @param groupID The ID that identifies the group that the slot belongs to.
	 */
	public SimpleSlot(SharedSlot parent, SlotOwner owner, int slotNumber, AbstractID groupID) {
		this(parent.getAllocatedSlot(), owner, slotNumber, parent, groupID);
	}
	
	/**
	 * Creates a new simple slot that belongs to the given shared slot and
	 * is identified by the given ID..
	 *
	 * @param allocatedSlot The allocated slot that this slot represents.
	 * @param owner The component from which this slot is allocated.
	 * @param slotNumber The number of the simple slot in its parent shared slot.
	 * @param parent The parent shared slot.
	 * @param groupID The ID that identifies the group that the slot belongs to.
	 */
	private SimpleSlot(
			AllocatedSlot allocatedSlot, SlotOwner owner, int slotNumber,
			@Nullable SharedSlot parent, @Nullable AbstractID groupID) {
		super(allocatedSlot, owner, slotNumber, parent, groupID);
	}

	// ------------------------------------------------------------------------
	//  Properties
	// ------------------------------------------------------------------------

	@Override
	public int getNumberLeaves() {
		return 1;
	}

	/**
	 * Gets the task execution attempt currently executed in this slot. This may return null, if no
	 * task execution attempt has been placed into this slot.
	 *
	 * @return The slot's task execution attempt, or null, if no task is executed in this slot, yet.
	 */
	public Execution getExecutedVertex() {
		return executedTask;
	}

	/**
	 * Atomically sets the executed vertex, if no vertex has been assigned to this slot so far.
	 * 如果目前为止还没有被分配过 Execution， 则原子设置 Execution，
	 *
	 * @param executedVertex The vertex to assign to this slot.
	 * @return True, if the vertex was assigned, false, otherwise.
	 */
	public boolean setExecutedVertex(Execution executedVertex) {
		if (executedVertex == null) {
			throw new NullPointerException();
		}

		// check that we can actually run in this slot
		// 检查是否可以在这个slot中运行task
		if (isCanceled()) {
			return false;
		}

		// atomically assign the vertex
		// 原子指定 Execution
		if (!VERTEX_UPDATER.compareAndSet(this, null, executedVertex)) {
			return false;
		}

		// we need to do a double check that we were not cancelled in the meantime
		// 我们需要双重校验，我们没有在同时取消slot
		if (isCanceled()) {
			this.executedTask = null;
			return false;
		}

		return true;
	}

	/**
	 * Gets the locality information attached to this slot.
	 * @return The locality attached to the slot.
	 */
	public Locality getLocality() {
		return locality;
	}

	/**
	 * Attached locality information to this slot.
	 * @param locality The locality attached to the slot.
	 */
	public void setLocality(Locality locality) {
		this.locality = locality;
	}

	// ------------------------------------------------------------------------
	//  Cancelling & Releasing
	//  取消 & 释放
	// ------------------------------------------------------------------------

	@Override
	public void releaseSlot() {
		if (!isCanceled()) {

			// kill all tasks currently running in this slot
			// 杀掉slot中当前运行的所有task
			Execution exec = this.executedTask;
			if (exec != null && !exec.isFinished()) {
				exec.fail(new Exception("TaskManager was lost/killed: " + getTaskManagerLocation()));
			}

			// release directly (if we are directly allocated),
			// otherwise release through the parent shared slot
			/**
			 * 直接释放(如果我们是直接分配的)
			 * 否则通过父SharedSlot释放
			 */
			if (getParent() == null) {
				// we have to give back the slot to the owning instance
				// 我们必须把slot归还给所有者
				if (markCancelled()) {
					getOwner().returnAllocatedSlot(this);
				}
			} else {
				// we have to ask our parent to dispose us
				// 请求父亲去释放
				getParent().releaseChild(this);
			}
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return "SimpleSlot " + super.toString();
	}
}
