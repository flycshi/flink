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

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmanager.slots.AllocatedSlot;
import org.apache.flink.runtime.jobmanager.slots.SlotOwner;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.AbstractID;
import org.apache.flink.api.common.JobID;

import javax.annotation.Nullable;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class for slots that the Scheduler / ExecutionGraph take from the SlotPool and use to place
 * tasks to execute into. A slot corresponds to an AllocatedSlot (a slice of a TaskManager's resources),
 * plus additional fields to track what is currently executed in that slot, or if the slot is still
 * used or disposed (ExecutionGraph gave it back to the pool).
 * 调度器/执行图 从 {@link SlotPool} 中获取并用来执行任务的槽位的基类。
 * 一个槽位对应于一个 {@link AllocatedSlot} (一个 TaskManager 的资源的一个分片)，
 * 再附加一些字段，用来跟踪当前槽位中执行的内容，或者槽位是否还在使用或者归还( ExecutionGraph 把它归还给了 pool)
 *
 * <p>In the simplest case, a slot holds a single task ({@link SimpleSlot}). In the more complex
 * case, a slot is shared ({@link SharedSlot}) and contains a set of tasks. Shared slots may contain
 * other shared slots which in turn can hold simple slots. That way, a shared slot may define a tree
 * of slots that belong to it.
 * 在最简单的情况下，一个槽位拥有一个单独的任务{@link SimpleSlot}.
 * 在更复杂的情况下，一个槽位时共享的{@link SharedSlot}，并且包含了一个任务集合。
 * SharedSlot 可能包含了其他拥有 SimpleSlot 的SharedSlot。
 * 也就是说，一个 SharedSlot 可能定义了一个 slot 的树
 */
public abstract class Slot {

	/**
	 * Updater for atomic state transitions
	 * 用于原子状态转换
	 */
	private static final AtomicIntegerFieldUpdater<Slot> STATUS_UPDATER =
			AtomicIntegerFieldUpdater.newUpdater(Slot.class, "status");

	/**
	 * State where slot is fresh and alive. Tasks may be added to the slot.
	 * 状态 —— 槽位是新生的和活着的，tasks 可以添加到槽中。
	 */
	private static final int ALLOCATED_AND_ALIVE = 0;

	/**
	 * State where the slot has been canceled and is in the process of being released
	 * 状态 —— 槽位已经被取消掉，并在释放的处理中。
	 */
	private static final int CANCELLED = 1;

	/**
	 * State where all tasks in this slot have been canceled and the slot been given back to the instance
	 * 状态 —— 在这个槽中的所有任务已经被取消掉，槽位已经被规划给实例
	 */
	private static final int RELEASED = 2;

	// temporary placeholder for Slots that are not constructed from an AllocatedSlot (prior to FLIP-6)
	// 不是从一个 AllocatedSlot 中构建的 slot 的临时占位符
	protected static final AllocationID NO_ALLOCATION_ID = new AllocationID(0, 0);

	// ------------------------------------------------------------------------

	/**
	 * The allocated slot that this slot represents.
	 * 这个slot描述的分配的slot
	 */
	private final AllocatedSlot allocatedSlot;

	/**
	 * The owner of this slot - the slot was taken from that owner and must be disposed to it
	 * 这个slot的所有者 - 这个slot从哪个owner获取，就必须归还给哪个。
	 */
	private final SlotOwner owner;

	/**
	 * The parent of this slot in the hierarchy, or null, if this is the parent
	 * 这个slot的父亲，或者，如果本身就是parent，则该字段为null
	 */
	@Nullable
	private final SharedSlot parent;

	/**
	 * The id of the group that this slot is allocated to. May be null.
	 * 这个slot分配给的组的id，可能为null
	 */
	@Nullable
	private final AbstractID groupID;

	/**
	 * The number of the slot on which the task is deployed
	 * task 部署的 slot 的 第几个
	 */
	private final int slotNumber;

	/**
	 * The state of the vertex, only atomically updated
	 * 该节点的状态，只能原子更新
	 */
	private volatile int status = ALLOCATED_AND_ALIVE;

	// --------------------------------------------------------------------------------------------

	/**
	 * Base constructor for slots.
	 * 
	 * <p>This is the old way of constructing slots, prior to the FLIP-6 resource management refactoring.
	 * 
	 * @param jobID The ID of the job that this slot is allocated for.
	 * @param owner The component from which this slot is allocated.
	 * @param location The location info of the TaskManager where the slot was allocated from
	 * @param slotNumber The number of this slot.
	 * @param taskManagerGateway The actor gateway to communicate with the TaskManager
	 * @param parent The parent slot that contains this slot. May be null, if this slot is the root.
	 * @param groupID The ID that identifies the task group for which this slot is allocated. May be null
	 *                if the slot does not belong to any task group.   
	 */
	protected Slot(
			JobID jobID,
			SlotOwner owner,
			TaskManagerLocation location,
			int slotNumber,
			TaskManagerGateway taskManagerGateway,
			@Nullable SharedSlot parent,
			@Nullable AbstractID groupID) {

		checkArgument(slotNumber >= 0);

		this.allocatedSlot = new AllocatedSlot(
			NO_ALLOCATION_ID,
			jobID,
			location,
			slotNumber,
			ResourceProfile.UNKNOWN,
			taskManagerGateway);

		this.owner = checkNotNull(owner);
		this.parent = parent; // may be null
		this.groupID = groupID; // may be null
		this.slotNumber = slotNumber;
	}

	/**
	 * Base constructor for slots.
	 *
	 * @param allocatedSlot The allocated slot that this slot represents.
	 * @param owner The component from which this slot is allocated.
	 * @param slotNumber The number of this slot.
	 * @param parent The parent slot that contains this slot. May be null, if this slot is the root.
	 * @param groupID The ID that identifies the task group for which this slot is allocated. May be null
	 *                if the slot does not belong to any task group.   
	 */
	protected Slot(
			AllocatedSlot allocatedSlot, SlotOwner owner, int slotNumber,
			@Nullable SharedSlot parent, @Nullable AbstractID groupID) {

		checkArgument(slotNumber >= 0);

		this.allocatedSlot = checkNotNull(allocatedSlot);
		this.owner = checkNotNull(owner);
		this.parent = parent; // may be null
		this.groupID = groupID; // may be null
		this.slotNumber = slotNumber;
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Gets the allocated slot that this slot refers to.
	 * 
	 * @return This slot's allocated slot.
	 */
	public AllocatedSlot getAllocatedSlot() {
		return allocatedSlot;
	}

	/**
	 * Returns the ID of the job this allocated slot belongs to.
	 *
	 * @return the ID of the job this allocated slot belongs to
	 */
	public JobID getJobID() {
		return allocatedSlot.getJobID();
	}

	/**
	 * Gets the ID of the TaskManager that offers this slot.
	 *
	 * @return The ID of the TaskManager that offers this slot
	 */
	public ResourceID getTaskManagerID() {
		return allocatedSlot.getTaskManagerLocation().getResourceID();
	}

	/**
	 * Gets the location info of the TaskManager that offers this slot.
	 *
	 * @return The location info of the TaskManager that offers this slot
	 */
	public TaskManagerLocation getTaskManagerLocation() {
		return allocatedSlot.getTaskManagerLocation();
	}

	/**
	 * Gets the actor gateway that can be used to send messages to the TaskManager.
	 *
	 * <p>This method should be removed once the new interface-based RPC abstraction is in place
	 *
	 * @return The actor gateway that can be used to send messages to the TaskManager.
	 */
	public TaskManagerGateway getTaskManagerGateway() {
		return allocatedSlot.getTaskManagerGateway();
	}

	/**
	 * Gets the owner of this slot. The owner is the component that the slot was created from
	 * and to which it needs to be returned after the executed tasks are done.
	 * 
	 * @return The owner of this slot.
	 */
	public SlotOwner getOwner() {
		return owner;
	}

	/**
	 * Gets the number of the slot. For a simple slot, that is the number of the slot
	 * on its instance. For a non-root slot, this returns the number of the slot in the
	 * list amongst its siblings in the tree.
	 *
	 * @return The number of the slot on the instance or amongst its siblings that share the same slot.
	 */
	public int getSlotNumber() {
		return slotNumber;
	}

	/**
	 * Gets the number of the root slot. This code behaves equal to {@code getRoot().getSlotNumber()}.
	 * If this slot is the root of the tree of shared slots, then this method returns the same
	 * value as {@link #getSlotNumber()}.
	 * 获取根slot的数量。
	 * 这段代码的行为等价于 {@code getRoot().getSlotNumber()}
	 * 如果这个slot是共享slot的树的根，那这个方法返回的值就等于 {@link #getSlotNumber()}
	 *
	 * @return The slot number of the root slot.
	 */
	public int getRootSlotNumber() {
		if (parent == null) {
			return slotNumber;
		} else {
			return parent.getRootSlotNumber();
		}
	}

	/**
	 * Gets the ID that identifies the logical group to which this slot belongs:
	 * <ul>
	 *     <li>If the slot does not belong to any group in particular, this field is null.</li>
	 *     <li>If this slot was allocated as a sub-slot of a
	 *         {@link org.apache.flink.runtime.instance.SlotSharingGroupAssignment}, 
	 *         then this ID will be the JobVertexID of the vertex whose task the slot
	 *         holds in its shared slot.</li>
	 *     <li>In case that the slot represents the shared slot of a co-location constraint, this ID will be the
	 *         ID of the co-location constraint.</li>
	 * </ul>
	 * 
	 * @return The ID identifying the logical group of slots.
	 */
	@Nullable
	public AbstractID getGroupID() {
		return groupID;
	}

	/**
	 * Gets the parent slot of this slot. Returns null, if this slot has no parent.
	 * 
	 * @return The parent slot, or null, if no this slot has no parent.
	 */
	@Nullable
	public SharedSlot getParent() {
		return parent;
	}

	/**
	 * Gets the root slot of the tree containing this slot. If this slot is the root,
	 * the method returns this slot directly, otherwise it recursively goes to the parent until
	 * it reaches the root.
	 * 
	 * @return The root slot of the tree containing this slot
	 */
	public Slot getRoot() {
		if (parent == null) {
			return this;
		} else {
			return parent.getRoot();
		}
	}

	/**
	 * Gets the number of simple slots that are at the leaves of the tree of slots.
	 *
	 * @return The number of simple slots at the leaves.
	 */
	public abstract int getNumberLeaves();

	// --------------------------------------------------------------------------------------------
	//  Status and life cycle
	// --------------------------------------------------------------------------------------------

	/**
	 * Checks of the slot is still alive, i.e. in state {@link #ALLOCATED_AND_ALIVE}.
	 *
	 * @return True if the slot is alive, false otherwise.
	 */
	public boolean isAlive() {
		return status == ALLOCATED_AND_ALIVE;
	}

	/**
	 * Checks of the slot has been cancelled. Note that a released slot is also cancelled.
	 * 检查slot是否被取消了。
	 * 注意的是一个 RELEASED slot 也是取消状态
	 *
	 * @return True if the slot is cancelled or released, false otherwise.
	 */
	public boolean isCanceled() {
		return status != ALLOCATED_AND_ALIVE;
	}

	/**
	 * Checks of the slot has been released.
	 *
	 * @return True if the slot is released, false otherwise.
	 */
	public boolean isReleased() {
		return status == RELEASED;
	}

	/**
	 * Atomically marks the slot as cancelled, if it was alive before.
	 *
	 * @return True, if the state change was successful, false otherwise.
	 */
	final boolean markCancelled() {
		return STATUS_UPDATER.compareAndSet(this, ALLOCATED_AND_ALIVE, CANCELLED);
	}

	/**
	 * Atomically marks the slot as released, if it was cancelled before.
	 *
	 * @return True, if the state change was successful, false otherwise.
	 */
	final boolean markReleased() {
		return STATUS_UPDATER.compareAndSet(this, CANCELLED, RELEASED);
	}

	/**
	 * This method cancels and releases the slot and all its sub-slots.
	 * 这个方法取消和释放该slot，以及其子slots
	 * 
	 * After this method completed successfully, the slot will be in state "released", and the
	 * {@link #isReleased()} method will return {@code true}.
	 * 在该方法成功完成后，slot将处于"RELEASED"状态，并且{@link #isReleased()}将返回true
	 * 
	 * If this slot is a simple slot, it will be returned to its instance. If it is a shared slot,
	 * it will release all of its sub-slots and release itself.
	 * 如果该slot是一个 SimpleSlot，它将归还给它的实例。
	 * 如果是一个 SharedSlot，他将释放所有子slot，再释放自己。
	 */
	public abstract void releaseSlot();


	// --------------------------------------------------------------------------------------------
	//  Utilities
	// --------------------------------------------------------------------------------------------

	/**
	 * Slots must always has based on reference identity.
	 */
	@Override
	public final int hashCode() {
		return super.hashCode();
	}

	/**
	 * Slots must always compare on referential equality.
	 */
	@Override
	public final boolean equals(Object obj) {
		return this == obj;
	}

	@Override
	public String toString() {
		return hierarchy() + " - " + getTaskManagerLocation() + " - " + getStateName(status);
	}

	protected String hierarchy() {
		return (getParent() != null ? getParent().hierarchy() : "") + '(' + slotNumber + ')';
	}

	private static String getStateName(int state) {
		switch (state) {
			case ALLOCATED_AND_ALIVE:
				return "ALLOCATED/ALIVE";
			case CANCELLED:
				return "CANCELLED";
			case RELEASED:
				return "RELEASED";
			default:
				return "(unknown)";
		}
	}
}
