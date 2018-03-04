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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotAvailabilityListener;
import org.apache.flink.runtime.jobmanager.slots.SlotOwner;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * An instance represents a {@link org.apache.flink.runtime.taskmanager.TaskManager}
 * registered at a JobManager and ready to receive work.
 * 表示注册在一个 JobManager 上，并且可以接收工作的 TaskManager 的一个实例。
 */
public class Instance implements SlotOwner {

	private final static Logger LOG = LoggerFactory.getLogger(Instance.class);

	/**
	 * The lock on which to synchronize allocations and failure state changes
	 * 用来同步分配和失败状态变化的lock
	 */
	private final Object instanceLock = new Object();

	/**
	 * The instance gateway to communicate with the instance
	 * 与 TaskManager 实例进行交互的网关实例
	 */
	private final TaskManagerGateway taskManagerGateway;

	/**
	 * The instance connection information for the data transfer.
	 * 用于数据传输的连接信息的实例
	 */
	private final TaskManagerLocation location;

	/**
	 * A description of the resources of the task manager
	 * TaskManager 的一个资源描述
	 */
	private final HardwareDescription resources;

	/**
	 * The ID identifying the taskManager.
	 * TaskManager 的ID标识
	 */
	private final InstanceID instanceId;

	/**
	 * The number of task slots available on the node
	 * 节点上有效的任务槽位的数量
	 */
	private final int numberOfSlots;

	/**
	 * A list of available slot positions
	 * 有效槽位位置的一个集合
	 */
	private final Queue<Integer> availableSlots;

	/**
	 * Allocated slots on this taskManager
	 * 这个 TaskManager 上的已分配槽位
	 */
	private final Set<Slot> allocatedSlots = new HashSet<Slot>();

	/**
	 * A listener to be notified upon new slot availability
	 * 在新的slot有效时需要通知的监听器
	 */
	private SlotAvailabilityListener slotAvailabilityListener;

	/**
	 * Time when last heat beat has been received from the task manager running on this taskManager.
	 * 从运行这个 TaskManager 的 TaskManager 那里收到的最新的心跳时间
	 */
	private volatile long lastReceivedHeartBeat = System.currentTimeMillis();

	/**
	 * Flag marking the instance as alive or as dead.
	 * 标记这个实例时存活还是死亡的标识
	 */
	private volatile boolean isDead;


	// --------------------------------------------------------------------------------------------

	/**
	 * Constructs an instance reflecting a registered TaskManager.
	 *
	 * @param taskManagerGateway The actor gateway to communicate with the remote instance
	 * @param location The remote connection where the task manager receives requests.
	 * @param id The id under which the taskManager is registered.
	 * @param resources The resources available on the machine.
	 * @param numberOfSlots The number of task slots offered by this taskManager.
	 */
	public Instance(
			TaskManagerGateway taskManagerGateway,
			TaskManagerLocation location,
			InstanceID id,
			HardwareDescription resources,
			int numberOfSlots) {
		this.taskManagerGateway = Preconditions.checkNotNull(taskManagerGateway);
		this.location = Preconditions.checkNotNull(location);
		this.instanceId = Preconditions.checkNotNull(id);
		this.resources = Preconditions.checkNotNull(resources);
		this.numberOfSlots = numberOfSlots;

		this.availableSlots = new ArrayDeque<>(numberOfSlots);
		for (int i = 0; i < numberOfSlots; i++) {
			this.availableSlots.add(i);
		}
	}

	// --------------------------------------------------------------------------------------------
	// Properties
	// 属性
	// --------------------------------------------------------------------------------------------

	public ResourceID getTaskManagerID() {
		return location.getResourceID();
	}

	public InstanceID getId() {
		return instanceId;
	}

	public HardwareDescription getResources() {
		return this.resources;
	}

	public int getTotalNumberOfSlots() {
		return numberOfSlots;
	}

	// --------------------------------------------------------------------------------------------
	// Life and Death
	// 存活和死亡
	// --------------------------------------------------------------------------------------------

	public boolean isAlive() {
		return !isDead;
	}

	public void markDead() {

		// create a copy of the slots to avoid concurrent modification exceptions
		// 创建 slots 的一个 copy，来避免并发修改异常
		List<Slot> slots;

		synchronized (instanceLock) {
			if (isDead) {
				return;
			}
			isDead = true;

			// no more notifications for the slot releasing
			// 不会再有 slot 释放的通知消息
			this.slotAvailabilityListener = null;

			slots = new ArrayList<Slot>(allocatedSlots);

			allocatedSlots.clear();
			availableSlots.clear();
		}

		/*
		 * releaseSlot must not own the instanceLock in order to avoid dead locks where a slot
		 * owning the assignment group lock wants to give itself back to the instance which requires
		 * the instance lock
		 * releaseSlot 方法必须不要占有 instanceLock ，以此来避免死锁，
		 * 比如一个 slot 占有了分配组的lock，想去把它自己归还给 instance，而intance有需要 instance lock
		 */
		for (Slot slot : slots) {
			slot.releaseSlot();
		}
	}


	// --------------------------------------------------------------------------------------------
	// Heartbeats
	// 心跳
	// --------------------------------------------------------------------------------------------

	/**
	 * Gets the timestamp of the last heartbeat.
	 * 获取上次心跳的时间戳
	 *
	 * @return The timestamp of the last heartbeat.
	 */
	public long getLastHeartBeat() {
		return this.lastReceivedHeartBeat;
	}

	/**
	 * Updates the time of last received heart beat to the current system time.
	 * 将上次收到心跳的事件变量更新为当前系统时间
	 */
	public void reportHeartBeat() {
		this.lastReceivedHeartBeat = System.currentTimeMillis();
	}

	/**
	 * Checks whether the last heartbeat occurred within the last {@code n} milliseconds
	 * before the given timestamp {@code now}.
	 * 检查上次心跳发生的时间，是否超时
	 *
	 * @param now The timestamp representing the current time.
	 *            当前时间
	 * @param cleanUpInterval The maximum time (in msecs) that the last heartbeat may lie in the past.
	 *                        从上次心跳可能还存活的最大时间，单位毫秒
	 * @return True, if this taskManager is considered alive, false otherwise.
	 */
	public boolean isStillAlive(long now, long cleanUpInterval) {
		return this.lastReceivedHeartBeat + cleanUpInterval > now;
	}

	// --------------------------------------------------------------------------------------------
	// Resource allocation
	// 资源分配
	// --------------------------------------------------------------------------------------------

	/**
	 * Allocates a simple slot on this TaskManager instance. This method returns {@code null}, if no slot
	 * is available at the moment.
	 * 在这个 TaskManager 实例上分配一个 SimpleSlot。
	 * 如果在当前时间没有slot是有效的，该方法会返回null
	 *
	 * @param jobID The ID of the job that the slot is allocated for.
	 *              slot 分配给的 job 的 id
	 *
	 * @return A simple slot that represents a task slot on this TaskManager instance, or null, if the
	 *         TaskManager instance has no more slots available.
	 *         一个 SimpleSlot ，表示这个 TaskManager 实例上的一个 task slot，
	 *         如果 TaskManager 实例上没有更多的有效槽位，则返回 null。
	 *
	 * @throws InstanceDiedException Thrown if the instance is no longer alive by the time the
	 *                               slot is allocated. 
	 */
	public SimpleSlot allocateSimpleSlot(JobID jobID) throws InstanceDiedException {
		if (jobID == null) {
			throw new IllegalArgumentException();
		}

		synchronized (instanceLock) {
			if (isDead) {
				throw new InstanceDiedException(this);
			}

			Integer nextSlot = availableSlots.poll();
			if (nextSlot == null) {
				return null;
			}
			else {
				SimpleSlot slot = new SimpleSlot(jobID, this, location, nextSlot, taskManagerGateway);
				allocatedSlots.add(slot);
				return slot;
			}
		}
	}

	/**
	 * Allocates a shared slot on this TaskManager instance. This method returns {@code null}, if no slot
	 * is available at the moment. The shared slot will be managed by the given  SlotSharingGroupAssignment.
	 * 在这个 TaskManager 实例上分配一个 SharedSlot 。
	 * 如果当前时间没有有效的 slot ，该方法会返回 null 。
	 * 这个 SharedSlot 由指定的 {@link SlotSharingGroupAssignment} 来管理。
	 *
	 * @param jobID The ID of the job that the slot is allocated for.
	 *              slot 将要分配给的 job 的 id
	 * @param sharingGroupAssignment The assignment group that manages this shared slot.
	 *                               管理这个 SharedSlot 的分配组
	 *
	 * @return A shared slot that represents a task slot on this TaskManager instance and can hold other
	 *         (shared) slots, or null, if the TaskManager instance has no more slots available.
	 *         SharedSlot 表示这个 TaskManager 实例上的一个 task slot，并且可以维护其他 slots(也可以是 SharedSlot)，
	 *         但如果 TaskManager 实例没有更多的有效的 slots 时， 则会返回null
	 *
	 * @throws InstanceDiedException Thrown if the instance is no longer alive by the time the slot is allocated.
	 * 									在 slot 分配时，如果实例不在 alive ，则抛出该异常
	 */
	public SharedSlot allocateSharedSlot(JobID jobID, SlotSharingGroupAssignment sharingGroupAssignment)
			throws InstanceDiedException
	{
		// the slot needs to be in the returned to taskManager state
		if (jobID == null) {
			throw new IllegalArgumentException();
		}

		synchronized (instanceLock) {
			if (isDead) {
				throw new InstanceDiedException(this);
			}

			Integer nextSlot = availableSlots.poll();
			if (nextSlot == null) {
				return null;
			}
			else {
				SharedSlot slot = new SharedSlot(
						jobID, this, location, nextSlot, taskManagerGateway, sharingGroupAssignment);
				allocatedSlots.add(slot);
				return slot;
			}
		}
	}

	/**
	 * Returns a slot that has been allocated from this instance. The slot needs have been canceled
	 * prior to calling this method.
	 * 返回一个从这个实例中分配出去的slot。
	 * 在调用该方法之前，slot需要已经被取消了
	 * 
	 * <p>The method will transition the slot to the "released" state. If the slot is already in state
	 * "released", this method will do nothing.</p>
	 * 这个方法会将slot的状态转换成 "release" 状态。
	 * 如果slot已经处于 "release" 状态，该返回不做任何操作。
	 * 
	 * @param slot The slot to return.
	 * @return True, if the slot was returned, false if not.
	 */
	@Override
	public boolean returnAllocatedSlot(Slot slot) {
		checkNotNull(slot);
		checkArgument(!slot.isAlive(), "slot is still alive");
		checkArgument(slot.getOwner() == this, "slot belongs to the wrong TaskManager.");

		/**
		 * 这里有两个地方会返回false，但是无法区分出是什么原因导致的返回false，是否有区分的必要？
		 */
		if (slot.markReleased()) {
			LOG.debug("Return allocated slot {}.", slot);
			synchronized (instanceLock) {
				if (isDead) {
					/**
					 * 这里返回false，是由于当前 TaskManager 实例，已经处于非活跃状态
					 */
					return false;
				}

				if (this.allocatedSlots.remove(slot)) {
					this.availableSlots.add(slot.getSlotNumber());

					if (this.slotAvailabilityListener != null) {
						this.slotAvailabilityListener.newSlotAvailable(this);
					}

					return true;
				}
				else {
					throw new IllegalArgumentException("Slot was not allocated from this TaskManager.");
				}
			}
		}
		else {
			/**
			 * 这里返回false，并不是有异常，而是slot的状态以及是"release"状态
			 */
			return false;
		}
	}

	public void cancelAndReleaseAllSlots() {
		// we need to do this copy because of concurrent modification exceptions
		List<Slot> copy;
		synchronized (instanceLock) {
			copy = new ArrayList<Slot>(this.allocatedSlots);
		}

		for (Slot slot : copy) {
			slot.releaseSlot();
		}
	}

	/**
	 * Returns the InstanceGateway of this Instance. This gateway can be used to communicate with
	 * it.
	 *
	 * @return InstanceGateway associated with this instance
	 */
	public TaskManagerGateway getTaskManagerGateway() {
		return taskManagerGateway;
	}

	public TaskManagerLocation getTaskManagerLocation() {
		return location;
	}

	public int getNumberOfAvailableSlots() {
		return this.availableSlots.size();
	}

	public int getNumberOfAllocatedSlots() {
		return this.allocatedSlots.size();
	}

	public boolean hasResourcesAvailable() {
		return !isDead && getNumberOfAvailableSlots() > 0;
	}

	// --------------------------------------------------------------------------------------------
	// Listeners
	// --------------------------------------------------------------------------------------------

	/**
	 * Sets the listener that receives notifications for slot availability.
	 * 设置监听器
	 * 
	 * @param slotAvailabilityListener The listener.
	 */
	public void setSlotAvailabilityListener(SlotAvailabilityListener slotAvailabilityListener) {
		synchronized (instanceLock) {
			if (this.slotAvailabilityListener != null) {
				throw new IllegalStateException("Instance has already a slot listener.");
			} else {
				this.slotAvailabilityListener = slotAvailabilityListener;
			}
		}
	}

	/**
	 * Removes the listener that receives notifications for slot availability.
	 * 移除监听器
	 */
	public void removeSlotListener() {
		synchronized (instanceLock) {
			this.slotAvailabilityListener = null;
		}
	}

	// --------------------------------------------------------------------------------------------
	// Standard Utilities
	// --------------------------------------------------------------------------------------------

	@Override
	public String toString() {
		return String.format("%s @ %s - %d slots - URL: %s", instanceId, location.getHostname(),
				numberOfSlots, (taskManagerGateway != null ? taskManagerGateway.getAddress() : "No instance gateway"));
	}
}
