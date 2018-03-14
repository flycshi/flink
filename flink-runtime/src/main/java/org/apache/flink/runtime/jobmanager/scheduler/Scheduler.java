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
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.instance.InstanceDiedException;
import org.apache.flink.runtime.instance.InstanceListener;
import org.apache.flink.runtime.instance.SharedSlot;
import org.apache.flink.runtime.instance.SimpleSlot;
import org.apache.flink.runtime.instance.SlotProvider;
import org.apache.flink.runtime.instance.SlotSharingGroupAssignment;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * The scheduler is responsible for distributing the ready-to-run tasks among instances and slots.
 * scheduler用来将 ready-to-run 任务在instances和slots之间进行分配。
 * 
 * <p>The scheduler supports two scheduling modes:</p>
 * scheduler 支持两种调度模式
 * <ul>
 *     <li>Immediate scheduling: A request for a task slot immediately returns a task slot, if one is
 *         available, or throws a {@link NoResourceAvailableException}.</li>
 *         立即调度: 如果有有效的任务槽位, 任务槽位的请求被立即返回一个任务槽位, 否则抛出异常
 *     <li>Queued Scheduling: A request for a task slot is queued and returns a future that will be
 *         fulfilled as soon as a slot becomes available.</li>
 *         排队调度: 任务槽位的请求会被加入队列, 并返回一个future, 只要一个槽位变的有效, 这个future就会被分配。
 * </ul>
 */
public class Scheduler implements InstanceListener, SlotAvailabilityListener, SlotProvider {

	/** Scheduler-wide logger */
	private static final Logger LOG = LoggerFactory.getLogger(Scheduler.class);
	
	
	/**
	 * All modifications to the scheduler structures are performed under a global scheduler lock
	 * 针对调度器结构的所有修改都在一个全局调度锁下执行
	 */
	private final Object globalLock = new Object();
	
	/**
	 * All instances that the scheduler can deploy to
	 * 调度器可以部署的所有实例(TaskManager)
	 */
	private final Set<Instance> allInstances = new HashSet<Instance>();
	
	/**
	 * All instances by hostname
	 * hostname -> Set<Instance>
	 * 一个主机上, 部署多个TaskManager实例 ?
	 */
	private final HashMap<String, Set<Instance>> allInstancesByHost = new HashMap<String, Set<Instance>>();
	
	/**
	 * All instances that still have available resources
	 * 仍然还有空闲有效资源的实例
	 */
	private final Map<ResourceID, Instance> instancesWithAvailableResources = new LinkedHashMap<>();
	
	/**
	 * All tasks pending to be scheduled
	 * 悬挂等待被调度的所有任务
	 */
	private final Queue<QueuedTask> taskQueue = new ArrayDeque<QueuedTask>();

	/**
	 * 新添加的Instance, 等到异步处理
	 */
	private final BlockingQueue<Instance> newlyAvailableInstances = new LinkedBlockingQueue<Instance>();
	
	/**
	 * The number of slot allocations that had no location preference
	 * 没有位置偏好的分配的槽的数量
	 */
	private int unconstrainedAssignments;

	/**
	 * The number of slot allocations where locality could be respected
	 * 指定位置分配的槽的数量
	 */
	private int localizedAssignments;

	/**
	 * The number of slot allocations where locality could not be respected
	 * 非指定位置的槽分配数量
	 */
	private int nonLocalizedAssignments;

	/**
	 * The Executor which is used to execute newSlotAvailable futures.
	 * 用来执行{@link #newSlotAvailable} futures 的执行器
	 */
	private final Executor executor;

	// ------------------------------------------------------------------------

	/**
	 * Creates a new scheduler.
	 * 创建一个新的调度器
	 */
	public Scheduler(Executor executor) {
		this.executor = Preconditions.checkNotNull(executor);
	}
	
	/**
	 * Shuts the scheduler down. After shut down no more tasks can be added to the scheduler.
	 * 关闭调度器。
	 * 关闭后, 就不能再向调度器添加任务了。
	 */
	public void shutdown() {
		// 先加锁
		synchronized (globalLock) {
			// 依次释放
			for (Instance i : allInstances) {
				i.removeSlotListener();
				i.cancelAndReleaseAllSlots();
			}
			allInstances.clear();
			allInstancesByHost.clear();
			instancesWithAvailableResources.clear();
			taskQueue.clear();
		}
	}

	// ------------------------------------------------------------------------
	//  Scheduling
	//  调度
	// ------------------------------------------------------------------------


	@Override
	public CompletableFuture<SimpleSlot> allocateSlot(
			ScheduledUnit task,
			boolean allowQueued,
			Collection<TaskManagerLocation> preferredLocations) {

		try {
			final Object ret = scheduleTask(task, allowQueued, preferredLocations);

			if (ret instanceof SimpleSlot) {
				// 如果返回的就是一个SimpleSlot, 则构建一个future
				return CompletableFuture.completedFuture((SimpleSlot) ret);
			}
			else if (ret instanceof CompletableFuture) {
				// 如果返回的就是future, 则强制类型转换后, 返回
				@SuppressWarnings("unchecked")
				CompletableFuture<SimpleSlot> typed = (CompletableFuture<SimpleSlot>) ret;
				return typed;
			}
			else {
				// this should never happen, simply guard this case with an exception
				// 这个分支应该从不会发生才对, 如果发生, 就抛出异常
				throw new RuntimeException();
			}
		} catch (NoResourceAvailableException e) {
			return FutureUtils.completedExceptionally(e);
		}
	}

	/**
	 * Returns either a {@link SimpleSlot}, or a {@link CompletableFuture}.
	 */
	private Object scheduleTask(ScheduledUnit task, boolean queueIfNoResource, Iterable<TaskManagerLocation> preferredLocations) throws NoResourceAvailableException {
		if (task == null) {
			throw new NullPointerException();
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("Scheduling task " + task);
		}

		final ExecutionVertex vertex = task.getTaskToExecute().getVertex();
		
		final boolean forceExternalLocation = false &&
									preferredLocations != null && preferredLocations.iterator().hasNext();
	
		synchronized (globalLock) {
			
			SlotSharingGroup sharingUnit = task.getSlotSharingGroup();
			
			if (sharingUnit != null) {

				// 1)  === If the task has a slot sharing group, schedule with shared slots ===
				
				if (queueIfNoResource) {
					throw new IllegalArgumentException(
							"A task with a vertex sharing group was scheduled in a queued fashion.");
				}
				
				final SlotSharingGroupAssignment assignment = sharingUnit.getTaskAssignment();
				final CoLocationConstraint constraint = task.getLocationConstraint();
				
				// sanity check that we do not use an externally forced location and a co-location constraint together
				if (constraint != null && forceExternalLocation) {
					throw new IllegalArgumentException("The scheduling cannot be constrained simultaneously by a "
							+ "co-location constraint and an external location constraint.");
				}
				
				// get a slot from the group, if the group has one for us (and can fulfill the constraint)
				final SimpleSlot slotFromGroup;
				if (constraint == null) {
					slotFromGroup = assignment.getSlotForTask(vertex.getJobvertexId(), preferredLocations);
				}
				else {
					slotFromGroup = assignment.getSlotForTask(constraint, preferredLocations);
				}

				SimpleSlot newSlot = null;
				SimpleSlot toUse = null;

				// the following needs to make sure any allocated slot is released in case of an error
				try {
					
					// check whether the slot from the group is already what we want.
					// any slot that is local, or where the assignment was unconstrained is good!
					if (slotFromGroup != null && slotFromGroup.getLocality() != Locality.NON_LOCAL) {
						
						// if this is the first slot for the co-location constraint, we lock
						// the location, because we are quite happy with the slot
						if (constraint != null && !constraint.isAssigned()) {
							constraint.lockLocation();
						}
						
						updateLocalityCounters(slotFromGroup, vertex);
						return slotFromGroup;
					}
					
					// the group did not have a local slot for us. see if we can one (or a better one)
					
					// our location preference is either determined by the location constraint, or by the
					// vertex's preferred locations
					final Iterable<TaskManagerLocation> locations;
					final boolean localOnly;
					if (constraint != null && constraint.isAssigned()) {
						locations = Collections.singleton(constraint.getLocation());
						localOnly = true;
					}
					else {
						locations = preferredLocations;
						localOnly = forceExternalLocation;
					}
					
					newSlot = getNewSlotForSharingGroup(vertex, locations, assignment, constraint, localOnly);

					if (newSlot == null) {
						if (slotFromGroup == null) {
							// both null, which means there is nothing available at all
							
							if (constraint != null && constraint.isAssigned()) {
								// nothing is available on the node where the co-location constraint forces us to
								throw new NoResourceAvailableException("Could not allocate a slot on instance " +
										constraint.getLocation() + ", as required by the co-location constraint.");
							}
							else if (forceExternalLocation) {
								// could not satisfy the external location constraint
								String hosts = getHostnamesFromInstances(preferredLocations);
								throw new NoResourceAvailableException("Could not schedule task " + vertex
										+ " to any of the required hosts: " + hosts);
							}
							else {
								// simply nothing is available
								throw new NoResourceAvailableException(task, getNumberOfAvailableInstances(),
										getTotalNumberOfSlots(), getNumberOfAvailableSlots());
							}
						}
						else {
							// got a non-local from the group, and no new one, so we use the non-local
							// slot from the sharing group
							toUse = slotFromGroup;
						}
					}
					else if (slotFromGroup == null || !slotFromGroup.isAlive() || newSlot.getLocality() == Locality.LOCAL) {
						// if there is no slot from the group, or the new slot is local,
						// then we use the new slot
						if (slotFromGroup != null) {
							slotFromGroup.releaseSlot();
						}
						toUse = newSlot;
					}
					else {
						// both are available and usable. neither is local. in that case, we may
						// as well use the slot from the sharing group, to minimize the number of
						// instances that the job occupies
						newSlot.releaseSlot();
						toUse = slotFromGroup;
					}

					// if this is the first slot for the co-location constraint, we lock
					// the location, because we are going to use that slot
					if (constraint != null && !constraint.isAssigned()) {
						constraint.lockLocation();
					}
					
					updateLocalityCounters(toUse, vertex);
				}
				catch (NoResourceAvailableException e) {
					throw e;
				}
				catch (Throwable t) {
					if (slotFromGroup != null) {
						slotFromGroup.releaseSlot();
					}
					if (newSlot != null) {
						newSlot.releaseSlot();
					}

					ExceptionUtils.rethrow(t, "An error occurred while allocating a slot in a sharing group");
				}

				return toUse;
			}
			else {
				
				// 2) === schedule without hints and sharing ===
				
				SimpleSlot slot = getFreeSlotForTask(vertex, preferredLocations, forceExternalLocation);
				if (slot != null) {
					updateLocalityCounters(slot, vertex);
					return slot;
				}
				else {
					// no resource available now, so queue the request
					if (queueIfNoResource) {
						CompletableFuture<SimpleSlot> future = new CompletableFuture<>();
						this.taskQueue.add(new QueuedTask(task, future));
						return future;
					}
					else if (forceExternalLocation) {
						String hosts = getHostnamesFromInstances(preferredLocations);
						throw new NoResourceAvailableException("Could not schedule task " + vertex
								+ " to any of the required hosts: " + hosts);
					}
					else {
						throw new NoResourceAvailableException(getNumberOfAvailableInstances(),
								getTotalNumberOfSlots(), getNumberOfAvailableSlots());
					}
				}
			}
		}
	}
	
	/**
	 * Gets a suitable instance to schedule the vertex execution to.
	 * <p>
	 * NOTE: This method does is not thread-safe, it needs to be synchronized by the caller.
	 * 
	 * @param vertex The task to run. 
	 * @return The instance to run the vertex on, it {@code null}, if no instance is available.
	 */
	protected SimpleSlot getFreeSlotForTask(ExecutionVertex vertex,
											Iterable<TaskManagerLocation> requestedLocations,
											boolean localOnly) {
		// we need potentially to loop multiple times, because there may be false positives
		// in the set-with-available-instances
		while (true) {
			Pair<Instance, Locality> instanceLocalityPair = findInstance(requestedLocations, localOnly);

			if (instanceLocalityPair == null){
				return null;
			}

			Instance instanceToUse = instanceLocalityPair.getLeft();
			Locality locality = instanceLocalityPair.getRight();

			try {
				SimpleSlot slot = instanceToUse.allocateSimpleSlot(vertex.getJobId());
				
				// if the instance has further available slots, re-add it to the set of available resources.
				if (instanceToUse.hasResourcesAvailable()) {
					this.instancesWithAvailableResources.put(instanceToUse.getTaskManagerID(), instanceToUse);
				}
				
				if (slot != null) {
					slot.setLocality(locality);
					return slot;
				}
			}
			catch (InstanceDiedException e) {
				// the instance died it has not yet been propagated to this scheduler
				// remove the instance from the set of available instances
				removeInstance(instanceToUse);
			}
			
			// if we failed to get a slot, fall through the loop
		}
	}

	/**
	 * Tries to allocate a new slot for a vertex that is part of a slot sharing group. If one
	 * of the instances has a slot available, the method will allocate it as a shared slot, add that
	 * shared slot to the sharing group, and allocate a simple slot from that shared slot.
	 * 
	 * <p>This method will try to allocate a slot from one of the local instances, and fall back to
	 * non-local instances, if permitted.</p>
	 * 
	 * @param vertex The vertex to allocate the slot for.
	 * @param requestedLocations The locations that are considered local. May be null or empty, if the
	 *                           vertex has no location preferences.
	 * @param groupAssignment The slot sharing group of the vertex. Mandatory parameter.
	 * @param constraint The co-location constraint of the vertex. May be null.
	 * @param localOnly Flag to indicate if non-local choices are acceptable.
	 * 
	 * @return A sub-slot for the given vertex, or {@code null}, if no slot is available.
	 */
	protected SimpleSlot getNewSlotForSharingGroup(ExecutionVertex vertex,
													Iterable<TaskManagerLocation> requestedLocations,
													SlotSharingGroupAssignment groupAssignment,
													CoLocationConstraint constraint,
													boolean localOnly)
	{
		// we need potentially to loop multiple times, because there may be false positives
		// in the set-with-available-instances
		while (true) {
			Pair<Instance, Locality> instanceLocalityPair = findInstance(requestedLocations, localOnly);
			
			if (instanceLocalityPair == null) {
				// nothing is available
				return null;
			}

			final Instance instanceToUse = instanceLocalityPair.getLeft();
			final Locality locality = instanceLocalityPair.getRight();

			try {
				JobVertexID groupID = vertex.getJobvertexId();
				
				// allocate a shared slot from the instance
				SharedSlot sharedSlot = instanceToUse.allocateSharedSlot(vertex.getJobId(), groupAssignment);

				// if the instance has further available slots, re-add it to the set of available resources.
				if (instanceToUse.hasResourcesAvailable()) {
					this.instancesWithAvailableResources.put(instanceToUse.getTaskManagerID(), instanceToUse);
				}

				if (sharedSlot != null) {
					// add the shared slot to the assignment group and allocate a sub-slot
					SimpleSlot slot = constraint == null ?
							groupAssignment.addSharedSlotAndAllocateSubSlot(sharedSlot, locality, groupID) :
							groupAssignment.addSharedSlotAndAllocateSubSlot(sharedSlot, locality, constraint);

					if (slot != null) {
						return slot;
					}
					else {
						// could not add and allocate the sub-slot, so release shared slot
						sharedSlot.releaseSlot();
					}
				}
			}
			catch (InstanceDiedException e) {
				// the instance died it has not yet been propagated to this scheduler
				// remove the instance from the set of available instances
				removeInstance(instanceToUse);
			}

			// if we failed to get a slot, fall through the loop
		}
	}

	/**
	 * Tries to find a requested instance. If no such instance is available it will return a non-
	 * local instance. If no such instance exists (all slots occupied), then return null.
	 * 
	 * <p><b>NOTE:</b> This method is not thread-safe, it needs to be synchronized by the caller.</p>
	 *
	 * @param requestedLocations The list of preferred instances. May be null or empty, which indicates that
	 *                           no locality preference exists.   
	 * @param localOnly Flag to indicate whether only one of the exact local instances can be chosen.  
	 */
	private Pair<Instance, Locality> findInstance(Iterable<TaskManagerLocation> requestedLocations, boolean localOnly) {
		
		// drain the queue of newly available instances
		while (this.newlyAvailableInstances.size() > 0) {
			Instance queuedInstance = this.newlyAvailableInstances.poll();
			if (queuedInstance != null) {
				this.instancesWithAvailableResources.put(queuedInstance.getTaskManagerID(), queuedInstance);
			}
		}
		
		// if nothing is available at all, return null
		if (this.instancesWithAvailableResources.isEmpty()) {
			return null;
		}

		Iterator<TaskManagerLocation> locations = requestedLocations == null ? null : requestedLocations.iterator();

		if (locations != null && locations.hasNext()) {
			// we have a locality preference

			while (locations.hasNext()) {
				TaskManagerLocation location = locations.next();
				if (location != null) {
					Instance instance = instancesWithAvailableResources.remove(location.getResourceID());
					if (instance != null) {
						return new ImmutablePair<Instance, Locality>(instance, Locality.LOCAL);
					}
				}
			}
			
			// no local instance available
			if (localOnly) {
				return null;
			}
			else {
				// take the first instance from the instances with resources
				Iterator<Instance> instances = instancesWithAvailableResources.values().iterator();
				Instance instanceToUse = instances.next();
				instances.remove();

				return new ImmutablePair<>(instanceToUse, Locality.NON_LOCAL);
			}
		}
		else {
			// no location preference, so use some instance
			Iterator<Instance> instances = instancesWithAvailableResources.values().iterator();
			Instance instanceToUse = instances.next();
			instances.remove();

			return new ImmutablePair<>(instanceToUse, Locality.UNCONSTRAINED);
		}
	}
	
	@Override
	public void newSlotAvailable(final Instance instance) {
		
		// WARNING: The asynchrony here is necessary, because  we cannot guarantee the order
		// of lock acquisition (global scheduler, instance) and otherwise lead to potential deadlocks:
		// 
		// -> The scheduler needs to grab them (1) global scheduler lock
		//                                     (2) slot/instance lock
		// -> The slot releasing grabs (1) slot/instance (for releasing) and
		//                             (2) scheduler (to check whether to take a new task item
		// 
		// that leads with a high probability to deadlocks, when scheduling fast
		/**
		 * 警告: 这里采用异步是必要的, 因为我们不能保障锁的获取顺序(global scheduler, instance), 然后可能会导致潜在的死锁:
		 *
		 * -> scheduler 需要占用 (1) global scheduler lock
		 *                      (2) slot/instance lock
		 *
		 * -> slot releasing 需要占用 (1) slot/instance (用于释放)
		 *                           (2) scheduler (检查是否有新任务)
		 *
		 * 当调度快时, 很容易导致死锁。
		 */

		newlyAvailableInstances.add(instance);

		executor.execute(new Runnable() {
			@Override
			public void run() {
				handleNewSlot();
			}
		});
	}
	
	private void handleNewSlot() {
		
		synchronized (globalLock) {
			Instance instance = this.newlyAvailableInstances.poll();

			/** 如果从队列中拿到的元素为null, 或者instance中没有有效的资源, 即槽位, 则结束本次执行 */
			if (instance == null || !instance.hasResourcesAvailable()) {
				// someone else took it
				return;
			}

			/** 从等待执行的任务队列中, 拿出head处的任务, 这里还没有从taskQueue这个队列中删除 */
			QueuedTask queued = taskQueue.peek();
			
			// the slot was properly released, we can allocate a new one from that instance
			/** 槽位可能被释放了, 我们可以从那个instance分配一个新的槽位 */
			
			if (queued != null) {
				ScheduledUnit task = queued.getTask();
				ExecutionVertex vertex = task.getTaskToExecute().getVertex();
				
				try {
					SimpleSlot newSlot = instance.allocateSimpleSlot(vertex.getJobId());
					if (newSlot != null) {
						
						// success, remove from the task queue and notify the future
						taskQueue.poll();
						if (queued.getFuture() != null) {
							try {
								queued.getFuture().complete(newSlot);
							}
							catch (Throwable t) {
								LOG.error("Error calling allocation future for task " + vertex.getTaskNameWithSubtaskIndex(), t);
								task.getTaskToExecute().fail(t);
							}
						}
					}
				}
				catch (InstanceDiedException e) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("Instance " + instance + " was marked dead asynchronously.");
					}
					
					removeInstance(instance);
				}
			}
			else {
				/** 如果没有新的任务需要分配操作, 则将这个instance记录在可用资源map中 */
				this.instancesWithAvailableResources.put(instance.getTaskManagerID(), instance);
			}
		}
	}
	
	private void updateLocalityCounters(SimpleSlot slot, ExecutionVertex vertex) {
		Locality locality = slot.getLocality();

		switch (locality) {
		case UNCONSTRAINED:
			this.unconstrainedAssignments++;
			break;
		case LOCAL:
			this.localizedAssignments++;
			break;
		case NON_LOCAL:
			this.nonLocalizedAssignments++;
			break;
		default:
			throw new RuntimeException(locality.name());
		}
		
		if (LOG.isDebugEnabled()) {
			switch (locality) {
				case UNCONSTRAINED:
					LOG.debug("Unconstrained assignment: " + vertex.getTaskNameWithSubtaskIndex() + " --> " + slot);
					break;
				case LOCAL:
					LOG.debug("Local assignment: " + vertex.getTaskNameWithSubtaskIndex() + " --> " + slot);
					break;
				case NON_LOCAL:
					LOG.debug("Non-local assignment: " + vertex.getTaskNameWithSubtaskIndex() + " --> " + slot);
					break;
			}
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//  Instance Availability
	// --------------------------------------------------------------------------------------------
	
	@Override
	public void newInstanceAvailable(Instance instance) {
		if (instance == null) {
			throw new IllegalArgumentException();
		}
		if (instance.getNumberOfAvailableSlots() <= 0) {
			throw new IllegalArgumentException("The given instance has no resources.");
		}
		if (!instance.isAlive()) {
			throw new IllegalArgumentException("The instance is not alive.");
		}
		
		// synchronize globally for instance changes
		synchronized (this.globalLock) {
			
			// check we do not already use this instance
			if (!this.allInstances.add(instance)) {
				throw new IllegalArgumentException("The instance is already contained.");
			}
			
			try {
				// make sure we get notifications about slots becoming available
				instance.setSlotAvailabilityListener(this);
				
				// store the instance in the by-host-lookup
				String instanceHostName = instance.getTaskManagerLocation().getHostname();
				Set<Instance> instanceSet = allInstancesByHost.get(instanceHostName);
				if (instanceSet == null) {
					instanceSet = new HashSet<Instance>();
					allInstancesByHost.put(instanceHostName, instanceSet);
				}
				instanceSet.add(instance);

				// add it to the available resources and let potential waiters know
				this.instancesWithAvailableResources.put(instance.getTaskManagerID(), instance);

				// add all slots as available
				for (int i = 0; i < instance.getNumberOfAvailableSlots(); i++) {
					newSlotAvailable(instance);
				}
			}
			catch (Throwable t) {
				LOG.error("Scheduler could not add new instance " + instance, t);
				removeInstance(instance);
			}
		}
	}
	
	@Override
	public void instanceDied(Instance instance) {
		if (instance == null) {
			throw new IllegalArgumentException();
		}
		
		instance.markDead();
		
		// we only remove the instance from the pools, we do not care about the 
		synchronized (this.globalLock) {
			// the instance must not be available anywhere any more
			removeInstance(instance);
		}
	}
	
	private void removeInstance(Instance instance) {
		if (instance == null) {
			throw new NullPointerException();
		}

		/** 从{@code Instance}实例集合中, 删除当前传入的这个实例 */
		allInstances.remove(instance);
		instancesWithAvailableResources.remove(instance.getTaskManagerID());

		String instanceHostName = instance.getTaskManagerLocation().getHostname();
		Set<Instance> instanceSet = allInstancesByHost.get(instanceHostName);
		if (instanceSet != null) {
			instanceSet.remove(instance);
			if (instanceSet.isEmpty()) {
				allInstancesByHost.remove(instanceHostName);
			}
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//  Status reporting
	// --------------------------------------------------------------------------------------------

	/**
	 *
	 * NOTE: In the presence of multi-threaded operations, this number may be inexact.
	 *
	 * @return The number of empty slots, for tasks.
	 */
	public int getNumberOfAvailableSlots() {
		int count = 0;

		synchronized (globalLock) {
			processNewlyAvailableInstances();

			for (Instance instance : instancesWithAvailableResources.values()) {
				count += instance.getNumberOfAvailableSlots();
			}
		}

		return count;
	}

	public int getTotalNumberOfSlots() {
		int count = 0;

		synchronized (globalLock) {
			for (Instance instance : allInstances) {
				if (instance.isAlive()) {
					count += instance.getTotalNumberOfSlots();
				}
			}
		}

		return count;
	}

	public int getNumberOfAvailableInstances() {
		int numberAvailableInstances = 0;
		synchronized (this.globalLock) {
			for (Instance instance: allInstances ){
				if (instance.isAlive()){
					numberAvailableInstances++;
				}
			}
		}

		return numberAvailableInstances;
	}
	
	public int getNumberOfInstancesWithAvailableSlots() {
		synchronized (globalLock) {
			processNewlyAvailableInstances();

			return instancesWithAvailableResources.size();
		}
	}
	
	public Map<String, List<Instance>> getInstancesByHost() {
		synchronized (globalLock) {
			HashMap<String, List<Instance>> copy = new HashMap<String, List<Instance>>();
			
			for (Map.Entry<String, Set<Instance>> entry : allInstancesByHost.entrySet()) {
				copy.put(entry.getKey(), new ArrayList<Instance>(entry.getValue()));
			}
			return copy;
		}
	}
	
	public int getNumberOfUnconstrainedAssignments() {
		return unconstrainedAssignments;
	}
	
	public int getNumberOfLocalizedAssignments() {
		return localizedAssignments;
	}
	
	public int getNumberOfNonLocalizedAssignments() {
		return nonLocalizedAssignments;
	}
	
	// --------------------------------------------------------------------------------------------

	private void processNewlyAvailableInstances() {
		synchronized (globalLock) {
			Instance instance;

			while ((instance = newlyAvailableInstances.poll()) != null) {
				if (instance.hasResourcesAvailable()) {
					instancesWithAvailableResources.put(instance.getTaskManagerID(), instance);
				}
			}
		}
	}


	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private static String getHostnamesFromInstances(Iterable<TaskManagerLocation> locations) {
		StringBuilder bld = new StringBuilder();

		boolean successive = false;
		for (TaskManagerLocation loc : locations) {
			if (successive) {
				bld.append(", ");
			} else {
				successive = true;
			}
			bld.append(loc.getHostname());
		}

		return bld.toString();
	}
	
	// ------------------------------------------------------------------------
	//  Nested members
	// ------------------------------------------------------------------------

	/**
	 * An entry in the queue of schedule requests. Contains the task to be scheduled and
	 * the future that tracks the completion.
	 * 调度请求队列中的一个实例。
	 * 包含等待调度的任务, 以及跟踪完成情况的future。
	 */
	private static final class QueuedTask {
		
		private final ScheduledUnit task;
		
		private final CompletableFuture<SimpleSlot> future;
		
		
		public QueuedTask(ScheduledUnit task, CompletableFuture<SimpleSlot> future) {
			this.task = task;
			this.future = future;
		}

		public ScheduledUnit getTask() {
			return task;
		}

		public CompletableFuture<SimpleSlot> getFuture() {
			return future;
		}
	}
}
