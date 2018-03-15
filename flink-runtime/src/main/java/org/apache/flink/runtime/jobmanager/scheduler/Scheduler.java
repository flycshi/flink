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


	/**
	 * 为一个task分配一个slot
	 *
	 * @param task					需要分配槽位的task
	 * @param allowQueued			表示无法立即分配slot时，是否可以将入队列，稍后再分配
	 * @param preferredLocations	表示在进行slot分配时，优先考虑slot所在的TaskManager的位置集合
	 * @return
	 */
	@Override
	public CompletableFuture<SimpleSlot> allocateSlot(
			ScheduledUnit task,
			boolean allowQueued,
			Collection<TaskManagerLocation> preferredLocations) {

		try {
			/**
			 * 分配slot
			 * 1）有"槽位共享组"，则从组内找出满足要求的；
			 * 2）没有"槽位共享组"
 			 */
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

		/**
		 * 这个值，在这个表达式里，只能为false
		 * 这个变量的含义是：是否强制要求分配的slot所在的TaskManager，一定要在preferredLocations这个集合中。
		 */
		final boolean forceExternalLocation = false &&
									preferredLocations != null && preferredLocations.iterator().hasNext();
	
		synchronized (globalLock) {
			
			SlotSharingGroup sharingUnit = task.getSlotSharingGroup();
			
			if (sharingUnit != null) {

				// 1)  === If the task has a slot sharing group, schedule with shared slots ===
				// 1)  === 如果任务有一个槽位共享组，在共享槽中调度 ===

				/** 拥有一个"节点共享槽位的组"的task，是不可能入队等待分配slot的 */
				if (queueIfNoResource) {
					throw new IllegalArgumentException(
							"A task with a vertex sharing group was scheduled in a queued fashion.");
				}

				// 获取"槽位共享分组分配器"
				final SlotSharingGroupAssignment assignment = sharingUnit.getTaskAssignment();
				// 获取"位置协调约束器"
				final CoLocationConstraint constraint = task.getLocationConstraint();
				
				// sanity check that we do not use an externally forced location and a co-location constraint together
				/**
				 * 检查，我们没有将 外部强制位置 与 co-location约束 同时使用
				 * 既要求满足"外部强制位置"，又要求有"位置协调约束"，这两者很容易冲突，所以直接禁止这两种条件同时发生
				 */
				if (constraint != null && forceExternalLocation) {
					throw new IllegalArgumentException("The scheduling cannot be constrained simultaneously by a "
							+ "co-location constraint and an external location constraint.");
				}
				
				// get a slot from the group, if the group has one for us (and can fulfill the constraint)
				// 如果group中有满足要求的，从group中获取一个slot，
				final SimpleSlot slotFromGroup;
				if (constraint == null) {
					/**
					 * 没有"位置协调约束"的要求
					 * 根据JobVertexID，从槽共享组中，为task分配一个slot */
					slotFromGroup = assignment.getSlotForTask(vertex.getJobvertexId(), preferredLocations);
				}
				else {
					slotFromGroup = assignment.getSlotForTask(constraint, preferredLocations);
				}

				SimpleSlot newSlot = null;
				SimpleSlot toUse = null;

				// the following needs to make sure any allocated slot is released in case of an error
				// 接下来的操作需要确保在发生error时，任何已经被分配的slot会被释放掉
				try {
					
					// check whether the slot from the group is already what we want.
					// any slot that is local, or where the assignment was unconstrained is good!
					/**
					 * 检查从group获取的slot是否是我们想要的。
					 * 任何local的，或者没有约束的，都是good
					 */
					if (slotFromGroup != null && slotFromGroup.getLocality() != Locality.NON_LOCAL) {
						
						// if this is the first slot for the co-location constraint, we lock
						// the location, because we are quite happy with the slot
						/**
						 * 如果这是co-location被分配的第一个slot，那我们锁定location，因为我们对这个slot很满意
						 */
						if (constraint != null && !constraint.isAssigned()) {
							constraint.lockLocation();
						}
						
						updateLocalityCounters(slotFromGroup, vertex);
						return slotFromGroup;
					}
					
					// the group did not have a local slot for us. see if we can one (or a better one)
					// 如果group没有能提供一个local的，我们看看是否可以获得一个更好的
					
					// our location preference is either determined by the location constraint, or by the
					// vertex's preferred locations
					// 我们优先考虑的位置，要么是由location constraint确定，要么由节点的preferred locations决定
					final Iterable<TaskManagerLocation> locations;
					final boolean localOnly;
					// 这里用来确定优先考虑进行slot分配的TaskManager
					if (constraint != null && constraint.isAssigned()) {
						locations = Collections.singleton(constraint.getLocation());
						localOnly = true;
					}
					else {
						locations = preferredLocations;
						localOnly = forceExternalLocation;
					}

					/** 这里, 一般刚开始分配是, 共享槽中是没有可用slot的, 这是需要新分配, 并添加到共享槽中 */
					newSlot = getNewSlotForSharingGroup(vertex, locations, assignment, constraint, localOnly);

					/**
					 * 1) newSlot为null
					 * 		1.1）slotFromGroup也为null，这种只能根据具体的原因抛出异常了
					 * 		1.2）slotFromGroup不为null，则只能用slotFromGroup了
					 *
					 * 2）newSlot不为null
					 * 		2.1）slotFromGroup为null，或者挂了，或者newSlot是local的，则使用newSlot
					 * 		2.2）slotFromGroup也是杠杠的，那就优先使用slotFromGroup，以此来减少可能占用的TaskManager的数量
					 */
					if (newSlot == null) {
						if (slotFromGroup == null) {
							// both null, which means there is nothing available at all
							// 都是null，意味着没有有效的
							
							if (constraint != null && constraint.isAssigned()) {
								// nothing is available on the node where the co-location constraint forces us to
								// 在 co-location约束 强制我们的那个node上，没有有效的资源
								throw new NoResourceAvailableException("Could not allocate a slot on instance " +
										constraint.getLocation() + ", as required by the co-location constraint.");
							}
							else if (forceExternalLocation) {
								// could not satisfy the external location constraint
								// 在外部听的位置约束中，无法满足
								String hosts = getHostnamesFromInstances(preferredLocations);
								throw new NoResourceAvailableException("Could not schedule task " + vertex
										+ " to any of the required hosts: " + hosts);
							}
							else {
								// simply nothing is available
								// 就是没有有效资源了
								throw new NoResourceAvailableException(task, getNumberOfAvailableInstances(),
										getTotalNumberOfSlots(), getNumberOfAvailableSlots());
							}
						}
						else {
							// got a non-local from the group, and no new one, so we use the non-local
							// slot from the sharing group
							// 从group中获取到了一个 non-local，并且没有新的了，所以我们就使用这个 non-local 了
							toUse = slotFromGroup;
						}
					}
					else if (slotFromGroup == null || !slotFromGroup.isAlive() || newSlot.getLocality() == Locality.LOCAL) {
						// if there is no slot from the group, or the new slot is local,
						// then we use the new slot
						// 如果group中没有slot了，或者新的slot是local的，那么就使用这个新的
						if (slotFromGroup != null) {
							slotFromGroup.releaseSlot();
						}
						toUse = newSlot;
					}
					else {
						// both are available and usable. neither is local. in that case, we may
						// as well use the slot from the sharing group, to minimize the number of
						// instances that the job occupies
						// 两边都获取到可用的，都不是local的，在这种情况下，我们还是选用从group获取的，以便减少这个job占用的instances的个数
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
				// 2) === 没有提示和共享的调度 ===

				/**
				 * 优先从preferredLocations集合中找出满足要求的实例分配slot，
				 * 如果没有，且forceExternalLocation为false，则从已有的instance有效集合中拿出第一个分配slot
				 */
				SimpleSlot slot = getFreeSlotForTask(vertex, preferredLocations, forceExternalLocation);
				if (slot != null) {
					updateLocalityCounters(slot, vertex);
					return slot;
				}
				else {
					// no resource available now, so queue the request
					// 目前没有有效的资源，所有将请求入队列
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
	 * 获取一个合适的instance来调度执行vertex
	 * <p>
	 * NOTE: This method does is not thread-safe, it needs to be synchronized by the caller.
	 * 注意：这个方法是非线程安全的，需要调用者进行synchronized操作
	 * 
	 * @param vertex The task to run. 
	 * @return The instance to run the vertex on, it {@code null}, if no instance is available.
	 */
	protected SimpleSlot getFreeSlotForTask(ExecutionVertex vertex,
											Iterable<TaskManagerLocation> requestedLocations,
											boolean localOnly) {
		// we need potentially to loop multiple times, because there may be false positives
		// in the set-with-available-instances
		/** 可能需要多次循环，因为有效instances集合中，可能存在无效的实例 */
		while (true) {
			/**
			 * 优先在指定的{@link TaskManager}实例列表查找合适的，
			 * 如果查找不到，在根据参数{@link localOnly}决定是否可以分配其他{@code TaskManager}实例
			 */
			Pair<Instance, Locality> instanceLocalityPair = findInstance(requestedLocations, localOnly);

			/** 如果找不到合适的{@code TaskManager}实例，则返回 null */
			if (instanceLocalityPair == null){
				return null;
			}

			Instance instanceToUse = instanceLocalityPair.getLeft();
			Locality locality = instanceLocalityPair.getRight();

			try {
				SimpleSlot slot = instanceToUse.allocateSimpleSlot(vertex.getJobId());
				
				// if the instance has further available slots, re-add it to the set of available resources.
				// 如果被使用的这个instance还有有效的slots，则添加到有效instances池中
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
				/** instance已经died，不能被scheduler所用，将其从有效instatnce集合中移除 */
				removeInstance(instanceToUse);
			}
			
			// if we failed to get a slot, fall through the loop
			// 如果获取slot失败，就是一次失败的循环
		}
	}

	/**
	 * Tries to allocate a new slot for a vertex that is part of a slot sharing group. If one
	 * of the instances has a slot available, the method will allocate it as a shared slot, add that
	 * shared slot to the sharing group, and allocate a simple slot from that shared slot.
	 * 尝试为一个"槽位共享组"中的一个"JobVertexID"对应的节点分配一个新的slot。
	 * 如果其中一个instance有一个有效的slot，该方法会分配一个 SharedSlot，并将其添加到组内，并且从这个SharedSlot中分配出一个SimpleSlot。
	 * 
	 * <p>This method will try to allocate a slot from one of the local instances, and fall back to
	 * non-local instances, if permitted.</p>
	 * 这个方法会尝试从其中一个local实例中分配一个slot，如果允许的化，那会以一个non-local作为后备
	 *
	 * 这里的local和no-local的含义
	 * local —— 分配slot的Instance是提供的Instance列表中的一个， requestedLocations
	 * non-local —— 非local
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
		// 可能多次循环
		while (true) {
			Pair<Instance, Locality> instanceLocalityPair = findInstance(requestedLocations, localOnly);
			
			if (instanceLocalityPair == null) {
				// nothing is available
				// 没有有效的，则直接返回null了
				return null;
			}

			final Instance instanceToUse = instanceLocalityPair.getLeft();
			final Locality locality = instanceLocalityPair.getRight();

			try {
				JobVertexID groupID = vertex.getJobvertexId();
				
				// allocate a shared slot from the instance
				// 从instance中分配出一个SharedSlot
				SharedSlot sharedSlot = instanceToUse.allocateSharedSlot(vertex.getJobId(), groupAssignment);

				// if the instance has further available slots, re-add it to the set of available resources.
				// 如果instance还有更多的资源，则重新加入到有效集合中
				if (instanceToUse.hasResourcesAvailable()) {
					this.instancesWithAvailableResources.put(instanceToUse.getTaskManagerID(), instanceToUse);
				}

				if (sharedSlot != null) {
					// add the shared slot to the assignment group and allocate a sub-slot
					// 添加SharedSlot到分配组中，并分配一个sub-slot
					SimpleSlot slot = constraint == null ?
							groupAssignment.addSharedSlotAndAllocateSubSlot(sharedSlot, locality, groupID) :
							groupAssignment.addSharedSlotAndAllocateSubSlot(sharedSlot, locality, constraint);

					if (slot != null) {
						return slot;
					}
					else {
						// could not add and allocate the sub-slot, so release shared slot
						// 不能添加并分配sub-slot，则释放这个SharedSlot
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
	 * 尝试发现一个请求的实例。
	 * 如果没有这样的实例存在，将会返回一个本地实例。
	 * 如果所有的都不存在，则返回null
	 * 
	 * <p><b>NOTE:</b> This method is not thread-safe, it needs to be synchronized by the caller.</p>
	 *
	 * @param requestedLocations The list of preferred instances. May be null or empty, which indicates that
	 *                           no locality preference exists.   
	 * @param localOnly Flag to indicate whether only one of the exact local instances can be chosen.
	 *
	 * <p>
	 * 1) 如果入参requestedLocations不为空
	 *    1.1) 优先从入参提供的TaskManagerLocation的集合requestedLocations中, 找出一个还有有效资源的TaskManager对应的Instance, 并且为 LOCAL
	 *    1.2) 上述步骤没有找到, 则localOnly的值决定后续操作
	 *         1.2.1) 如果localOnly为true, 则此时只能返回null了
	 *         1.2.2) 如果localOnly为false, 则还可以从{@code instancesWithAvailableResources}中拿出第一个, 为 NON_LOCAL
	 * 2) 如果入参requestedLocations为空, 则从{@code instancesWithAvailableResources}中拿出第一个, 为 UNCONSTRAINED
	 *
	 */
	private Pair<Instance, Locality> findInstance(Iterable<TaskManagerLocation> requestedLocations, boolean localOnly) {
		
		// drain the queue of newly available instances
		// 先处理完{@link newlyAvailableInstances}中的数据
		while (this.newlyAvailableInstances.size() > 0) {
			Instance queuedInstance = this.newlyAvailableInstances.poll();
			if (queuedInstance != null) {
				this.instancesWithAvailableResources.put(queuedInstance.getTaskManagerID(), queuedInstance);
			}
		}
		
		// if nothing is available at all, return null
		// 如果压根就没有有效的，那直接返回null
		if (this.instancesWithAvailableResources.isEmpty()) {
			return null;
		}

		Iterator<TaskManagerLocation> locations = requestedLocations == null ? null : requestedLocations.iterator();

		if (locations != null && locations.hasNext()) {
			// we have a locality preference
			// 我们有一个偏好的位置

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
			// 要求的{@code TaskManager}实例，都没有有效的状态
			if (localOnly) {
				return null;
			}
			else {
				// take the first instance from the instances with resources
				// 返回有资源的instance列中的第一个
				Iterator<Instance> instances = instancesWithAvailableResources.values().iterator();
				Instance instanceToUse = instances.next();
				instances.remove();

				return new ImmutablePair<>(instanceToUse, Locality.NON_LOCAL);
			}
		}
		else {
			// no location preference, so use some instance
			// 没有偏好位置，那就使用一些instance
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
						// 分配成功，从任务队列中移除这个task，通知futre
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
		/** 首先对{@code Instance}实例进行一些校验，如拥有的槽位数要大于0，实例是存活的 */
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
		// instance的变化，需要全局锁
		synchronized (this.globalLock) {
			
			// check we do not already use this instance
			// 检查这个instance还没有被注册过
			if (!this.allInstances.add(instance)) {
				throw new IllegalArgumentException("The instance is already contained.");
			}
			
			try {
				// make sure we get notifications about slots becoming available
				// 确保有有效槽位时可以获得通知，这个监听器
				instance.setSlotAvailabilityListener(this);
				
				// store the instance in the by-host-lookup
				// 保存instance
				String instanceHostName = instance.getTaskManagerLocation().getHostname();
				Set<Instance> instanceSet = allInstancesByHost.get(instanceHostName);
				if (instanceSet == null) {
					instanceSet = new HashSet<Instance>();
					allInstancesByHost.put(instanceHostName, instanceSet);
				}
				instanceSet.add(instance);

				// add it to the available resources and let potential waiters know
				// 添加到有效资源池中
				this.instancesWithAvailableResources.put(instance.getTaskManagerID(), instance);

				// add all slots as available
				// 添加到有效槽位池中
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
		// 从池中移除这个{@code Instance}
		synchronized (this.globalLock) {
			// the instance must not be available anywhere any more
			// 这个实例在任何地方都不能再有效了
			removeInstance(instance);
		}
	}

	/** 将一个{@code TaskManager}实例从{@code Scheduler}中移除 */
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
	//  状态报告
	// --------------------------------------------------------------------------------------------

	/**
	 *
	 * NOTE: In the presence of multi-threaded operations, this number may be inexact.
	 * 注意：在多线程操作的情况下，这个数字可能是不精确的。
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

	/** 获取所有注册的{@code TaskManager}实例上的总的{@code Slot}的个数 */
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

	/** 获取所有有效{@code TaskManager}实例的个数 */
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

	/** 获取还有有效{@code Slot}的{@code TaskManager}实例的个数 */
	public int getNumberOfInstancesWithAvailableSlots() {
		synchronized (globalLock) {
			processNewlyAvailableInstances();

			return instancesWithAvailableResources.size();
		}
	}

	/** 获取{@link allInstancesByHost}的一个拷贝 */
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

	/** 将{@link newlyAvailableInstances}这个队列中的元素添加到{@link instancesWithAvailableResources}这个map中 */
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
	//  工具
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
	//  内嵌成员
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
