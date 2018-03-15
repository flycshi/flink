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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationConstraint;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.AbstractID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


/**
 * The SlotSharingGroupAssignment manages a set of shared slots, which are shared between
 * tasks of a {@link org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup}.
 * SlotSharingGroupAssignment 管理着一个 SharedSlot 集合，其在一个 SlotSharingGroup 的任务之间共享
 * 
 * <p>The assignments shares tasks by allowing a shared slot to hold one vertex per
 * JobVertexID. For example, consider a program consisting of job vertices "source", "map",
 * "reduce", and "sink". If the slot sharing group spans all four job vertices, then
 * each shared slot can hold one parallel subtask of the source, the map, the reduce, and the
 * sink vertex. Each shared slot holds the actual subtasks in child slots, which are (at the leaf level),
 * the {@link SimpleSlot}s.</p>
 * assignments 是通过允许一个 SharedSlot 中持有每个 JobVertexID 的一个vertex 来实现共享 tasks 的。
 * 比如:
 * 一个程序包含这些 JobVertex, "source", "map", "reduce", and "sink" 。
 * 如果 SlotShareGroup 贯穿4个节点, 那么每一个 SharedSlot 可以持有 "source", "map", "reduce", and "sink" 这些节点的一个并行的子任务。
 * 每个 SharedSlot 真正持有子任务的是叶节点 SimpleSlot 。
 * 
 * <p>An exception are the co-location-constraints, that define that the i-th subtask of one
 * vertex needs to be scheduled strictly together with the i-th subtasks of the vertices
 * that share the co-location-constraint. To manage that, a co-location-constraint gets its
 * own shared slot inside the shared slots of a sharing group.</p>
 * 一个例外是共存约束(co-location-constraints),
 * 它要求一个节点的第i个子任务, 与它共享共存约束的节点的第i个子任务, 需要严格的安排在一起调度。
 * 为了实现共功能, 一个共享约束在一个 SlotSharedGroup 中的 SharedSlot 中获取属于自己的 SharedSlot
 * 
 * <p>Consider a job set up like this:</p>
 * 		一个job的配置如下:
 * 
 * <pre>{@code
 * +-------------- Slot Sharing Group --------------+
 * |                                                |
 * |            +-- Co Location Group --+           |
 * |            |                       |           |
 * |  (source) ---> (head) ---> (tail) ---> (sink)  |
 * |            |                       |           |
 * |            +-----------------------+           |
 * +------------------------------------------------+
 * }</pre>
 * 
 * <p>The slot hierarchy in the slot sharing group will look like the following</p>
 * 		SlotSharedGroup 中 slot 的层级如下所示:
 * 
 * <pre>
 *     Shared(0)(root)
 *        |
 *        +-- Simple(2)(sink)
 *        |
 *        +-- Shared(1)(co-location-group)
 *        |      |
 *        |      +-- Simple(0)(tail)
 *        |      +-- Simple(1)(head)
 *        |
 *        +-- Simple(0)(source)
 * </pre>
 */
public class SlotSharingGroupAssignment {

	private final static Logger LOG = LoggerFactory.getLogger(SlotSharingGroupAssignment.class);

	/**
	 * The lock globally guards against concurrent modifications in the data structures
	 * 全局保障数据结构中的并发修改的lock
	 */
	private final Object lock = new Object();
	
	/**
	 * All slots currently allocated to this sharing group
	 * 当前已经分配给共享组的所有slots
	 */
	private final Set<SharedSlot> allSlots = new LinkedHashSet<SharedSlot>();

	/**
	 * The slots available per vertex type (JobVertexId), keyed by TaskManager, to make them locatable
	 * 每个节点类型的的有效slot
	 * 以 TaskManager 为键, 为了让他们可定位
	 * [JobVertexID -> [ResourceID -> List<SharedSlot>]]
	 * 每个JobVertexID，都有对应的资源分配，而分配给它的资源，可能在不同的TaskManager上
	 * 每个TaskManager都有一个ResourceID，且每个TaskManager上可能会给JobVertexID分配多个SharedSlot
	 *
	 * 该变量的含义：一个JobVertexID，可以使用的{@link SharedSlot}，
	 * 这些{@link SharedSlot}按照其归属的{@code TaskManager}划分到不同的列表中，其中{@code TaskManager}用{@link ResourceID}来表示
	 */
	private final Map<AbstractID, Map<ResourceID, List<SharedSlot>>> availableSlotsPerJid = new LinkedHashMap<>();


	// --------------------------------------------------------------------------------------------
	//  Accounting
	//  计数
	// --------------------------------------------------------------------------------------------

	/**
	 * Gets the number of slots that are currently governed by this assignment group.
	 * This refers to the slots allocated from an {@link org.apache.flink.runtime.instance.Instance},
	 * and not the sub-slots given out as children of those shared slots.
	 * 获取这个分配组当前管理的slots的数量。
	 * 这是从{@link Instance}分配的slot, 而不是这些共享槽的子槽。
	 * 
	 * @return The number of resource slots managed by this assignment group.
	 */
	public int getNumberOfSlots() {
		return allSlots.size();
	}

	/**
	 * Gets the number of shared slots into which the given group can place subtasks or 
	 * nested task groups.
	 * 获取指定group可以放置子任务或者内嵌任务组的 SharedSlot 的数量。
	 * 
	 * @param groupId The ID of the group.
	 * @return The number of shared slots available to the given job vertex.
	 */
	public int getNumberOfAvailableSlotsForGroup(AbstractID groupId) {
		synchronized (lock) {
			Map<ResourceID, List<SharedSlot>> available = availableSlotsPerJid.get(groupId);

			if (available != null) {
				Set<SharedSlot> set = new HashSet<SharedSlot>();

				for (List<SharedSlot> list : available.values()) {
					for (SharedSlot slot : list) {
						set.add(slot);
					}
				}

				return set.size();
			}
			else {
				// if no entry exists for a JobVertexID so far, then the vertex with that ID can
				// add a subtask into each shared slot of this group. Consequently, all
				// of them are available for that JobVertexID.
				/**
				 * 如果对一个 JobVertexID 至今还没有一个入口, 那ID对应的vertex可以添加一个子任务到该组中的每个 SharedSlot 。
				 * 所以也就是说, 对这个 JobVertexID , 他们都是有效的。
				 */
				return allSlots.size();
			}
		}
	}
	
	// ------------------------------------------------------------------------
	//  Slot allocation
	//  slot分配
	// ------------------------------------------------------------------------

	public SimpleSlot addSharedSlotAndAllocateSubSlot(SharedSlot sharedSlot, Locality locality, JobVertexID groupId) {
		return addSharedSlotAndAllocateSubSlot(sharedSlot, locality, groupId, null);
	}

	public SimpleSlot addSharedSlotAndAllocateSubSlot(
			SharedSlot sharedSlot, Locality locality, CoLocationConstraint constraint)
	{
		return addSharedSlotAndAllocateSubSlot(sharedSlot, locality, null, constraint);
	}

	private SimpleSlot addSharedSlotAndAllocateSubSlot(
			SharedSlot sharedSlot, Locality locality, JobVertexID groupId, CoLocationConstraint constraint) {

		// sanity checks
		/** sharedSlot需要是根节点，且子节点集合为空，否则，抛出异常 */
		if (!sharedSlot.isRootAndEmpty()) {
			throw new IllegalArgumentException("The given slot is not an empty root slot.");
		}

		final ResourceID location = sharedSlot.getTaskManagerID();

		synchronized (lock) {
			// early out in case that the slot died (instance disappeared)
			/** 在slot挂掉的情况下早点退出(实例消失了) */
			if (!sharedSlot.isAlive()) {
				return null;
			}
			
			// add to the total bookkeeping
			/** 添加到总的备份集合中，如果是集合中已经存在的元素，则抛出异常 */
			if (!allSlots.add(sharedSlot)) {
				throw new IllegalArgumentException("Slot was already contained in the assignment group");
			}
			
			SimpleSlot subSlot;
			AbstractID groupIdForMap;

			/**
			 * 1) "位置协调约束"为null, 也就是没有位置上的约束, 则直接分配slot
			 * 2) "位置协调约束"不为null, 则先在sharedSlot中分配一个新的constraintGroupSlot, 然后在新的里面, 再分配一个slot
			 */
					
			if (constraint == null) {
				// allocate us a sub slot to return
				// 分配一个子slot
				subSlot = sharedSlot.allocateSubSlot(groupId);
				groupIdForMap = groupId;
			}
			else {
				// sanity check
				if (constraint.isAssignedAndAlive()) {
					throw new IllegalStateException(
							"Trying to add a shared slot to a co-location constraint that has a life slot.");
				}
				
				// we need a co-location slot --> a SimpleSlot nested in a SharedSlot to
				//                                host other co-located tasks
				/** 我们需要一个 co-location slot --> 一个 SimpleSlot 内嵌在一个 SharedSlot 中，用来持有 co-located 任务 */
				SharedSlot constraintGroupSlot = sharedSlot.allocateSharedSlot(constraint.getGroupId());
				groupIdForMap = constraint.getGroupId();
				
				if (constraintGroupSlot != null) {
					// the sub-slots in the co-location constraint slot have no own group IDs
					/** 在 co-location 约束下的 sub-slots 没有自己的 groupID */
					subSlot = constraintGroupSlot.allocateSubSlot(null);
					if (subSlot != null) {
						// all went well, we can give the constraint its slot
						/** 一切正常，我们可以给 constraint 设置slot */
						constraint.setSharedSlot(constraintGroupSlot);
						
						// NOTE: Do not lock the location constraint, because we don't yet know whether we will
						// take the slot here
					}
					else {
						// if we could not create a sub slot, release the co-location slot
						// note that this does implicitly release the slot we have just added
						// as well, because we release its last child slot. That is expected
						// and desired.
						constraintGroupSlot.releaseSlot();
					}
				}
				else {
					// this should not happen, as we are under the lock that also
					// guards slot disposals. Keep the check to be on the safe side
					subSlot = null;
				}
			}
			
			if (subSlot != null) {
				// preserve the locality information
				// 设定定位信息
				subSlot.setLocality(locality);
				
				// let the other groups know that this slot exists and that they
				// can place a task into this slot.
				/** 让其他 groups 知道，这个slot已经存在，并且他们可以将任务放到这个slot中 */
				boolean entryForNewJidExists = false;

				/**
				 * 由于针对当前groupIdFomMap这个group分配的slot, 可以被其他的group所共享使用, 所以将其添加到其他group的可用列表中
				 */
				for (Map.Entry<AbstractID, Map<ResourceID, List<SharedSlot>>> entry : availableSlotsPerJid.entrySet()) {
					// there is already an entry for this groupID
					// 对这个groupID，已经存在一个entry了
					if (entry.getKey().equals(groupIdForMap)) {
						entryForNewJidExists = true;
						continue;
					}

					Map<ResourceID, List<SharedSlot>> available = entry.getValue();
					putIntoMultiMap(available, location, sharedSlot);
				}

				// make sure an empty entry exists for this group, if no other entry exists
				/** 如果没有其他entry存在，确保给groupIdForMap构建一个空的entry */
				if (!entryForNewJidExists) {
					availableSlotsPerJid.put(groupIdForMap, new LinkedHashMap<ResourceID, List<SharedSlot>>());
				}

				return subSlot;
			}
			else {
				// if sharedSlot is releases, abort.
				// This should be a rare case, since this method is called with a fresh slot.
				/**
				 * 如果 sharedSlot 被释放了，终止
				 * 这应该是一个很罕见的情况，因为这个方法的入参是一个新的slot
				 */
				return null;
			}
		}
		// end synchronized (lock)
		// 同步锁结束
	}

	/**
	 * Gets a slot suitable for the given task vertex. This method will prefer slots that are local
	 * (with respect to {@link ExecutionVertex#getPreferredLocationsBasedOnInputs()}), but will return non local
	 * slots if no local slot is available. The method returns null, when this sharing group has
	 * no slot is available for the given JobVertexID.
	 * 为给定的 task vertex 获取一个合适的 slot 。
	 * 这个方法有优先选择local节点，但如果没有local是有效的，则会返回 non-local。
	 * 当这个 sharing group 为给定的 JobVertexID 没有有效的 slot，该方法会返回 null。
	 *
	 * @param vertexID the vertex id
	 * @param locationPreferences location preferences
	 *
	 * @return A slot to execute the given ExecutionVertex in, or null, if none is available.
	 */
	public SimpleSlot getSlotForTask(JobVertexID vertexID, Iterable<TaskManagerLocation> locationPreferences) {
		synchronized (lock) {
			/** 在内部已经存在的allSlots集合中，找出满足要求的 */
			Tuple2<SharedSlot, Locality> p = getSlotForTaskInternal(vertexID, locationPreferences, false);

			if (p != null) {
				SharedSlot ss = p.f0;
				SimpleSlot slot = ss.allocateSubSlot(vertexID);
				slot.setLocality(p.f1);
				return slot;
			}
			else {
				return null;
			}
		}
	}

	/**
	 * Gets a slot for a task that has a co-location constraint. This method tries to grab
	 * a slot form the location-constraint's shared slot. If that slot has not been initialized,
	 * then the method tries to grab another slot that is available for the location-constraint-group.
	 * 为一个拥有一个 co-location约束的task获取一个slot。
	 * 这个方法尝试从location-constraint's shared slot中获取一个slot。
	 * 如果那个slot还没有被初始化，这个方法则尝试夺取另一个slot，其对location-constraint-group是有效的
	 * 
	 * <p>In cases where the co-location constraint has not yet been initialized with a slot,
	 * or where that slot has been disposed in the meantime, this method tries to allocate a shared
	 * slot for the co-location constraint (inside one of the other available slots).</p>
	 * 在 co-location 约束 还没有用一个slot初始化过时，或者这个slot已经被废弃了，
	 * 这个方法会尝试为 co-location constraint 分配一个共享的slot(在另一个有效的slot中分配的)
	 * 
	 * <p>If a suitable shared slot is available, this method allocates a simple slot within that
	 * shared slot and returns it. If no suitable shared slot could be found, this method
	 * returns null.</p>
	 * 如果一个合适的共享slot是有效的，这个方法会在那个共享slot中分配一个simple slot，并返回它。
	 * 如果没有合适的shared slot发现，这个方法返回null
	 * 
	 * @param constraint The co-location constraint for the placement of the execution vertex.
	 * @param locationPreferences location preferences
	 * 
	 * @return A simple slot allocate within a suitable shared slot, or {@code null}, if no suitable
	 *         shared slot is available.
	 *
	 * 1）{@code CoLocationConstraint}已经用{@code SharedSlot}初始化过，
	 * 	  且当前这个{@code SharedSlot}处于alive状态，则直接用它分配一个slot
	 *
	 * 2）{@code CoLocationConstraint}被分配了，但是其中的{@code SharedSlot}挂了
	 *
	 * 3）{@code CoLocationConstraint}还没有用{@code SharedSlot}初始化过
	 */
	public SimpleSlot getSlotForTask(CoLocationConstraint constraint, Iterable<TaskManagerLocation> locationPreferences) {
		synchronized (lock) {
			if (constraint.isAssignedAndAlive()) {
				// the shared slot of the co-location group is initialized and set we allocate a sub-slot
				// co-location group 的共享槽被初始化过，那我们分配一个 sub-slot
				final SharedSlot shared = constraint.getSharedSlot();
				SimpleSlot subslot = shared.allocateSubSlot(null);
				subslot.setLocality(Locality.LOCAL);
				return subslot;
			}
			else if (constraint.isAssigned()) {
				// we had an assignment before.
				// 之前被分配过，但是当前那个sharedSlot已经挂了
				
				SharedSlot previous = constraint.getSharedSlot();
				if (previous == null) {
					throw new IllegalStateException("Bug: Found assigned co-location constraint without a slot.");
				}

				TaskManagerLocation location = previous.getTaskManagerLocation();
				// 从内部找出一个有效的
				Tuple2<SharedSlot, Locality> p = getSlotForTaskInternal(
						constraint.getGroupId(), Collections.singleton(location), true);
				// 如果内部找不到，只能返回null了
				if (p == null) {
					return null;
				}
				else {
					SharedSlot newSharedSlot = p.f0;

					// allocate the co-location group slot inside the shared slot
					// 在一个SharedSlot内部为co-location分配一个slot
					SharedSlot constraintGroupSlot = newSharedSlot.allocateSharedSlot(constraint.getGroupId());
					if (constraintGroupSlot != null) {
						constraint.setSharedSlot(constraintGroupSlot);

						// the sub slots in the co location constraint slot have no group that they belong to
						// (other than the co-location-constraint slot)
						SimpleSlot subSlot = constraintGroupSlot.allocateSubSlot(null);
						subSlot.setLocality(Locality.LOCAL);
						return subSlot;
					}
					else {
						// could not allocate the co-location-constraint shared slot
						// 无法分配，则返回null
						return null;
					}
				}
			}
			else {
				// the location constraint has not been associated with a shared slot, yet.
				// grab a new slot and initialize the constraint with that one.
				// preferred locations are defined by the vertex
				/**
				 * 还没有被关联过一个shared slot。
				 * 抓取一个并用来初始化。
				 */
				Tuple2<SharedSlot, Locality> p =
						getSlotForTaskInternal(constraint.getGroupId(), locationPreferences, false);
				if (p == null) {
					// could not get a shared slot for this co-location-group
					return null;
				}
				else {
					final SharedSlot availableShared = p.f0;
					final Locality l = p.f1;

					// allocate the co-location group slot inside the shared slot
					SharedSlot constraintGroupSlot = availableShared.allocateSharedSlot(constraint.getGroupId());
					
					// IMPORTANT: We do not lock the location, yet, since we cannot be sure that the
					//            caller really sticks with the slot we picked!
					constraint.setSharedSlot(constraintGroupSlot);
					
					// the sub slots in the co location constraint slot have no group that they belong to
					// (other than the co-location-constraint slot)
					SimpleSlot sub = constraintGroupSlot.allocateSubSlot(null);
					sub.setLocality(l);
					return sub;
				}
			}
		}
	}

	/**
	 * 能在指定的preferredLocations中找到满足要求的最好，
	 * 如果不能，再看localOnly，如果localOnly为ture，那就没辙了，返回null，如果为false，那还有一线生机
	 * @param groupId
	 * @param preferredLocations
	 * @param localOnly
	 * @return
	 */
	private Tuple2<SharedSlot, Locality> getSlotForTaskInternal(
			AbstractID groupId, Iterable<TaskManagerLocation> preferredLocations, boolean localOnly)
	{
		// check if there is anything at all in this group assignment
		// 如果压根就不存在slot了，那还分配个毛，直接返回null
		if (allSlots.isEmpty()) {
			return null;
		}

		// get the available slots for the group
		// 获取给定groupId的有效slot
		Map<ResourceID, List<SharedSlot>> slotsForGroup = availableSlotsPerJid.get(groupId);
		
		if (slotsForGroup == null) {
			// we have a new group, so all slots are available
			// 我们新增了一个新的group，所以所有的slots都是有效的
			slotsForGroup = new LinkedHashMap<>();
			availableSlotsPerJid.put(groupId, slotsForGroup);

			for (SharedSlot availableSlot : allSlots) {
				putIntoMultiMap(slotsForGroup, availableSlot.getTaskManagerID(), availableSlot);
			}
		}
		else if (slotsForGroup.isEmpty()) {
			// the group exists, but nothing is available for that group
			// 如果这个group存在，但是没有任何有效的，那也只能返回null了
			return null;
		}

		// check whether we can schedule the task to a preferred location
		// 检查我们是否可以将task调度到一个preferred location
		// 这个变量表示从 preferredLocations 中，没有找到满足要求的
		boolean didNotGetPreferred = false;

		if (preferredLocations != null) {
			for (TaskManagerLocation location : preferredLocations) {

				// set the flag that we failed a preferred location. If one will be found,
				// we return early anyways and skip the flag evaluation
				// 设置这个flat，表示我们没能成功的获取到一个优先考虑的位置。
				// 如果发现了一个，我们会提前return，直接跳过了后续的flag校验
				didNotGetPreferred = true;

				SharedSlot slot = removeFromMultiMap(slotsForGroup, location.getResourceID());
				if (slot != null && slot.isAlive()) {
					return new Tuple2<>(slot, Locality.LOCAL);
				}
			}
		}

		// if we want only local assignments, exit now with a "not found" result
		// 如果我们只想local，那只能返回null了
		if (didNotGetPreferred && localOnly) {
			return null;
		}

		Locality locality = didNotGetPreferred ? Locality.NON_LOCAL : Locality.UNCONSTRAINED;

		// schedule the task to any available location
		// 获取任意有效的location
		SharedSlot slot;
		while ((slot = pollFromMultiMap(slotsForGroup)) != null) {
			if (slot.isAlive()) {
				return new Tuple2<>(slot, locality);
			}
		}
		
		// nothing available after all, all slots were dead
		// 全部处理过，没有有效的，则就是所有的slot都挂了，只能返回null了
		return null;
	}

	// ------------------------------------------------------------------------
	//  Slot releasing
	// ------------------------------------------------------------------------

	/**
	 * Releases the simple slot from the assignment group.
	 * 从分配组中释放简单的slot
	 * 
	 * @param simpleSlot The SimpleSlot to be released
	 */
	void releaseSimpleSlot(SimpleSlot simpleSlot) {
		synchronized (lock) {
			// try to transition to the CANCELED state. That state marks
			// that the releasing is in progress
			/** 尝试将状态转换为{@link Slot#CANCELLED}状态。表示已经开始进行releasing操作 */

			/**
			 * 1) 首先将自身标记为 CANCELLED 状态
			 * 2) 再将状态从 CANCELLED 状态转换为 RELEASED 状态
			 */
			if (simpleSlot.markCancelled()) {

				// sanity checks
				// 明智的检查
				if (simpleSlot.isAlive()) {
					throw new IllegalStateException("slot is still alive");
				}

				// check whether the slot is already released
				/** 检查slot是否已经处于{@link Slot#RELEASED}，状态转换成功，则继续操作 */
				if (simpleSlot.markReleased()) {
					LOG.debug("Release simple slot {}.", simpleSlot);

					AbstractID groupID = simpleSlot.getGroupID();
					SharedSlot parent = simpleSlot.getParent();

					// if we have a group ID, then our parent slot is tracked here
					/** 如果我们拥有一个group ID，那我们的 parent slot 应该在这里是被跟踪的 */
					if (groupID != null && !allSlots.contains(parent)) {
						throw new IllegalArgumentException("Slot was not associated with this SlotSharingGroup before.");
					}

					/**
					 * 从父亲{@link SharedSlot}中，移除这个子节点
					 * 对于属于一个{@code SlotSharedGroup}的{@code SimpleSlot}, 其parent肯定是不会为null, 所以这里不用对parent进行null判断
					 */
					int parentRemaining = parent.removeDisposedChildSlot(simpleSlot);

					if (parentRemaining > 0) {
						// the parent shared slot is still alive. make sure we make it
						// available again to the group of the just released slot
						/**
						 * parent shared slot 仍然是alive的。
						 * 确保让他对刚释放的slot的group是有效的
						 */

						if (groupID != null) {
							// if we have a group ID, then our parent becomes available
							// for that group again. otherwise, the slot is part of a
							// co-location group and nothing becomes immediately available

							Map<ResourceID, List<SharedSlot>> slotsForJid = availableSlotsPerJid.get(groupID);

							// sanity check
							if (slotsForJid == null) {
								throw new IllegalStateException("Trying to return a slot for group " + groupID +
										" when available slots indicated that all slots were available.");
							}

							putIntoMultiMap(slotsForJid, parent.getTaskManagerID(), parent);
						}
					} else {
						// the parent shared slot is now empty and can be released
						/** 父亲{@link SharedSlot}的子节点已经空了，现在可以释放父亲自身了 */
						parent.markCancelled();
						internalDisposeEmptySharedSlot(parent);
					}
				}
			}
		}
	}

	/**
	 * Called from {@link org.apache.flink.runtime.instance.SharedSlot#releaseSlot()}.
	 * 
	 * @param sharedSlot The slot to be released.
	 */
	void releaseSharedSlot(SharedSlot sharedSlot) {
		synchronized (lock) {
			if (sharedSlot.markCancelled()) {
				// we are releasing this slot
				
				if (sharedSlot.hasChildren()) {
					// by simply releasing all children, we should eventually release this slot.
					// 通过释放所有的children，也就最终释放了这个slot
					Set<Slot> children = sharedSlot.getSubSlots();
					while (children.size() > 0) {
						children.iterator().next().releaseSlot();
					}
				}
				else {
					// if there are no children that trigger the release, we trigger it directly
					internalDisposeEmptySharedSlot(sharedSlot);
				}
			}
		}
	}

	/**
	 * 
	 * <p><b>NOTE: This method must be called from within a scope that holds the lock.</b></p>
	 */
	private void internalDisposeEmptySharedSlot(SharedSlot sharedSlot) {
		// sanity check
		/**
		 * 如果{@link sharedSlot}还处于alive状态，或者子slot集合不为空，则抛出异常
		 * 也就是说，对于{@link SharedSlot}实例来说，只有当其子slots集合为空，且自身状态处于非活跃状态时，才可以进行释放操作。
		 */
		if (sharedSlot.isAlive() | !sharedSlot.getSubSlots().isEmpty()) {
			throw new IllegalArgumentException();
		}
		
		final SharedSlot parent = sharedSlot.getParent();
		final AbstractID groupID = sharedSlot.getGroupID();
		
		// 1) If we do not have a parent, we are a root slot.
		// 2) If we are not a root slot, we are a slot with a groupID and our parent
		//    becomes available for that group
		/**
		 * 1) 如果没有parent，那即使个根节点；
		 * 2）如果我们不是根节点，我们是拥有一个groupID的slot，并且我们的parent对那个group是有效的
		 */
		
		if (parent == null) {
			// root slot, return to the instance.
			// 根slot，归还给 instance
			sharedSlot.getOwner().returnAllocatedSlot(sharedSlot);
			
			// also, make sure we remove this slot from everywhere
			// 当然，确保所有地方都移除了
			allSlots.remove(sharedSlot);
			removeSlotFromAllEntries(availableSlotsPerJid, sharedSlot);
		}
		else if (groupID != null) {
			// we remove ourselves from our parent slot

			if (sharedSlot.markReleased()) {
				LOG.debug("Internally dispose empty shared slot {}.", sharedSlot);

				int parentRemaining = parent.removeDisposedChildSlot(sharedSlot);
				
				if (parentRemaining > 0) {
					// the parent becomes available for the group again
					Map<ResourceID, List<SharedSlot>> slotsForGroup = availableSlotsPerJid.get(groupID);

					// sanity check
					if (slotsForGroup == null) {
						throw new IllegalStateException("Trying to return a slot for group " + groupID +
								" when available slots indicated that all slots were available.");
					}

					putIntoMultiMap(slotsForGroup, parent.getTaskManagerID(), parent);
					
				}
				else {
					// this was the last child of the parent. release the parent.
					parent.markCancelled();
					internalDisposeEmptySharedSlot(parent);
				}
			}
		}
		else {
			throw new IllegalStateException(
					"Found a shared slot that is neither a root slot, nor associated with a vertex group.");
		}
	}
	
	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private static void putIntoMultiMap(Map<ResourceID, List<SharedSlot>> map, ResourceID location, SharedSlot slot) {
		List<SharedSlot> slotsForInstance = map.get(location);
		if (slotsForInstance == null) {
			slotsForInstance = new ArrayList<SharedSlot>();
			map.put(location, slotsForInstance);
		}
		slotsForInstance.add(slot);
	}
	
	private static SharedSlot removeFromMultiMap(Map<ResourceID, List<SharedSlot>> map, ResourceID location) {
		List<SharedSlot> slotsForLocation = map.get(location);
		
		if (slotsForLocation == null) {
			return null;
		}
		else {
			SharedSlot slot = slotsForLocation.remove(slotsForLocation.size() - 1);
			if (slotsForLocation.isEmpty()) {
				map.remove(location);
			}
			
			return slot;
		}
	}
	
	private static SharedSlot pollFromMultiMap(Map<ResourceID, List<SharedSlot>> map) {
		Iterator<Map.Entry<ResourceID, List<SharedSlot>>> iter = map.entrySet().iterator();
		
		while (iter.hasNext()) {
			List<SharedSlot> slots = iter.next().getValue();
			
			if (slots.isEmpty()) {
				iter.remove();
			}
			else if (slots.size() == 1) {
				SharedSlot slot = slots.remove(0);
				iter.remove();
				return slot;
			}
			else {
				return slots.remove(slots.size() - 1);
			}
		}
		
		return null;
	}
	
	private static void removeSlotFromAllEntries(
			Map<AbstractID, Map<ResourceID, List<SharedSlot>>> availableSlots, SharedSlot slot)
	{
		final ResourceID taskManagerId = slot.getTaskManagerID();
		
		for (Map.Entry<AbstractID, Map<ResourceID, List<SharedSlot>>> entry : availableSlots.entrySet()) {
			Map<ResourceID, List<SharedSlot>> map = entry.getValue();

			List<SharedSlot> list = map.get(taskManagerId);
			if (list != null) {
				list.remove(slot);
				if (list.isEmpty()) {
					map.remove(taskManagerId);
				}
			}
		}
	}
}
