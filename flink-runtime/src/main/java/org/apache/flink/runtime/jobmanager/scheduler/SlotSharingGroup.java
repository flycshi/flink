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

import org.apache.flink.runtime.instance.SlotSharingGroupAssignment;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

/**
 * A slot sharing units defines which different task (from different job vertices) can be
 * deployed together within a slot. This is a soft permission, in contrast to the hard constraint
 * defined by a co-location hint.
 * 一个槽位共享单元, 定义了来自不同节点的不同任务可以部署在同一个slot中。这是一个软限定,是相对于co-location定义的硬限定的。
 */
public class SlotSharingGroup implements java.io.Serializable {
	
	private static final long serialVersionUID = 1L;
	

	private final Set<JobVertexID> ids = new TreeSet<JobVertexID>();
	
	/**
	 * Mapping of tasks to subslots. This field is only needed inside the JobManager, and is not RPCed.
	 * task -> subslot 映射。
	 * 该字段只有在 JobManager 中才需要, 不是rpc
	 */
	private transient SlotSharingGroupAssignment taskAssignment;
	
	
	public SlotSharingGroup() {}
	
	public SlotSharingGroup(JobVertexID ... sharedVertices) {
		for (JobVertexID id : sharedVertices) {
			this.ids.add(id);
		}
	}

	// --------------------------------------------------------------------------------------------
	
	public void addVertexToGroup(JobVertexID id) {
		this.ids.add(id);
	}
	
	public void removeVertexFromGroup(JobVertexID id) {
		this.ids.remove(id);
	}
	
	public Set<JobVertexID> getJobVertexIds() {
		return Collections.unmodifiableSet(ids);
	}
	
	
	public SlotSharingGroupAssignment getTaskAssignment() {
		if (this.taskAssignment == null) {
			this.taskAssignment = new SlotSharingGroupAssignment();
		}
		
		return this.taskAssignment;
	}
	
	public void clearTaskAssignment() {
		if (this.taskAssignment != null) {
			if (this.taskAssignment.getNumberOfSlots() > 0) {
				throw new IllegalStateException("SlotSharingGroup cannot clear task assignment, group still has allocated resources.");
			}
		}
		this.taskAssignment = null;
	}
	
	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------
	
	@Override
	public String toString() {
		return "SlotSharingGroup " + this.ids.toString();
	}
}
