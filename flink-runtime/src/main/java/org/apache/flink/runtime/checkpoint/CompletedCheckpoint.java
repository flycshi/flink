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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A CompletedCheckpoint describes a checkpoint after all required tasks acknowledged it (with their state)
 * and that is considered successful. The CompletedCheckpoint class contains all the metadata of the
 * checkpoint, i.e., checkpoint ID, timestamps, and the handles to all states that are part of the
 * checkpoint.
 * 一个{@code CompletedCheckpoint}描述了这样的一个checkpoint，其所有需要ack的任务都进行了ack操作(附带了它们的状态)，然后就表示成功了。
 * {@code CompletedCheckpoint}包含了checkpoint的所有元数据，比如 checkpoint id，时间戳，以及所有状态的句柄。
 * 
 * <h2>Size the CompletedCheckpoint Instances</h2>
 * 
 * In most cases, the CompletedCheckpoint objects are very small, because the handles to the checkpoint
 * states are only pointers (such as file paths). However, the some state backend implementations may
 * choose to store some payload data directly with the metadata (for example to avoid many small files).
 * If those thresholds are increased to large values, the memory consumption of the CompletedCheckpoint
 * objects can be significant.
 * 在大多数情况下，{@code CompletedCheckpoint}对象是非常小的，因为checkpoint中状态的句柄只是指针(比如文件路径)。
 * 但是，有些状态后端实现，可能会选取将一些负载数据直接保存在元数据中(比如为了避免很多小文件)。
 * 如果这些阈值增长到比较大的值，{@code CompletedCheckpoint}对象的内存消化会是非常大的。
 * 
 * <h2>Externalized Metadata</h2>
 * 
 * The metadata of the CompletedCheckpoint is optionally also persisted in an external storage
 * system. In that case, the checkpoint is called <i>externalized</i>.
 * CompletedCheckpoint的元数据可以选择持久化到外部存储系统中。在这个情况下，checkpoint被称为<i>externalized</i>
 * 
 * <p>Externalized checkpoints have an external pointer, which points to the metadata. For example
 * when externalizing to a file system, that pointer is the file path to the checkpoint's folder
 * or the metadata file. For a state backend that stores metadata in database tables, the pointer
 * could be the table name and row key. The pointer is encoded as a String.
 * 外部持久化的checkpoint有一个外部的指针，其指向元数据。
 * 比如，当持久化到一个文件系统，指针是一个指向checkpoint的目录或者元数据文件的文件地址。
 * 对于将元数据存储在数据库表中的state backend，指针可能是表名和row key。
 * 指针编码为一个字符串。
 * 
 * <h2>Externalized Metadata and High-availability</h2>
 * 
 * For high availability setups, the checkpoint metadata must be stored persistent and available
 * as well. The high-availability services that stores the checkpoint ground-truth (meaning what are
 * the latest completed checkpoints in what order) often rely on checkpoints being externalized. That
 * way, those services only store pointers to the externalized metadata, rather than the complete
 * metadata itself (for example ZooKeeper's ZNode payload should ideally be less than megabytes).
 * 对于HA配置，checkpoint元数据必须被持久化并且要高可用。
 * 存储checkpoint基本事实(也就是什么是最新的完成的检查点)通常依赖于被外部化的检查点。
 * 这种情况下，这些服务仅存储外部存储的元数据的指针，而不是完整的元数据本身(比如zk的ZNode的负载需要小于1MB)
 */
public class CompletedCheckpoint implements Serializable {

	private static final Logger LOG = LoggerFactory.getLogger(CompletedCheckpoint.class);

	private static final long serialVersionUID = -8360248179615702014L;

	// ------------------------------------------------------------------------

	/**
	 * The ID of the job that the checkpoint belongs to
	 * 检查点属于的job的id
	 */
	private final JobID job;

	/**
	 * The ID (logical timestamp) of the checkpoint
	 * 检查点的ID(逻辑时间戳)
	 */
	private final long checkpointID;

	/**
	 * The timestamp when the checkpoint was triggered.
	 * 检查点被触发时的时间戳
	 */
	private final long timestamp;

	/**
	 * The duration of the checkpoint (completion timestamp - trigger timestamp).
	 * 检查点持续时长(完成时间戳 - 触发时间戳)
	 */
	private final long duration;

	/**
	 * States of the different operator groups belonging to this checkpoint
	 * 属于该检查点的不同操作符组的状态
	 */
	private final Map<OperatorID, OperatorState> operatorStates;

	/** Properties for this checkpoint. */
	private final CheckpointProperties props;

	/**
	 * States that were created by a hook on the master (in the checkpoint coordinator).
	 * 由master(在checkpoint协调器)上的hook创建的状态
	 * */
	private final Collection<MasterState> masterHookStates;

	/**
	 * The state handle to the externalized meta data, if the metadata has been externalized.
	 * 如果元数据被外部持久化了，就表示外部元数据的状态句柄
	 * */
	@Nullable
	private final StreamStateHandle externalizedMetadata;

	/**
	 * External pointer to the completed checkpoint (for example file path) if externalized; null otherwise.
	 * 如果外部持久化了，就表示外部指针(比如文件路径)，否则就是null
	 * */
	@Nullable
	private final String externalPointer;

	/**
	 * Optional stats tracker callback for discard.
	 * 针对discard操作的可选状态跟踪回调
	 * */
	@Nullable
	private transient volatile CompletedCheckpointStats.DiscardCallback discardCallback;

	// ------------------------------------------------------------------------

	public CompletedCheckpoint(
			JobID job,
			long checkpointID,
			long timestamp,
			long completionTimestamp,
			Map<OperatorID, OperatorState> operatorStates,
			@Nullable Collection<MasterState> masterHookStates,
			CheckpointProperties props,
			@Nullable StreamStateHandle externalizedMetadata,
			@Nullable String externalPointer) {

		checkArgument(checkpointID >= 0);
		checkArgument(timestamp >= 0);
		checkArgument(completionTimestamp >= 0);

		checkArgument((externalPointer == null) == (externalizedMetadata == null),
				"external pointer without externalized metadata must be both null or both non-null");

		checkArgument(!props.externalizeCheckpoint() || externalPointer != null,
			"Checkpoint properties require externalized checkpoint, but checkpoint is not externalized");

		this.job = checkNotNull(job);
		this.checkpointID = checkpointID;
		this.timestamp = timestamp;
		this.duration = completionTimestamp - timestamp;

		// we create copies here, to make sure we have no shared mutable
		// data structure with the "outside world"
		this.operatorStates = new HashMap<>(checkNotNull(operatorStates));
		this.masterHookStates = masterHookStates == null || masterHookStates.isEmpty() ?
				Collections.<MasterState>emptyList() :
				new ArrayList<>(masterHookStates);

		this.props = checkNotNull(props);
		this.externalizedMetadata = externalizedMetadata;
		this.externalPointer = externalPointer;
	}

	// ------------------------------------------------------------------------

	public JobID getJobId() {
		return job;
	}

	public long getCheckpointID() {
		return checkpointID;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public long getDuration() {
		return duration;
	}

	public CheckpointProperties getProperties() {
		return props;
	}

	public void discardOnFailedStoring() throws Exception {
		doDiscard();
	}

	public boolean discardOnSubsume() throws Exception {

		if (props.discardOnSubsumed()) {
			doDiscard();
			return true;
		}

		return false;
	}

	public boolean discardOnShutdown(JobStatus jobStatus) throws Exception {

		if (jobStatus == JobStatus.FINISHED && props.discardOnJobFinished() ||
				jobStatus == JobStatus.CANCELED && props.discardOnJobCancelled() ||
				jobStatus == JobStatus.FAILED && props.discardOnJobFailed() ||
				jobStatus == JobStatus.SUSPENDED && props.discardOnJobSuspended()) {

			doDiscard();
			return true;
		} else {
			if (externalPointer != null) {
				LOG.info("Persistent checkpoint with ID {} at '{}' not discarded.",
						checkpointID, externalPointer);
			}

			return false;
		}
	}

	private void doDiscard() throws Exception {

		LOG.trace("Executing discard procedure for {}.", this);

		try {
			// collect exceptions and continue cleanup
			Exception exception = null;

			// drop the metadata, if we have some
			// 如果有元数据，则删除
			if (externalizedMetadata != null) {
				try {
					externalizedMetadata.discardState();
				} catch (Exception e) {
					exception = e;
				}
			}

			// discard private state objects
			try {
				StateUtil.bestEffortDiscardAllStateObjects(operatorStates.values());
			} catch (Exception e) {
				exception = ExceptionUtils.firstOrSuppressed(e, exception);
			}

			if (exception != null) {
				throw exception;
			}
		} finally {
			operatorStates.clear();

			// to be null-pointer safe, copy reference to stack
			CompletedCheckpointStats.DiscardCallback discardCallback = this.discardCallback;
			if (discardCallback != null) {
				discardCallback.notifyDiscardedCheckpoint();
			}
		}
	}

	public long getStateSize() {
		long result = 0L;

		for (OperatorState operatorState : operatorStates.values()) {
			result += operatorState.getStateSize();
		}

		return result;
	}

	public Map<OperatorID, OperatorState> getOperatorStates() {
		return operatorStates;
	}

	public Collection<MasterState> getMasterHookStates() {
		return Collections.unmodifiableCollection(masterHookStates);
	}

	public boolean isExternalized() {
		return externalizedMetadata != null;
	}

	@Nullable
	public StreamStateHandle getExternalizedMetadata() {
		return externalizedMetadata;
	}

	@Nullable
	public String getExternalPointer() {
		return externalPointer;
	}

	/**
	 * Sets the callback for tracking when this checkpoint is discarded.
	 *
	 * @param discardCallback Callback to call when the checkpoint is discarded.
	 */
	void setDiscardCallback(@Nullable CompletedCheckpointStats.DiscardCallback discardCallback) {
		this.discardCallback = discardCallback;
	}

	/**
	 * Register all shared states in the given registry. This is method is called
	 * before the checkpoint is added into the store.
	 *
	 * @param sharedStateRegistry The registry where shared states are registered
	 */
	public void registerSharedStatesAfterRestored(SharedStateRegistry sharedStateRegistry) {
		sharedStateRegistry.registerAll(operatorStates.values());
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public String toString() {
		return String.format("Checkpoint %d @ %d for %s", checkpointID, timestamp, job);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		CompletedCheckpoint that = (CompletedCheckpoint) o;

		if (checkpointID != that.checkpointID) {
			return false;
		}
		return job.equals(that.job);
	}

	@Override
	public int hashCode() {
		int result = job.hashCode();
		result = 31 * result + (int) (checkpointID ^ (checkpointID >>> 32));
		return result;
	}
}
