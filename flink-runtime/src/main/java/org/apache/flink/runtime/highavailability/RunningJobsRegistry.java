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

package org.apache.flink.runtime.highavailability;

import org.apache.flink.api.common.JobID;

import java.io.IOException;

/**
 * A simple registry that tracks if a certain job is pending execution, running, or completed.
 * 一个简单的注册表, 用于跟踪某个作业是否正在执行、运行或者完成。
 * 
 * <p>This registry is used in highly-available setups with multiple master nodes,
 * to determine whether a new leader should attempt to recover a certain job (because the 
 * job is still running), or whether the job has already finished successfully (in case of a
 * finite job) and the leader has only been granted leadership because the previous leader
 * quit cleanly after the job was finished.
 * 这个注册表用在有多个主节点的HA模式下,用来决定一个新的leader是否需要试图恢复某个job(因为工作仍在运行),
 * 或者这份工作是否已经成功完成(在有限任务的情况下),
 * 以及leader只是被授予了领导权, 因为前一个leader在job完成后,已经做了清理工作。
 * 
 * <p>In addition, the registry can help to determine whether a newly assigned leader JobManager
 * should attempt reconciliation with running TaskManagers, or immediately schedule the job from
 * the latest checkpoint/savepoint.
 * 此外, 注册中心还可以帮助确定新指定的leader JobManager 应该尝试与运行中的TaskManager进行沟通,或者直接从最新的检查点/保存点进行工作安排。
 */
public interface RunningJobsRegistry {

	/**
	 * The scheduling status of a job, as maintained by the {@code RunningJobsRegistry}.
	 * 一个 job 的调度状态, 被 RunningJobsRegistry 维护着
	 */
	enum JobSchedulingStatus {

		/**
		 * Job has not been scheduled, yet
		 * job 还没有被调度
		 */
		PENDING,

		/**
		 * Job has been scheduled and is not yet finished
		 * job 已经被调度,但是还没有完成
		 */
		RUNNING,

		/**
		 * Job has been finished, successfully or unsuccessfully
		 * job 已经完成, 成功或者失败
		 */
		DONE;
	}

	// ------------------------------------------------------------------------

	/**
	 * Marks a job as running. Requesting the job's status via the {@link #getJobSchedulingStatus(JobID)}
	 * method will return {@link JobSchedulingStatus#RUNNING}.
	 * 标记一个job的状态为running。
	 * 
	 * @param jobID The id of the job.
	 *
	 * @throws IOException Thrown when the communication with the highly-available storage or registry
	 *                     failed and could not be retried.
	 */
	void setJobRunning(JobID jobID) throws IOException;

	/**
	 * Marks a job as completed. Requesting the job's status via the {@link #getJobSchedulingStatus(JobID)}
	 * method will return {@link JobSchedulingStatus#DONE}.
	 *
	 * @param jobID The id of the job.
	 * 
	 * @throws IOException Thrown when the communication with the highly-available storage or registry
	 *                     failed and could not be retried.
	 */
	void setJobFinished(JobID jobID) throws IOException;

	/**
	 * Gets the scheduling status of a job.
	 *
	 * @param jobID The id of the job to check.
	 * @return The job scheduling status.
	 * 
	 * @throws IOException Thrown when the communication with the highly-available storage or registry
	 *                     failed and could not be retried.
	 */
	JobSchedulingStatus getJobSchedulingStatus(JobID jobID) throws IOException;

	/**
	 * Clear job state form the registry, usually called after job finish
	 * 从注册表中清理job的状态, 一般在job完成时调用
	 *
	 * @param jobID The id of the job to check.
	 * 
	 * @throws IOException Thrown when the communication with the highly-available storage or registry
	 *                     failed and could not be retried.
	 */
	void clearJob(JobID jobID) throws IOException;
}
