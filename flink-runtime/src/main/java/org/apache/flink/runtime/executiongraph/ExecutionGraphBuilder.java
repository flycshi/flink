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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;
import org.apache.flink.runtime.checkpoint.hooks.MasterHooks;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.client.JobSubmissionException;
import org.apache.flink.runtime.executiongraph.failover.FailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.FailoverStrategyLoader;
import org.apache.flink.runtime.executiongraph.metrics.DownTimeGauge;
import org.apache.flink.runtime.executiongraph.metrics.NumberOfFullRestartsGauge;
import org.apache.flink.runtime.executiongraph.metrics.RestartTimeGauge;
import org.apache.flink.runtime.executiongraph.metrics.UpTimeGauge;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.instance.SlotProvider;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.jsonplan.JsonPlanGenerator;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.util.DynamicCodeLoadingException;
import org.apache.flink.util.SerializedValue;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utility class to encapsulate the logic of building an {@link ExecutionGraph} from a {@link JobGraph}.
 * 用来封装从一个{@code JobGraph}构建一个{@code ExecutionGraph}的逻辑的工具类
 */
public class ExecutionGraphBuilder {

	/**
	 * Builds the ExecutionGraph from the JobGraph.
	 * If a prior execution graph exists, the JobGraph will be attached. If no prior execution
	 * graph exists, then the JobGraph will become attach to a new empty execution graph.
	 * 从{@code JobGraph}构建{@code ExecutionGraph}。
	 * 如果存在一个之前的{@code ExecutionGraph}, {@code JobGraph}将会被附加上。
	 * 如果不存在{@code ExecutionGraph}, {@code JobGraph}会附加到一个新的空的{@code ExecutionGraph}。
	 */
	public static ExecutionGraph buildGraph(
			@Nullable ExecutionGraph prior,
			JobGraph jobGraph,
			Configuration jobManagerConfig,
			ScheduledExecutorService futureExecutor,
			Executor ioExecutor,
			SlotProvider slotProvider,
			ClassLoader classLoader,
			CheckpointRecoveryFactory recoveryFactory,
			Time timeout,
			RestartStrategy restartStrategy,
			MetricGroup metrics,
			int parallelismForAutoMax,
			BlobWriter blobWriter,
			Logger log)
		throws JobExecutionException, JobException {

		/** jobGraph 不能为 null */
		checkNotNull(jobGraph, "job graph cannot be null");

		/** 提取job的name和id */
		final String jobName = jobGraph.getName();
		final JobID jobId = jobGraph.getJobID();

		/** 从配置中获取容灾恢复策略的工厂类 */
		final FailoverStrategy.Factory failoverStrategy =
				FailoverStrategyLoader.loadFailoverStrategy(jobManagerConfig, log);

		/** 构建{@code JobInformation}实例 */
		final JobInformation jobInformation = new JobInformation(
			jobId,
			jobName,
			jobGraph.getSerializedExecutionConfig(),
			jobGraph.getJobConfiguration(),
			jobGraph.getUserJarBlobKeys(),
			jobGraph.getClasspaths());

		// create a new execution graph, if none exists so far
		/** 如果入参中的prior不为null，则直接使用，如果为null，则构建一个新的{@code ExecutionGraph}实例 */
		final ExecutionGraph executionGraph;
		try {
			executionGraph = (prior != null) ? prior :
                new ExecutionGraph(
                    jobInformation,
                    futureExecutor,
                    ioExecutor,
                    timeout,
                    restartStrategy,
                    failoverStrategy,
                    slotProvider,
                    classLoader,
                    blobWriter);
		} catch (IOException e) {
			throw new JobException("Could not create the ExecutionGraph.", e);
		}

		// set the basic properties
		/** 设置基本属性 */
		executionGraph.setScheduleMode(jobGraph.getScheduleMode());
		executionGraph.setQueuedSchedulingAllowed(jobGraph.getAllowQueuedScheduling());

		try {
			executionGraph.setJsonPlan(JsonPlanGenerator.generatePlan(jobGraph));
		}
		catch (Throwable t) {
			log.warn("Cannot create JSON plan for job", t);
			// give the graph an empty plan
			executionGraph.setJsonPlan("{}");
		}

		// initialize the vertices that have a master initialization hook
		// file output formats create directories here, input formats create splits

		final long initMasterStart = System.nanoTime();
		log.info("Running initialization on master for job {} ({}).", jobName, jobId);

		for (JobVertex vertex : jobGraph.getVertices()) {
			String executableClass = vertex.getInvokableClassName();
			if (executableClass == null || executableClass.isEmpty()) {
				throw new JobSubmissionException(jobId,
						"The vertex " + vertex.getID() + " (" + vertex.getName() + ") has no invokable class.");
			}

			if (vertex.getParallelism() == ExecutionConfig.PARALLELISM_AUTO_MAX) {
				vertex.setParallelism(parallelismForAutoMax);
			}

			try {
				vertex.initializeOnMaster(classLoader);
			}
			catch (Throwable t) {
					throw new JobExecutionException(jobId,
							"Cannot initialize task '" + vertex.getName() + "': " + t.getMessage(), t);
			}
		}

		log.info("Successfully ran initialization on master in {} ms.",
				(System.nanoTime() - initMasterStart) / 1_000_000);

		// topologically sort the job vertices and attach the graph to the existing one
		/** 从数据源依次遍历所有的{@code JobVertex}, 得到一个有序的集合 */
		List<JobVertex> sortedTopology = jobGraph.getVerticesSortedTopologicallyFromSources();
		if (log.isDebugEnabled()) {
			log.debug("Adding {} vertices from job graph {} ({}).", sortedTopology.size(), jobName, jobId);
		}
		/**
		 * 1、这里就是将{@code JobGraph}中的各个{@code JobVertex}, 按上面排好的顺序,
		 * 依次转化成{@code ExecutionJobVertex}, 添加到{@code ExecutionGraph}中,
		 * 2、在{@code ExecutionJobVertex}的构造函数中, 会根据并行度, 构建相应的子任务集合{@code ExecutionVertex}
		 * 3、在{@code ExecutionVertex}的构造函数中, 会构建当前要执行的{@code Execution}
		 */
		executionGraph.attachJobGraph(sortedTopology);

		if (log.isDebugEnabled()) {
			log.debug("Successfully created execution graph from job graph {} ({}).", jobName, jobId);
		}

		// configure the state checkpointing
		/** 配置状态checkpointing */
		JobCheckpointingSettings snapshotSettings = jobGraph.getCheckpointingSettings();
		if (snapshotSettings != null) {
			List<ExecutionJobVertex> triggerVertices = 
					idToVertex(snapshotSettings.getVerticesToTrigger(), executionGraph);

			List<ExecutionJobVertex> ackVertices =
					idToVertex(snapshotSettings.getVerticesToAcknowledge(), executionGraph);

			List<ExecutionJobVertex> confirmVertices =
					idToVertex(snapshotSettings.getVerticesToConfirm(), executionGraph);

			CompletedCheckpointStore completedCheckpoints;
			CheckpointIDCounter checkpointIdCounter;
			try {
				int maxNumberOfCheckpointsToRetain = jobManagerConfig.getInteger(
					CoreOptions.MAX_RETAINED_CHECKPOINTS);

				if (maxNumberOfCheckpointsToRetain <= 0) {
					// warning and use 1 as the default value if the setting in
					// state.checkpoints.max-retained-checkpoints is not greater than 0.
					log.warn("The setting for '{} : {}' is invalid. Using default value of {}",
							CoreOptions.MAX_RETAINED_CHECKPOINTS.key(),
							maxNumberOfCheckpointsToRetain,
							CoreOptions.MAX_RETAINED_CHECKPOINTS.defaultValue());

					maxNumberOfCheckpointsToRetain = CoreOptions.MAX_RETAINED_CHECKPOINTS.defaultValue();
				}

				completedCheckpoints = recoveryFactory.createCheckpointStore(jobId, maxNumberOfCheckpointsToRetain, classLoader);
				checkpointIdCounter = recoveryFactory.createCheckpointIDCounter(jobId);
			}
			catch (Exception e) {
				throw new JobExecutionException(jobId, "Failed to initialize high-availability checkpoint handler", e);
			}

			// Maximum number of remembered checkpoints
			int historySize = jobManagerConfig.getInteger(WebOptions.CHECKPOINTS_HISTORY_SIZE);

			CheckpointStatsTracker checkpointStatsTracker = new CheckpointStatsTracker(
					historySize,
					ackVertices,
					snapshotSettings.getCheckpointCoordinatorConfiguration(),
					metrics);

			// The default directory for externalized checkpoints
			String externalizedCheckpointsDir = jobManagerConfig.getString(CoreOptions.CHECKPOINTS_DIRECTORY);

			// load the state backend for checkpoint metadata.
			// if specified in the application, use from there, otherwise load from configuration
			final StateBackend metadataBackend;

			final SerializedValue<StateBackend> applicationConfiguredBackend = snapshotSettings.getDefaultStateBackend();
			if (applicationConfiguredBackend != null) {
				try {
					metadataBackend = applicationConfiguredBackend.deserializeValue(classLoader);
				} catch (IOException | ClassNotFoundException e) {
					throw new JobExecutionException(jobId, "Could not instantiate configured state backend.", e);
				}

				log.info("Using application-defined state backend for checkpoint/savepoint metadata: {}.",
					metadataBackend);
			} else {
				try {
					metadataBackend = AbstractStateBackend
							.loadStateBackendFromConfigOrCreateDefault(jobManagerConfig, classLoader, log);
				} catch (IllegalConfigurationException | IOException | DynamicCodeLoadingException e) {
					throw new JobExecutionException(jobId, "Could not instantiate configured state backend", e);
				}
			}

			// instantiate the user-defined checkpoint hooks

			final SerializedValue<MasterTriggerRestoreHook.Factory[]> serializedHooks = snapshotSettings.getMasterHooks();
			final List<MasterTriggerRestoreHook<?>> hooks;

			if (serializedHooks == null) {
				hooks = Collections.emptyList();
			}
			else {
				final MasterTriggerRestoreHook.Factory[] hookFactories;
				try {
					hookFactories = serializedHooks.deserializeValue(classLoader);
				}
				catch (IOException | ClassNotFoundException e) {
					throw new JobExecutionException(jobId, "Could not instantiate user-defined checkpoint hooks", e);
				}

				final Thread thread = Thread.currentThread();
				final ClassLoader originalClassLoader = thread.getContextClassLoader();
				thread.setContextClassLoader(classLoader);

				try {
					hooks = new ArrayList<>(hookFactories.length);
					for (MasterTriggerRestoreHook.Factory factory : hookFactories) {
						hooks.add(MasterHooks.wrapHook(factory.create(), classLoader));
					}
				}
				finally {
					thread.setContextClassLoader(originalClassLoader);
				}
			}

			final CheckpointCoordinatorConfiguration chkConfig = snapshotSettings.getCheckpointCoordinatorConfiguration();

			executionGraph.enableCheckpointing(
				chkConfig.getCheckpointInterval(),
				chkConfig.getCheckpointTimeout(),
				chkConfig.getMinPauseBetweenCheckpoints(),
				chkConfig.getMaxConcurrentCheckpoints(),
				chkConfig.getExternalizedCheckpointSettings(),
				triggerVertices,
				ackVertices,
				confirmVertices,
				hooks,
				checkpointIdCounter,
				completedCheckpoints,
				externalizedCheckpointsDir,
				metadataBackend,
				checkpointStatsTracker);
		}

		// create all the metrics for the Execution Graph
		/** 为{@code ExecutionGraph}创建所有的metrics */
		metrics.gauge(RestartTimeGauge.METRIC_NAME, new RestartTimeGauge(executionGraph));
		metrics.gauge(DownTimeGauge.METRIC_NAME, new DownTimeGauge(executionGraph));
		metrics.gauge(UpTimeGauge.METRIC_NAME, new UpTimeGauge(executionGraph));
		metrics.gauge(NumberOfFullRestartsGauge.METRIC_NAME, new NumberOfFullRestartsGauge(executionGraph));

		executionGraph.getFailoverStrategy().registerMetrics(metrics);

		return executionGraph;
	}

	private static List<ExecutionJobVertex> idToVertex(
			List<JobVertexID> jobVertices, ExecutionGraph executionGraph) throws IllegalArgumentException {

		List<ExecutionJobVertex> result = new ArrayList<>(jobVertices.size());

		for (JobVertexID id : jobVertices) {
			ExecutionJobVertex vertex = executionGraph.getJobVertex(id);
			if (vertex != null) {
				result.add(vertex);
			} else {
				throw new IllegalArgumentException(
						"The snapshot checkpointing settings refer to non-existent vertex " + id);
			} 
		}

		return result;
	}

	// ------------------------------------------------------------------------

	/** This class is not supposed to be instantiated */
	private ExecutionGraphBuilder() {}
}
