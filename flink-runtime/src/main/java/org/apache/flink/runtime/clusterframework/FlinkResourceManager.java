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

package org.apache.flink.runtime.clusterframework;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.dispatch.OnComplete;
import akka.pattern.Patterns;
import akka.util.Timeout;

import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.akka.FlinkUntypedActor;
import org.apache.flink.runtime.clusterframework.messages.CheckAndAllocateContainers;
import org.apache.flink.runtime.clusterframework.messages.FatalErrorOccurred;
import org.apache.flink.runtime.clusterframework.messages.InfoMessage;
import org.apache.flink.runtime.clusterframework.messages.NewLeaderAvailable;
import org.apache.flink.runtime.clusterframework.messages.NotifyResourceStarted;
import org.apache.flink.runtime.clusterframework.messages.RegisterInfoMessageListener;
import org.apache.flink.runtime.clusterframework.messages.RegisterInfoMessageListenerSuccessful;
import org.apache.flink.runtime.clusterframework.messages.RegisterResourceManager;
import org.apache.flink.runtime.clusterframework.messages.RegisterResourceManagerSuccessful;
import org.apache.flink.runtime.clusterframework.messages.RemoveResource;
import org.apache.flink.runtime.clusterframework.messages.ResourceRemoved;
import org.apache.flink.runtime.clusterframework.messages.SetWorkerPoolSize;
import org.apache.flink.runtime.clusterframework.messages.StopCluster;
import org.apache.flink.runtime.clusterframework.messages.StopClusterSuccessful;
import org.apache.flink.runtime.clusterframework.messages.TriggerRegistrationAtJobManager;
import org.apache.flink.runtime.clusterframework.messages.UnRegisterInfoMessageListener;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceIDRetrievable;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.JobManagerMessages.LeaderSessionMessage;

import org.apache.flink.util.Preconditions;

import scala.concurrent.Future;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

/**
 *
 * <h1>Worker allocation steps</h1>
 * worker 分配步骤
 *
 * <ol>
 *     <li>The resource manager decides to request more workers. This can happen in order
 *         to fill the initial pool, or as a result of the JobManager requesting more workers.</li>
 *         资源管理器决定请求更多的workers。
 *         这么做可能是为了填充初始池, 或者由于JobManager请求更多的workers。
 *
 *     <li>The resource master calls {@link #requestNewWorkers(int)}, which triggers requests
 *         for more containers. After that, the {@link #getNumWorkerRequestsPending()}
 *         should reflect the pending requests.</li>
 *         resource master 调用 requestNewWorkers(int) 方法, 会触发请求更多的容器。
 *         之后, getNumWorkerRequestsPending() 方法应该反映出悬挂着的请求。
 *
 *     <li>The concrete framework may acquire containers and then trigger to start TaskManagers
 *         in those containers. That should be reflected in {@link #getNumWorkersPendingRegistration()}.</li>
 *         具体的框架可能获得容器,然后触发启动容器中的TaskManager。 getNumWorkersPendingRegistration 方法中应该有所反映。
 *
 *     <li>At some point, the TaskManager processes will have started and send a registration
 *         message to the JobManager. The JobManager will perform
 *         a lookup with the ResourceManager to check if it really started this TaskManager.
 *         The method {@link #workerStarted(ResourceID)} will be called
 *         to inform about a registered worker.</li>
 *         在某个时刻, TaskManager进程将启动并发送一个注册消息给JobManager。JobManager将向ResourceManager进行查询,
 *         以检测它是否真正的启动了这个TaskManager。workerStarted 方法会被调用来通知注册的worker。
 * </ol>
 *
 */
public abstract class FlinkResourceManager<WorkerType extends ResourceIDRetrievable> extends FlinkUntypedActor {

	/**
	 * The exit code with which the process is stopped in case of a fatal error
	 * 在致命错误情况下进程停止时的退出码
	 */
	protected static final int EXIT_CODE_FATAL_ERROR = -13;

	/**
	 * The default name of the resource manager actor
	 * resource manager actor的默认名称
	 */
	public static final String RESOURCE_MANAGER_NAME = "resourcemanager";

	// ------------------------------------------------------------------------

	/**
	 * The Flink configuration object
	 * flink的配置对象
	 */
	protected final Configuration config;

	/**
	 * The timeout for actor messages sent to the JobManager / TaskManagers
	 * 向JobManager和TaskManager发送消息的超时时间。
	 */
	private final FiniteDuration messageTimeout;

	/**
	 * The service to find the right leader JobManager (to support high availability)
	 * 发现正确的leader JobManager的服务(支持ha)
	 */
	private final LeaderRetrievalService leaderRetriever;

	/**
	 * Map which contains the workers from which we know that they have been successfully started in a container.
	 * 包含了我们知道他们已经成功地从一个容器中启动的worker的map。
	 * This notification is sent by the JM when a TM tries to register at it.
	 * 当一个 TaskManager 尝试注册时, 这个通知是由 JobManager 发送的
	 */
	private final Map<ResourceID, WorkerType> startedWorkers;

	/**
	 * List of listeners for info messages
	 * info消息的监听者列表
	 */
	private final Set<ActorRef> infoMessageListeners;

	/**
	 * The JobManager that the framework master manages resources for
	 * 管理资源的JobManager
	 */
	private ActorRef jobManager;

	/**
	 * Our JobManager's leader session
	 * 我们的JobManager的leader session id
	 */
	private UUID leaderSessionID;

	/**
	 * The size of the worker pool that the resource master strives to maintain
	 * 资源主努力维持的工作池的大小
	 */
	private int designatedPoolSize;

	// ------------------------------------------------------------------------

	/**
	 * Creates a AbstractFrameworkMaster actor.
	 *
	 * @param flinkConfig The Flink configuration object.
	 */
	protected FlinkResourceManager(
			int numInitialTaskManagers,
			Configuration flinkConfig,
			LeaderRetrievalService leaderRetriever) {
		this.config = requireNonNull(flinkConfig);
		this.leaderRetriever = requireNonNull(leaderRetriever);
		this.startedWorkers = new HashMap<>();

		/**
		 * 通过配置参数 akka.lookup.timeout 获取超时时间设置, 如果没有设置, 则采用默认值。
		 */
		FiniteDuration lt;
		try {
			lt = AkkaUtils.getLookupTimeout(config);
		}
		catch (Exception e) {
			lt = new FiniteDuration(
				Duration.apply(AkkaOptions.LOOKUP_TIMEOUT.defaultValue()).toMillis(),
				TimeUnit.MILLISECONDS);
		}
		this.messageTimeout = lt;
		this.designatedPoolSize = numInitialTaskManagers;
		this.infoMessageListeners = new HashSet<>();
	}

	// ------------------------------------------------------------------------
	//  Actor Behavior
	//  actor的行为
	// ------------------------------------------------------------------------

	@Override
	public void preStart() {
		try {
			// we start our leader retrieval service to make sure we get informed
			// about JobManager leader changes
			/** 启动leader提取服务, 确保JobManager的leader变化时, 我们能获得通知。 */
			leaderRetriever.start(new LeaderRetrievalListener() {

				@Override
				public void notifyLeaderAddress(String leaderAddress, UUID leaderSessionID) {
					self().tell(
						new NewLeaderAvailable(leaderAddress, leaderSessionID),
						ActorRef.noSender());
				}

				@Override
				public void handleError(Exception e) {
					self().tell(
						new FatalErrorOccurred("Leader retrieval service failed", e),
						ActorRef.noSender());
				}
			});

			// framework specific initialization
			// 框架特定的初始化
			initialize();

		}
		catch (Throwable t) {
			self().tell(
				new FatalErrorOccurred("Error during startup of ResourceManager actor", t),
				ActorRef.noSender());
		}
	}

	@Override
	public void postStop() {
		try {
			leaderRetriever.stop();
		}
		catch (Throwable t) {
			LOG.error("Could not cleanly shut down leader retrieval service", t);
		}
	}

	/**
	 *
	 * This method receives the actor messages after they have been filtered for
	 * a match with the leader session.
	 * 该方法接收已经被过滤过,匹配携带了leader session的actor的消息
	 *
	 * @param message The incoming actor message.
	 */
	@Override
	protected void handleMessage(Object message) {
		try {
			// --- messages about worker allocation and pool sizes
			//  worker分配和池大小的消息

			if (message instanceof CheckAndAllocateContainers) {
				checkWorkersPool();
			}
			else if (message instanceof SetWorkerPoolSize) {
				SetWorkerPoolSize msg = (SetWorkerPoolSize) message;
				adjustDesignatedNumberOfWorkers(msg.numberOfWorkers());
			}
			else if (message instanceof RemoveResource) {
				RemoveResource msg = (RemoveResource) message;
				removeRegisteredResource(msg.resourceId());
			}

			// --- lookup of registered resources
			//  搜寻已经注册的资源

			else if (message instanceof NotifyResourceStarted) {
				NotifyResourceStarted msg = (NotifyResourceStarted) message;
				handleResourceStarted(sender(), msg.getResourceID());
			}

			// --- messages about JobManager leader status and registration
			//  关于JobManager leader的状态和注册的消息

			/**
			 * 1、首先应该是会收到 NewLeaderAvailable 消息, 触发新leader JobManager的处理;
			 * 2、如果上述处理连接失败,会给自己发送一个 TriggerRegistrationAtJobManager(自己给自己发的) 消息, 尝试再连接
			 * 3、如果连接成功,则会收到 RegisterResourceManagerSuccessful(自己给自己发的) 消息, 处理注册成功的状态
			 */
			else if (message instanceof NewLeaderAvailable) {
				NewLeaderAvailable msg = (NewLeaderAvailable) message;
				newJobManagerLeaderAvailable(msg.leaderAddress(), msg.leaderSessionId());
			}
			else if (message instanceof TriggerRegistrationAtJobManager) {
				TriggerRegistrationAtJobManager msg = (TriggerRegistrationAtJobManager) message;
				triggerConnectingToJobManager(msg.jobManagerAddress());
			}
			else if (message instanceof RegisterResourceManagerSuccessful) {
				RegisterResourceManagerSuccessful msg = (RegisterResourceManagerSuccessful) message;
				jobManagerLeaderConnected(msg.jobManager(), msg.currentlyRegisteredTaskManagers());
			}

			// --- end of application
			// 	应用结束

			else if (message instanceof StopCluster) {
				StopCluster msg = (StopCluster) message;
				shutdownCluster(msg.finalStatus(), msg.message());
				sender().tell(decorateMessage(StopClusterSuccessful.getInstance()), ActorRef.noSender());
			}

			// --- miscellaneous messages
			// 	其他消息

			else if (message instanceof RegisterInfoMessageListener) {
				if (jobManager != null) {
					infoMessageListeners.add(sender());
					sender().tell(decorateMessage(
						RegisterInfoMessageListenerSuccessful.get()),
						// answer as the JobManager
						jobManager);
				}
			}

			else if (message instanceof UnRegisterInfoMessageListener) {
				infoMessageListeners.remove(sender());
			}

			else if (message instanceof FatalErrorOccurred) {
				FatalErrorOccurred fatalErrorOccurred = (FatalErrorOccurred) message;
				fatalError(fatalErrorOccurred.message(), fatalErrorOccurred.error());
			}

			// --- unknown messages

			else {
				LOG.error("Discarding unknown message: {}", message);
			}
		}
		catch (Throwable t) {
			// fatal error, needs master recovery
			fatalError("Error processing actor message", t);
		}
	}

	@Override
	protected final UUID getLeaderSessionID() {
		return leaderSessionID;
	}

	// ------------------------------------------------------------------------
	//  Status
	// ------------------------------------------------------------------------

	/**
	 * Gets the current designated worker pool size, meaning the number of workers
	 * that the resource master strives to maintain. The actual number of workers
	 * may be lower (if worker requests are still pending) or higher (if workers have
	 * not yet been released).
	 * 获取当前设计的worker池大小, 也就是ResourceManager努力去管理的worker的数量。
	 * 实际的worker数量可能会少(如果worker的请求还在pending) 或者 多(如果worker还没有被释放)
	 *
	 * @return The designated worker pool size.
	 */
	public int getDesignatedWorkerPoolSize() {
		return designatedPoolSize;
	}

	/**
	 * Gets the number of currently started TaskManagers.
	 * 获取当前已经启动的TaskManager的数量
	 *
	 * @return The number of currently started TaskManagers.
	 */
	public int getNumberOfStartedTaskManagers() {
		return startedWorkers.size();
	}

	/**
	 * Gets the currently registered resources.
	 * 获取当前已经注册的resources
	 * @return
	 */
	public Collection<WorkerType> getStartedTaskManagers() {
		return startedWorkers.values();
	}

	/**
	 * Gets the started worker for a given resource ID, if one is available.
	 * 对给定的resourceId,判断其是否启动了
	 *
	 * @param resourceId The resource ID for the worker.
	 * @return True if already registered, otherwise false
	 */
	public boolean isStarted(ResourceID resourceId) {
		return startedWorkers.containsKey(resourceId);
	}

	/**
	 * Gets an iterable for all currently started TaskManagers.
	 * 获取当前所有启动TaskManager的一个迭代器
	 *
	 * @return All currently started TaskManagers.
	 */
	public Collection<WorkerType> allStartedWorkers() {
		return startedWorkers.values();
	}

	/**
	 * Tells the ResourceManager that a TaskManager had been started in a container with the given
	 * resource id.
	 * 告诉 ResourceManager , 一个 TaskManager 已经在容器中启动, 以及 resource id
	 *
	 * @param jobManager The sender (JobManager) of the message
	 * @param resourceID The resource id of the started TaskManager
	 */
	private void handleResourceStarted(ActorRef jobManager, ResourceID resourceID) {
		if (resourceID != null) {
			// check if resourceID is already registered (TaskManager may send duplicate register messages)
			// 检查resourceID是否已经被注册了(TaskManager可能发送了两次注册消息)
			WorkerType oldWorker = startedWorkers.get(resourceID);
			if (oldWorker != null) {
				LOG.debug("Notification that TaskManager {} had been started was sent before.", resourceID);
			} else {
				WorkerType newWorker = workerStarted(resourceID);

				if (newWorker != null) {
					startedWorkers.put(resourceID, newWorker);
					LOG.info("TaskManager {} has started.", resourceID);
				} else {
					LOG.info("TaskManager {} has not been started by this resource manager.", resourceID);
				}
			}
		}

		// Acknowledge the resource registration
		jobManager.tell(decorateMessage(Acknowledge.get()), self());
	}

	/**
	 * Releases the given resource. Note that this does not automatically shrink
	 * the designated worker pool size.
	 * 释放指定的资源。
	 * 需要注意的是, 这个操作不会自动缩小设计的worker池大小。
	 *
	 * @param resourceId The TaskManager's resource id.
	 */
	private void removeRegisteredResource(ResourceID resourceId) {

		WorkerType worker = startedWorkers.remove(resourceId);
		if (worker != null) {
			releaseStartedWorker(worker);
		} else {
			LOG.warn("Resource {} could not be released", resourceId);
		}
	}


	// ------------------------------------------------------------------------
	//  Registration and consolidation with JobManager Leader
	//  与JobManager的leader进行注册与合作。
	// ------------------------------------------------------------------------

	/**
	 * Called as soon as we discover (via leader election) that a JobManager lost leadership
	 * or a different one gained leadership.
	 * 只要我们发现(通过leader选择器)一个JobManager丢失了leader权,或者一个不同的人获得了leader权, 则被调用。
	 *
	 * @param leaderAddress The address (Akka URL) of the new leader. Null if there is currently no leader.
	 * @param leaderSessionID The unique session ID marking the leadership session.
	 */
	private void newJobManagerLeaderAvailable(String leaderAddress, UUID leaderSessionID) {
		LOG.debug("Received new leading JobManager {}. Connecting.", leaderAddress);

		// disconnect from the current leader (no-op if no leader yet)
		// 从当前leader断开连接(如果没有leader则不做操作)
		jobManagerLostLeadership();

		// a null leader session id means that only a leader disconnect
		// happened, without a new leader yet
		/**
		 * 一个 null leader会话id, 意味着leader断连,但是没有新的leader被选择出来
		 */
		if (leaderSessionID != null && leaderAddress != null) {
			// the leaderSessionID implicitly filters out success and failure messages
			// that come after leadership changed again
			/** leaderSessionID 会隐式的过滤那些在leader变化后的成功或失败的消息 */
			this.leaderSessionID = leaderSessionID;
			triggerConnectingToJobManager(leaderAddress);
		}
	}

	/**
	 * Causes the resource manager to announce itself at the new leader JobManager and
	 * obtains its connection information and currently known TaskManagers.
	 * 让resource manager在新的leader JobManager中申明自己, 并获取其连接信息和当前已知的TaskManager。
	 *
	 * @param leaderAddress The akka actor URL of the new leader JobManager.
	 */
	protected void triggerConnectingToJobManager(String leaderAddress) {

		LOG.info("Trying to associate with JobManager leader " + leaderAddress);

		final Object registerMessage = decorateMessage(new RegisterResourceManager(self()));
		final Object retryMessage = decorateMessage(new TriggerRegistrationAtJobManager(leaderAddress));

		// send the registration message to the JobManager
		ActorSelection jobManagerSel = context().actorSelection(leaderAddress);
		Future<Object> future = Patterns.ask(jobManagerSel, registerMessage, new Timeout(messageTimeout));

		future.onComplete(new OnComplete<Object>() {

			@Override
			public void onComplete(Throwable failure, Object msg) {
				// only process if we haven't been connected in the meantime
				if (jobManager == null) {
					if (msg != null) {
						if (msg instanceof LeaderSessionMessage &&
							((LeaderSessionMessage) msg).message() instanceof RegisterResourceManagerSuccessful) {
							self().tell(msg, ActorRef.noSender());
						} else {
							LOG.error("Invalid response type to registration at JobManager: {}", msg);
							self().tell(retryMessage, ActorRef.noSender());
						}
					} else {
						// no success
						LOG.error("Resource manager could not register at JobManager", failure);
						self().tell(retryMessage, ActorRef.noSender());
					}
				}
			}

		}, context().dispatcher());
	}

	/**
	 * This method disassociates from the current leader JobManager.
	 * 该方法取消与当前leader JobManager的关联
	 */
	private void jobManagerLostLeadership() {
		if (jobManager != null) {
			LOG.info("Associated JobManager {} lost leader status", jobManager);

			jobManager = null;
			leaderSessionID = null;

			infoMessageListeners.clear();
		}
	}

	/**
	 * Callback when we're informed about a new leading JobManager.
	 * 在被通知一个新的leading JobManager时, 被调用
	 *
	 * @param newJobManagerLeader The ActorRef of the new jobManager
	 * @param workers The existing workers the JobManager has registered.
	 */
	private void jobManagerLeaderConnected(
						ActorRef newJobManagerLeader,
						Collection<ResourceID> workers) {

		if (jobManager == null) {
			LOG.info("Resource Manager associating with leading JobManager {} - leader session {}",
						newJobManagerLeader, leaderSessionID);

			jobManager = newJobManagerLeader;

			if (workers.size() > 0) {
				LOG.info("Received TaskManagers that were registered at the leader JobManager. " +
						"Trying to consolidate.");

				// keep track of which TaskManagers are not handled
				Set<ResourceID> toHandle = new HashSet<>(workers.size());
				toHandle.addAll(workers);

				try {
					// ask the framework to tell us which ones we should keep for now
					// 询问框架, 现在哪些还需要继续保留
					Collection<WorkerType> consolidated = reacceptRegisteredWorkers(workers);
					LOG.info("Consolidated {} TaskManagers", consolidated.size());

					// put the consolidated TaskManagers into our bookkeeping
					for (WorkerType worker : consolidated) {
						ResourceID resourceID = worker.getResourceID();
						startedWorkers.put(resourceID, worker);
						toHandle.remove(resourceID);
					}
				}
				catch (Throwable t) {
					LOG.error("Error during consolidation of known TaskManagers", t);
					// the framework should release the remaining unclear resources
					for (ResourceID id : toHandle) {
						releasePendingWorker(id);
					}
				}

				/**
				 * 对于 toHandle 中可能剩余的 ResourceID 要如何处理
				 * ????????
				 */

			}

			// trigger initial check for requesting new workers
			checkWorkersPool();

		} else {
			String msg = "Attempting to associate with new JobManager leader " + newJobManagerLeader
				+ " without previously disassociating from current leader " + jobManager;
			fatalError(msg, new Exception(msg));
		}
	}

	// ------------------------------------------------------------------------
	//  ClusterClient Shutdown
	//  集群客户端关闭
	// ------------------------------------------------------------------------

	private void shutdownCluster(ApplicationStatus status, String diagnostics) {
		LOG.info("Shutting down cluster with status {} : {}", status, diagnostics);

		shutdownApplication(status, diagnostics);
	}

	// ------------------------------------------------------------------------
	//  Worker pool size management
	// ------------------------------------------------------------------------

	/**
	 * This method causes the resource framework master to <b>synchronously</b>re-examine
	 * the set of available and pending workers containers, and allocate containers
	 * if needed.
	 * 该方法使资源管理框架同步的重新检查可用的和挂起的worker的容器, 并在需要时分配容器。
	 *
	 * This method does not automatically release workers, because it is not visible to
	 * this resource master which workers can be released. Instead, the JobManager must
	 * explicitly release individual workers.
	 * 这个方法不会自动释放workers, 因为对于那些workers可以被释放,它是不知道的。
	 * 相反, JobManager必须显示的释放单个worker。
	 */
	private void checkWorkersPool() {
		int numWorkersPending = getNumWorkerRequestsPending();
		int numWorkersPendingRegistration = getNumWorkersPendingRegistration();

		// sanity checks
		Preconditions.checkState(numWorkersPending >= 0,
			"Number of pending workers should never be below 0.");
		Preconditions.checkState(numWorkersPendingRegistration >= 0,
			"Number of pending workers pending registration should never be below 0.");

		// see how many workers we want, and whether we have enough
		int allAvailableAndPending = startedWorkers.size() +
			numWorkersPending + numWorkersPendingRegistration;

		int missing = designatedPoolSize - allAvailableAndPending;

		if (missing > 0) {
			requestNewWorkers(missing);
		}
	}

	/**
	 * Sets the designated worker pool size. If this size is larger than the current pool
	 * size, then the resource manager will try to acquire more TaskManagers.
	 * 设置配置的worker池大小。
	 * 如果这个size比当前池大小还要大的话, 资源管理器会尝试获取更多的TaskManager
	 *
	 * @param num The number of workers in the pool.
	 */
	private void adjustDesignatedNumberOfWorkers(int num) {
		if (num >= 0) {
			LOG.info("Adjusting designated worker pool size to {}", num);
			designatedPoolSize = num;
			checkWorkersPool();
		} else {
			LOG.warn("Ignoring invalid designated worker pool size: " + num);
		}
	}

	// ------------------------------------------------------------------------
	//  Callbacks
	//  回调方法
	// ------------------------------------------------------------------------

	/**
	 * This method causes the resource framework master to <b>asynchronously</b>re-examine
	 * the set of available and pending workers containers, and release or allocate
	 * containers if needed. The method sends an actor message which will trigger the
	 * re-examination.
	 * 该方法会让资源框架master异步去检查有效的和悬挂的worker容器的集合, 如果需要的话, 释放或者分配容器。
	 * 该方法会发送一个actor消息,该消息会触发重新检查。
	 */
	public void triggerCheckWorkers() {
		self().tell(
			decorateMessage(
				CheckAndAllocateContainers.get()),
			ActorRef.noSender());
	}

	/**
	 * This method should be called by the framework once it detects that a currently registered
	 * worker has failed.
	 * 该方法是在框架检查到当前一个注册的worker失败时,会被回调。
	 *
	 * @param resourceID Id of the worker that has failed.
	 * @param message An informational message that explains why the worker failed.
	 */
	public void notifyWorkerFailed(ResourceID resourceID, String message) {
		WorkerType worker = startedWorkers.remove(resourceID);
		if (worker != null) {
			jobManager.tell(
				decorateMessage(
					new ResourceRemoved(resourceID, message)),
				self());
		}
	}

	// ------------------------------------------------------------------------
	//  Framework specific behavior
	//  框架特定行为
	// ------------------------------------------------------------------------

	/**
	 * Initializes the framework specific components.
	 * 初始化框架特定组件
	 *
	 * @throws Exception Exceptions during initialization cause the resource manager to fail.
	 *                   If the framework is able to recover this resource manager, it will be
	 *                   restarted.
	 */
	protected abstract void initialize() throws Exception;

	/**
	 * The framework specific code for shutting down the application. This should report the
	 * application's final status and shut down the resource manager cleanly.
	 * 框架指定的code,用来关闭应用。
	 * 这里应该报告应用的最终状态,并完全关闭resource manager
	 *
	 * This method also needs to make sure all pending containers that are not registered
	 * yet are returned.
	 * 这个方法也需要确保所有处于pending状态的容器,也就是还没有完成注册的,也需要被返回。
	 *
	 * @param finalStatus The application status to report.
	 * @param optionalDiagnostics An optional diagnostics message.
	 */
	protected abstract void shutdownApplication(ApplicationStatus finalStatus, String optionalDiagnostics);

	/**
	 * Notifies the resource master of a fatal error.
	 * 通知ResourceManager一个致命错误
	 *
	 * <p><b>IMPORTANT:</b> This should not cleanly shut down this master, but exit it in
	 * such a way that a high-availability setting would restart this or fail over
	 * to another master.
	 */
	protected abstract void fatalError(String message, Throwable error);

	/**
	 * Requests to allocate a certain number of new workers.
	 * 请求分配给定数量的新workers
	 *
	 * @param numWorkers The number of workers to allocate.
	 */
	protected abstract void requestNewWorkers(int numWorkers);

	/**
	 * Trigger a release of a pending worker.
	 * 触发一个pending worker的释放
	 *
	 * @param resourceID The worker resource id
	 */
	protected abstract void releasePendingWorker(ResourceID resourceID);

	/**
	 * Trigger a release of a started worker.
	 * 触发释放一个已经启动的worker
	 *
	 * @param resourceID The worker resource id
	 */
	protected abstract void releaseStartedWorker(WorkerType resourceID);

	/**
	 * Callback when a worker was started.
	 * 当一个worker被启动后,调用
	 *
	 * @param resourceID The worker resource id
	 */
	protected abstract WorkerType workerStarted(ResourceID resourceID);

	/**
	 * This method is called when the resource manager starts after a failure and reconnects to
	 * the leader JobManager, who still has some workers registered. The method is used to consolidate
	 * the view between resource manager and JobManager. The resource manager gets the list of TaskManagers
	 * that the JobManager considers available and should return a list or nodes that the
	 * resource manager considers available.
	 * 当resource manager在一次失败后启动,并重新连接到leader JobManager, 而JobManager仍然有一些已经注册的workers, 该方法会被调用。
	 * 这个方法是用来同步 ResourceManager 和 JobManager 之间的资源视图的。
	 * ResourceManager 获取 JobManager 认为有效的 TaskManager 的列表, 并返回 ResourceManager 认为有效的列表。
	 *
	 * After that, the JobManager is informed of loss of all TaskManagers that are not part of the
	 * returned list.
	 * 这之后, JobManager 会被通知到哪些 ResourceManager 认为无效的 TaskManager
	 *
	 * It is possible that the resource manager initially confirms some TaskManagers to be alive, even
	 * through they are in an uncertain status, if it later sends necessary failure notifications
	 * via calling {@link #notifyWorkerFailed(ResourceID, String)}.
	 *
	 * @param registered The list of TaskManagers that the JobManager knows.
	 * @return The subset of TaskManagers that the resource manager can confirm to be alive.
	 */
	protected abstract Collection<WorkerType> reacceptRegisteredWorkers(Collection<ResourceID> registered);

	/**
	 * Gets the number of requested workers that have not yet been granted.
	 * 获取已经请求的,但是还没有被授权的worker的数量
	 *
	 * @return The number pending worker requests. Must never be smaller than 0.
	 */
	protected abstract int getNumWorkerRequestsPending();

	/**
	 * Gets the number of containers that have been started, but where the TaskManager
	 * has not yet registered at the job manager.
	 * 获取容器已经启动, 但是里面的TaskManager还没有被注册到JobManager的容器的数量
	 *
	 * @return The number of started containers pending TaskManager registration.
	 * Must never be smaller than 0.
	 */
	protected abstract int getNumWorkersPendingRegistration();

	// ------------------------------------------------------------------------
	//  Info messaging
	// ------------------------------------------------------------------------

	protected void sendInfoMessage(String message) {
		for (ActorRef listener : infoMessageListeners) {
			listener.tell(decorateMessage(new InfoMessage(message)), self());
		}
	}


	// ------------------------------------------------------------------------
	//  Startup
	//  启动
	// ------------------------------------------------------------------------

	/**
	 * Starts the resource manager actors.
	 * @param configuration The configuration for the resource manager
	 * @param actorSystem The actor system to start the resource manager in
	 * @param leaderRetriever The leader retriever service to intialize the resource manager
	 * @param resourceManagerClass The class of the ResourceManager to be started
	 * @return ActorRef of the resource manager
	 */
	public static ActorRef startResourceManagerActors(
			Configuration configuration,
			ActorSystem actorSystem,
			LeaderRetrievalService leaderRetriever,
			Class<? extends FlinkResourceManager<?>> resourceManagerClass) {

		return startResourceManagerActors(
			configuration, actorSystem, leaderRetriever, resourceManagerClass,
			RESOURCE_MANAGER_NAME + "-" + UUID.randomUUID());
	}

	/**
	 * Starts the resource manager actors.
	 * @param configuration The configuration for the resource manager
	 * @param actorSystem The actor system to start the resource manager in
	 * @param leaderRetriever The leader retriever service to intialize the resource manager
	 * @param resourceManagerClass The class of the ResourceManager to be started
	 * @param resourceManagerActorName The name of the resource manager actor.
	 * @return ActorRef of the resource manager
	 */
	public static ActorRef startResourceManagerActors(
			Configuration configuration,
			ActorSystem actorSystem,
			LeaderRetrievalService leaderRetriever,
			Class<? extends FlinkResourceManager<?>> resourceManagerClass,
			String resourceManagerActorName) {

		Props resourceMasterProps = getResourceManagerProps(
			resourceManagerClass,
			configuration,
			leaderRetriever);

		return actorSystem.actorOf(resourceMasterProps, resourceManagerActorName);
	}

	public static Props getResourceManagerProps(
		Class<? extends FlinkResourceManager> resourceManagerClass,
		Configuration configuration,
		LeaderRetrievalService leaderRetrievalService) {

		return Props.create(resourceManagerClass, configuration, leaderRetrievalService);
	}
}
