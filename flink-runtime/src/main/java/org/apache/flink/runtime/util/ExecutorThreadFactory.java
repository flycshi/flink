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

package org.apache.flink.runtime.util;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A thread factory intended for use by critical thread pools. Critical thread pools here
 * mean thread pools that support Flink's core coordination and processing work, and which
 * must not simply cause unnoticed errors.
 * 准备被严格线程池使用的一个线程工厂。
 * 这里的严格线程池是指支持Flink的核心协调和处理工作的线程池, 并且必须不能导致被忽视的错误。
 *
 * 
 * <p>The thread factory can be given an {@link UncaughtExceptionHandler} for the threads.
 * If no handler is explicitly given, the default handler for uncaught exceptions will log
 * the exceptions and kill the process afterwards. That guarantees that critical exceptions are
 * not accidentally lost and leave the system running in an inconsistent state.
 * 线程池可以给线程提供一个{@code UncaughtExceptionHandler}。
 * 如果没有具备被显式提供, 默认的句柄会打印日志, 并kill掉进程。
 * 保障不会丢失故障, 然后让系统在不一致状态下运行。
 * 
 * <p>Threads created by this factory are all called '(pool-name)-thread-n', where
 * <i>(pool-name)</i> is configurable, and <i>n</i> is an incrementing number.
 * 
 * <p>All threads created by this factory are daemon threads and have the default (normal)
 * priority.
 */
public class ExecutorThreadFactory implements ThreadFactory {

	/** The thread pool name used when no explicit pool name has been specified */ 
	private static final String DEFAULT_POOL_NAME = "flink-executor-pool";

	private final AtomicInteger threadNumber = new AtomicInteger(1);

	private final ThreadGroup group;

	private final String namePrefix;

	private final UncaughtExceptionHandler exceptionHandler;

	// ------------------------------------------------------------------------

	/**
	 * Creates a new thread factory using the default thread pool name ('flink-executor-pool')
	 * and the default uncaught exception handler (log exception and kill process).
	 */
	public ExecutorThreadFactory() {
		this(DEFAULT_POOL_NAME);
	}

	/**
	 * Creates a new thread factory using the given thread pool name and the default
	 * uncaught exception handler (log exception and kill process).
	 * 
	 * @param poolName The pool name, used as the threads' name prefix
	 */
	public ExecutorThreadFactory(String poolName) {
		this(poolName, FatalExitExceptionHandler.INSTANCE);
	}

	/**
	 * Creates a new thread factory using the given thread pool name and the given
	 * uncaught exception handler.
	 * 
	 * @param poolName The pool name, used as the threads' name prefix
	 * @param exceptionHandler The uncaught exception handler for the threads
	 */
	public ExecutorThreadFactory(String poolName, UncaughtExceptionHandler exceptionHandler) {
		checkNotNull(poolName, "poolName");

		SecurityManager securityManager = System.getSecurityManager();
		this.group = (securityManager != null) ? securityManager.getThreadGroup() :
				Thread.currentThread().getThreadGroup();

		this.namePrefix = poolName + "-thread-";
		this.exceptionHandler = exceptionHandler;
	}

	// ------------------------------------------------------------------------

	@Override
	public Thread newThread(Runnable runnable) {
		Thread t = new Thread(group, runnable, namePrefix + threadNumber.getAndIncrement());
		t.setDaemon(true);

		// normalize the priority
		if (t.getPriority() != Thread.NORM_PRIORITY) {
			t.setPriority(Thread.NORM_PRIORITY);
		}

		// optional handler for uncaught exceptions
		if (exceptionHandler != null) {
			t.setUncaughtExceptionHandler(exceptionHandler);
		}

		return t;
	}

	// --------------------------------------------------------------------------------------------

}
