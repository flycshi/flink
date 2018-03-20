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

package org.apache.flink.runtime.rpc;

import org.apache.flink.api.common.time.Time;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

/**
 * Base class for fenced {@link RpcEndpoint}. A fenced rpc endpoint expects all rpc messages
 * being enriched with fencing tokens. Furthermore, the rpc endpoint has its own fencing token
 * assigned. The rpc is then only executed if the attached fencing token equals the endpoint's own
 * token.
 * fenced {@link RpcEndpoint}的基类。
 * 一个fenced rpc端点希望所有的rpc消息都添加了fencing令牌。
 * 此外，rpc端点有自己的分配fencing令牌。
 * 只有当附加的fencing令牌等于端点本身的令牌时，才会执行rpc。
 *
 * @param <F> type of the fencing token
 *           围墙令牌的类型
 */
public class FencedRpcEndpoint<F extends Serializable> extends RpcEndpoint {

	private volatile F fencingToken;
	private volatile MainThreadExecutor fencedMainThreadExecutor;

	protected FencedRpcEndpoint(RpcService rpcService, String endpointId) {
		super(rpcService, endpointId);

		// no fencing token == no leadership
		// 没有围墙令牌 == 没有leader
		this.fencingToken = null;
		this.fencedMainThreadExecutor = new MainThreadExecutor(
			getRpcService().fenceRpcServer(
				rpcServer,
				null));
	}

	protected FencedRpcEndpoint(RpcService rpcService) {
		this(rpcService, UUID.randomUUID().toString());
	}

	public F getFencingToken() {
		return fencingToken;
	}

	protected void setFencingToken(@Nullable F newFencingToken) {
		// this method should only be called from within the main thread
		// 这个方法应该只会在主线程汇总调用
		validateRunsInMainThread();

		this.fencingToken = newFencingToken;

		// setting a new fencing token entails that we need a new MainThreadExecutor
		// which is bound to the new fencing token
		/** 设置一个新的围墙令牌, 导致我们需要一个新的{@code MainThreadExecutor}, 其需要绑定到新的围墙令牌上 */
		MainThreadExecutable mainThreadExecutable = getRpcService().fenceRpcServer(
			rpcServer,
			newFencingToken);

		this.fencedMainThreadExecutor = new MainThreadExecutor(mainThreadExecutable);
	}

	/**
	 * Returns a main thread executor which is bound to the currently valid fencing token.
	 * This means that runnables which are executed with this executor fail after the fencing
	 * token has changed. This allows to scope operations by the fencing token.
	 * 返回一个绑定到当前有效围墙令牌上的主线程执行器。
	 * 这意味着在围墙令牌发生变化后，在该执行器中执行的runnables将失败。
	 * 这允许使用围墙令牌来进行范围操作。
	 *
	 * @return MainThreadExecutor bound to the current fencing token
	 */
	@Override
	protected MainThreadExecutor getMainThreadExecutor() {
		return fencedMainThreadExecutor;
	}

	/**
	 * Run the given runnable in the main thread of the RpcEndpoint without checking the fencing
	 * token. This allows to run operations outside of the fencing token scope.
	 * 在{@code RpcEndpoint}的主线程中, 不做围墙令牌检查, 就执行给定的runnable。
	 * 这个就允许运行在围墙令牌范围之外的操作。
	 *
	 * @param runnable to execute in the main thread of the rpc endpoint without checking the fencing token.
	 */
	protected void runAsyncWithoutFencing(Runnable runnable) {
		if (rpcServer instanceof FencedMainThreadExecutable) {
			((FencedMainThreadExecutable) rpcServer).runAsyncWithoutFencing(runnable);
		} else {
			throw new RuntimeException("FencedRpcEndpoint has not been started with a FencedMainThreadExecutable RpcServer.");
		}
	}

	/**
	 * Run the given callable in the main thread of the RpcEndpoint without checking the fencing
	 * token. This allows to run operations outside of the fencing token scope.
	 * 在{@code RpcEndpoint}的主线程中, 不做围墙令牌检查, 就执行给定的callable。
	 * 这个就允许运行在围墙令牌范围之外的操作。
	 *
	 * @param callable to run in the main thread of the rpc endpoint without checkint the fencing token.
	 * @param timeout for the operation.
	 * @return Future containing the callable result.
	 */
	protected <V> CompletableFuture<V> callAsyncWithoutFencing(Callable<V> callable, Time timeout) {
		if (rpcServer instanceof FencedMainThreadExecutable) {
			return ((FencedMainThreadExecutable) rpcServer).callAsyncWithoutFencing(callable, timeout);
		} else {
			throw new RuntimeException("FencedRpcEndpoint has not been started with a FencedMainThreadExecutable RpcServer.");
		}
	}
}
