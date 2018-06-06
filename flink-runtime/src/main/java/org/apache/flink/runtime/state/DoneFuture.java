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
package org.apache.flink.runtime.state;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A {@link Future} that is always done and will just yield the object that was given at creation
 * time.
 * 总是处于完成状态的Future，并在创建时指定对象。
 *
 * @param <T> The type of object in this {@code Future}.
 */
public class DoneFuture<T> implements RunnableFuture<T> {

	private static final DoneFuture<?> NULL_FUTURE = new DoneFuture<Object>(null);

	private final T payload;

	public DoneFuture(T payload) {
		this.payload = payload;
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		return false;
	}

	@Override
	public boolean isCancelled() {
		return false;
	}

	@Override
	public boolean isDone() {
		return true;
	}

	@Override
	public T get() throws InterruptedException, ExecutionException {
		return payload;
	}

	@Override
	public T get(
			long timeout,
			TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		return get();
	}

	@Override
	public void run() {

	}

	@SuppressWarnings("unchecked")
	public static <T> DoneFuture<T> nullValue() {
		return (DoneFuture<T>) NULL_FUTURE;
	}
}
