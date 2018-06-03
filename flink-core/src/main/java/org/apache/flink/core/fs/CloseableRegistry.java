/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.core.fs;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.AbstractCloseableRegistry;

import javax.annotation.Nonnull;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;

/**
 * This class allows to register instances of {@link Closeable}, which are all closed if this registry is closed.
 * 这个类允许注册{@code Closeable}的实例，当这个注册器关闭的时候，注册的{@code Closeable}的实例也都会被关闭。
 *
 * <p>Registering to an already closed registry will throw an exception and close the provided {@link Closeable}
 * 向一个已经关闭的注册器注册，将抛出异常，并关闭提供的{@code Closeable}实例
 *
 * <p>All methods in this class are thread-safe.
 * 这个类中的方法都是线程安全的。
 */
@Internal
public class CloseableRegistry extends AbstractCloseableRegistry<Closeable, Object> {

	// 虚假的
	private static final Object DUMMY = new Object();

	public CloseableRegistry() {
		super(new HashMap<>());
	}

	@Override
	protected void doRegister(@Nonnull Closeable closeable, @Nonnull Map<Closeable, Object> closeableMap) {
		closeableMap.put(closeable, DUMMY);
	}

	@Override
	protected boolean doUnRegister(@Nonnull Closeable closeable, @Nonnull Map<Closeable, Object> closeableMap) {
		return closeableMap.remove(closeable) != null;
	}
}
