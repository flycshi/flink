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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.state.OperatorStateStore;

import java.io.Closeable;

/**
 * Interface that combines both, the user facing {@link OperatorStateStore} interface and the system interface
 * {@link Snapshotable}
 * 接口结合了这两个接口，用户面对{@code OperatorStateStore}接口和系统接口{@code Snapshotable}
 *
 */
public interface OperatorStateBackend extends OperatorStateStore, Snapshotable<OperatorStateHandle>, Closeable {

	/**
	 * Disposes the backend and releases all resources.
	 * 处置后端，并释放资源
	 */
	void dispose();

}
