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

import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.OperatorStateStore;

/**
 * This interface provides a context in which operators can initialize by registering to managed state (i.e. state that
 * is managed by state backends).
 * 这个接口提供了一个上下文，操作符可以通过注册到托管状态(即由状态后端管理的状态)来初始化。
 *
 * <p>
 * Operator state is available to all operators, while keyed state is only available for operators after keyBy.
 * 操作符状态对所有操作符都可用，而键控状态仅对keyBy之后的操作符可用
 *
 * <p>
 * For the purpose of initialization, the context signals if the state is empty (new operator) or was restored from
 * a previous execution of this operator.
 * 为了进行初始化，如果状态为空(新操作符)或从该操作符以前的执行中恢复，上下文将发出信号。
 *
 */
public interface ManagedInitializationContext {

	/**
	 * Returns true, if state was restored from the snapshot of a previous execution. This returns always false for
	 * stateless tasks.
	 * 如果状态从前一个执行的快照中加载了，则返回true
	 * 对于无状态的任务，该方法总是返回false
	 */
	boolean isRestored();

	/**
	 * Returns an interface that allows for registering operator state with the backend.
	 */
	OperatorStateStore getOperatorStateStore();

	/**
	 * Returns an interface that allows for registering keyed state with the backend.
	 */
	KeyedStateStore getKeyedStateStore();

}
