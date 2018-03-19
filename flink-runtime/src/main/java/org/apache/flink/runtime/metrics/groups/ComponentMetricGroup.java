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

package org.apache.flink.runtime.metrics.groups;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.metrics.MetricRegistry;

import java.util.HashMap;
import java.util.Map;

/**
 * Abstract {@link org.apache.flink.metrics.MetricGroup} for system components (e.g.,
 * TaskManager, Job, Task, Operator).
 * 系统组件(比如TaskManager, Job, Task, Operator)的抽象{@code MetricGroup}
 *
 * <p>Usually, the scope of metrics is simply the hierarchy of the containing groups. For example
 * the Metric {@code "MyMetric"} in group {@code "B"} nested in group {@code "A"} would have a
 * fully scoped name of {@code "A.B.MyMetric"}, with {@code "A.B"} being the Metric's scope.
 * 通常，metrics的范围只是包含的组的层次结构。
 * 比如：{@code "MyMetric"}这个度量，在组{@code "B"}中，组{@code "B"}是组{@code "A"}的子组，
 * 那全限定范围名称就是{@code "A.B.MyMetric"}，{@code "A.B"}就是度量的范围。
 *
 * <p>Component groups, however, have configurable scopes. This allow users to include or exclude
 * certain identifiers from the scope. The scope for metrics belonging to the "Task"
 * group could for example include the task attempt number (more fine grained identification), or
 * exclude it (for continuity of the namespace across failure and recovery).
 * 组件groups，具有可配置的范围。
 * 这个允许用户从scope中包含或排除特定标识。
 * 属于"task"组的metrics的范围，比如可以包含task尝试编号(更细粒度的标识)，或者排除它(为了在故障和恢复之间保持名称空间的连续性)
 *
 * @param <P> The type of the parent MetricGroup.
 */
@Internal
public abstract class ComponentMetricGroup<P extends AbstractMetricGroup<?>> extends AbstractMetricGroup<P> {

	/**
	 * Creates a new ComponentMetricGroup.
	 *
	 * @param registry     registry to register new metrics with
	 * @param scope        the scope of the group
	 */
	public ComponentMetricGroup(MetricRegistry registry, String[] scope, P parent) {
		super(registry, scope, parent);
	}

	@Override
	public Map<String, String> getAllVariables() {
		if (variables == null) { // avoid synchronization for common case
			synchronized (this) {
				if (variables == null) {
					variables = new HashMap<>();
					putVariables(variables);
					if (parent != null) { // not true for Job-/TaskManagerMetricGroup
						variables.putAll(parent.getAllVariables());
					}
				}
			}
		}
		return variables;
	}

	/**
	 * Enters all variables specific to this ComponentMetricGroup and their associated values into the map.
	 * 将特定于这个ComponentMetricGroup的所有变量及其关联的值输入到映射中。
	 *
	 * @param variables map to enter variables and their values into
     */
	protected abstract void putVariables(Map<String, String> variables);

	/**
	 * Closes the component group by removing and closing all metrics and subgroups
	 * (inherited from {@link AbstractMetricGroup}), plus closing and removing all dedicated
	 * component subgroups.
	 */
	@Override
	public void close() {
		synchronized (this) {
			if (!isClosed()) {
				// remove all metrics and generic subgroups
				super.close();

				// remove and close all subcomponent metrics
				for (ComponentMetricGroup group : subComponents()) {
					group.close();
				}
			}
		}
	}

	// ------------------------------------------------------------------------
	//  Component Metric Group Specifics
	// ------------------------------------------------------------------------

	/**
	 * Gets all component metric groups that are contained in this component metric group.
	 *
	 * @return All component metric groups that are contained in this component metric group.
	 */
	protected abstract Iterable<? extends ComponentMetricGroup> subComponents();
}
