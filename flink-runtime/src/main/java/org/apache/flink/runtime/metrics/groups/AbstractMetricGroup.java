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
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Abstract {@link MetricGroup} that contains key functionality for adding metrics and groups.
 * 包含了添加metrics和groups的关键功能的抽象{@code MetricGroup}
 *
 * <p><b>IMPORTANT IMPLEMENTATION NOTE</b>
 * 重要的实现注意
 *
 * <p>This class uses locks for adding and removing metrics objects. This is done to
 * prevent resource leaks in the presence of concurrently closing a group and adding
 * metrics and subgroups.
 * 这个类使用锁来进行添加和移除度量对象。
 * 这样做是为了防止在并发关闭group和添加metrics和子group时的资源泄露。
 *
 * Since closing groups recursively closes the subgroups, the lock acquisition order must
 * be strictly from parent group to subgroup. If at any point, a subgroup holds its group
 * lock and calls a parent method that also acquires the lock, it will create a deadlock
 * condition.
 * 由于关闭组会递归地关闭子组, 因此锁捕获顺序必须严格地从父组到子组。
 * 如果在任何点, 一个子组持有它的组锁, 同时调用了一个父类方法, 也需要请求这个锁, 将导致死锁。
 *
 * <p>An AbstractMetricGroup can be {@link #close() closed}. Upon closing, the group de-register all metrics
 * from any metrics reporter and any internal maps. Note that even closed metrics groups
 * return Counters, Gauges, etc to the code, to prevent exceptions in the monitored code.
 * These metrics simply do not get reported any more, when created on a closed group.
 * 一个{@code AbstractMetricGroup}可以被关闭。
 * 在结束时, 组会注销来自所有指标报告器和任何内部映射的所有指标。
 * 请注意，即使是关闭的{@code MetricGroup}也会返回计数器、测量器等, 以防止被监控代码中的异常。
 * 当在一个被关闭的组上创建时, 这些度量就不再被报告了。
 *
 * @param <A> The type of the parent MetricGroup	父{@code MetricGroup}的类型
 */
@Internal
public abstract class AbstractMetricGroup<A extends AbstractMetricGroup<?>> implements MetricGroup {

	protected static final Logger LOG = LoggerFactory.getLogger(MetricGroup.class);

	// ------------------------------------------------------------------------

	/**
	 * The parent group containing this group.
	 * 包含这个group的父group
	 */
	protected final A parent;

	/**
	 * The map containing all variables and their associated values, lazily computed.
	 * 包含了所有变量和他们的关联值的map, 延迟计算
	 */
	protected volatile Map<String, String> variables;

	/**
	 * The registry that this metrics group belongs to.
	 * 这个{@code MetricGroup}归属的注册器
	 */
	protected final MetricRegistry registry;

	/**
	 * All metrics that are directly contained in this group.
	 * 直接包含在该group中的所有的metrics
	 */
	private final Map<String, Metric> metrics = new HashMap<>();

	/**
	 * All metric subgroups of this group.
	 * 这个group中的所有子MetricGroup
	 */
	private final Map<String, AbstractMetricGroup> groups = new HashMap<>();

	/**
	 * The metrics scope represented by this group.
	 * For example ["host-7", "taskmanager-2", "window_word_count", "my-mapper" ].
	 * 表示这个组的度量范围
	 * 比如 ["host-7", "taskmanager-2", "window_word_count", "my-mapper" ]
	 */
	private final String[] scopeComponents;

	/**
	 * Array containing the metrics scope represented by this group for each reporter, as a concatenated string, lazily computed.
	 * For example: "host-7.taskmanager-2.window_word_count.my-mapper"
	 * 包含由这个组代表的每个报告器的度量范围的数组，作为连接字符串，延迟计算。
	 */
	private final String[] scopeStrings;

	/**
	 * The logical metrics scope represented by this group, as a concatenated string, lazily computed.
	 * For example: "taskmanager.job.task"
	 * 这个group表示的逻辑度量范围, 作为一个连接字符, 延迟计算
	 * 比如: "taskmanager.job.task"
	 */
	private String logicalScopeString;

	/**
	 * The metrics query service scope represented by this group, lazily computed.
	 */
	protected QueryScopeInfo queryServiceScopeInfo;

	/**
	 * Flag indicating whether this group has been closed.
	 * 标识这个group是否背关闭的flag
	 */
	private volatile boolean closed;

	// ------------------------------------------------------------------------

	public AbstractMetricGroup(MetricRegistry registry, String[] scope, A parent) {
		this.registry = checkNotNull(registry);
		this.scopeComponents = checkNotNull(scope);
		this.parent = parent;
		this.scopeStrings = new String[registry.getNumberReporters()];
	}

	@Override
	public Map<String, String> getAllVariables() {
		if (variables == null) { // avoid synchronization for common case
			synchronized (this) {
				if (variables == null) {
					if (parent != null) {
						variables = parent.getAllVariables();
					} else { // this case should only be true for mock groups
						variables = new HashMap<>();
					}
				}
			}
		}
		return variables;
	}

	/**
	 * Returns the logical scope of this group, for example
	 * {@code "taskmanager.job.task"}.
	 *
	 * @param filter character filter which is applied to the scope components
	 * @return logical scope
	 */
	public String getLogicalScope(CharacterFilter filter) {
		return getLogicalScope(filter, registry.getDelimiter());
	}

	/**
	 * Returns the logical scope of this group, for example
	 * {@code "taskmanager.job.task"}.
	 * 返回这个group的逻辑范围, 比如 {@code "taskmanager.job.task"}
	 *
	 * @param filter character filter which is applied to the scope components
	 * @return logical scope
	 */
	public String getLogicalScope(CharacterFilter filter, char delimiter) {
		if (logicalScopeString == null) {
			if (parent == null) {
				logicalScopeString = getGroupName(filter);
			} else {
				logicalScopeString = parent.getLogicalScope(filter, delimiter) + delimiter + getGroupName(filter);
			}
		}
		return logicalScopeString;
	}

	/**
	 * Returns the name for this group, meaning what kind of entity it represents, for example "taskmanager".
	 * 返回这个group的名称, 也就是它表示的什么类型的实体, 比如 "taskmanager"
	 *
	 * @param filter character filter which is applied to the name	应用到名称上的字符过滤器
	 * @return logical name for this group	这个group的逻辑名称
	 */
	protected abstract String getGroupName(CharacterFilter filter);

	/**
	 * Gets the scope as an array of the scope components, for example
	 * {@code ["host-7", "taskmanager-2", "window_word_count", "my-mapper"]}.
	 *
	 * @see #getMetricIdentifier(String)
	 */
	@Override
	public String[] getScopeComponents() {
		return scopeComponents;
	}

	/**
	 * Returns the metric query service scope for this group.
	 *
	 * @param filter character filter
	 * @return query service scope
     */
	public QueryScopeInfo getQueryServiceMetricInfo(CharacterFilter filter) {
		if (queryServiceScopeInfo == null) {
			queryServiceScopeInfo = createQueryServiceMetricInfo(filter);
		}
		return queryServiceScopeInfo;
	}

	/**
	 * Creates the metric query service scope for this group.
	 * 为这个group构建 metric query service scope
	 *
	 * @param filter character filter
	 * @return query service scope
     */
	protected abstract QueryScopeInfo createQueryServiceMetricInfo(CharacterFilter filter);

	/**
	 * Returns the fully qualified metric name, for example
	 * {@code "host-7.taskmanager-2.window_word_count.my-mapper.metricName"}.
	 *
	 * @param metricName metric name
	 * @return fully qualified metric name
	 */
	@Override
	public String getMetricIdentifier(String metricName) {
		return getMetricIdentifier(metricName, null);
	}

	/**
	 * Returns the fully qualified metric name, for example
	 * {@code "host-7.taskmanager-2.window_word_count.my-mapper.metricName"}.
	 *
	 * @param metricName metric name
	 * @param filter character filter which is applied to the scope components if not null.
	 * @return fully qualified metric name
	 */
	@Override
	public String getMetricIdentifier(String metricName, CharacterFilter filter) {
		return getMetricIdentifier(metricName, filter, -1);
	}

	/**
	 * Returns the fully qualified metric name using the configured delimiter for the reporter with the given index, for example
	 * {@code "host-7.taskmanager-2.window_word_count.my-mapper.metricName"}.
	 * 为给定索引的报告者, 使用配置的分隔符, 构造全限定metric名称, 并返回。
	 * 比如, {@code "host-7.taskmanager-2.window_word_count.my-mapper.metricName"}。
	 *
	 * @param metricName metric name
	 * @param filter character filter which is applied to the scope components if not null.
	 * @param reporterIndex index of the reporter whose delimiter should be used
	 * @return fully qualified metric name
	 */
	public String getMetricIdentifier(String metricName, CharacterFilter filter, int reporterIndex) {
		if (scopeStrings.length == 0 || (reporterIndex < 0 || reporterIndex >= scopeStrings.length)) {
			char delimiter = registry.getDelimiter();
			String newScopeString;
			if (filter != null) {
				newScopeString = ScopeFormat.concat(filter, delimiter, scopeComponents);
				metricName = filter.filterCharacters(metricName);
			} else {
				newScopeString = ScopeFormat.concat(delimiter, scopeComponents);
			}
			return newScopeString + delimiter + metricName;
		} else {
			char delimiter = registry.getDelimiter(reporterIndex);
			if (scopeStrings[reporterIndex] == null) {
				if (filter != null) {
					scopeStrings[reporterIndex] = ScopeFormat.concat(filter, delimiter, scopeComponents);
				} else {
					scopeStrings[reporterIndex] = ScopeFormat.concat(delimiter, scopeComponents);
				}
			}
			if (filter != null) {
				metricName = filter.filterCharacters(metricName);
			}
			return scopeStrings[reporterIndex] + delimiter + metricName;
		}
	}

	// ------------------------------------------------------------------------
	//  Closing
	//  关闭
	// ------------------------------------------------------------------------

	public void close() {
		synchronized (this) {
			if (!closed) {
				closed = true;

				// close all subgroups
				// 关闭所有的子groups
				for (AbstractMetricGroup group : groups.values()) {
					group.close();
				}
				groups.clear();

				// un-register all directly contained metrics
				// 注销所有直接包含的metrics
				for (Map.Entry<String, Metric> metric : metrics.entrySet()) {
					registry.unregister(metric.getValue(), metric.getKey(), this);
				}
				metrics.clear();
			}
		}
	}

	public final boolean isClosed() {
		return closed;
	}

	// -----------------------------------------------------------------------------------------------------------------
	//  Metrics
	//  度量
	// -----------------------------------------------------------------------------------------------------------------

	@Override
	public Counter counter(int name) {
		return counter(String.valueOf(name));
	}

	@Override
	public Counter counter(String name) {
		return counter(name, new SimpleCounter());
	}

	@Override
	public <C extends Counter> C counter(int name, C counter) {
		return counter(String.valueOf(name), counter);
	}

	@Override
	public <C extends Counter> C counter(String name, C counter) {
		addMetric(name, counter);
		return counter;
	}

	@Override
	public <T, G extends Gauge<T>> G gauge(int name, G gauge) {
		return gauge(String.valueOf(name), gauge);
	}

	@Override
	public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
		addMetric(name, gauge);
		return gauge;
	}

	@Override
	public <H extends Histogram> H histogram(int name, H histogram) {
		return histogram(String.valueOf(name), histogram);
	}

	@Override
	public <H extends Histogram> H histogram(String name, H histogram) {
		addMetric(name, histogram);
		return histogram;
	}

	@Override
	public <M extends Meter> M meter(int name, M meter) {
		return meter(String.valueOf(name), meter);
	}

	@Override
	public <M extends Meter> M meter(String name, M meter) {
		addMetric(name, meter);
		return meter;
	}

	/**
	 * Adds the given metric to the group and registers it at the registry, if the group
	 * is not yet closed, and if no metric with the same name has been registered before.
	 * 如果group还没有被关闭, 并且没有相同名称的metric被注册过, 则将给定的metric添加到group, 并注册到registry。
	 *
	 * @param name the name to register the metric under
	 * @param metric the metric to register
	 */
	protected void addMetric(String name, Metric metric) {
		if (metric == null) {
			LOG.warn("Ignoring attempted registration of a metric due to being null for name {}.", name);
			return;
		}
		// add the metric only if the group is still open
		// 只有group仍然打开的情况下, 才添加这个metric
		synchronized (this) {
			if (!closed) {
				// immediately put without a 'contains' check to optimize the common case (no collision)
				// collisions are resolved later
				/**
				 * 在没有进行"contains"校验下, 立即进行put操作, 来优化常见的情况(没有碰撞)
				 * 碰撞的情况后面会处理。
				 */
				Metric prior = metrics.put(name, metric);

				// check for collisions with other metric names
				// 检查与其他度量名称的冲突
				if (prior == null) {
					// no other metric with this name yet
					// 这个名字还没有其他指标

					if (groups.containsKey(name)) {
						// we warn here, rather than failing, because metrics are tools that should not fail the
						// program when used incorrectly
						// 这里给出warn日志, 而不是fail, 因为metrics是工具, 当使用错误时, 不应该使得程序失败
						LOG.warn("Name collision: Adding a metric with the same name as a metric subgroup: '" +
								name + "'. Metric might not get properly reported. " + Arrays.toString(scopeComponents));
					}

					registry.register(metric, name, this);
				}
				else {
					// we had a collision. put back the original value
					// 有碰撞, 放回原来的metric
					metrics.put(name, prior);

					// we warn here, rather than failing, because metrics are tools that should not fail the
					// program when used incorrectly
					LOG.warn("Name collision: Group already contains a Metric with the name '" +
							name + "'. Metric will not be reported." + Arrays.toString(scopeComponents));
				}
			}
		}
	}

	// ------------------------------------------------------------------------
	//  Groups
	//  组
	// ------------------------------------------------------------------------

	@Override
	public MetricGroup addGroup(int name) {
		return addGroup(String.valueOf(name));
	}

	@Override
	public MetricGroup addGroup(String name) {
		synchronized (this) {
			if (!closed) {
				// adding a group with the same name as a metric creates problems in many reporters/dashboards
				// we warn here, rather than failing, because metrics are tools that should not fail the
				// program when used incorrectly
				/**
				 * 添加与一个metric同名的group会导致在很多reporters/dashboards出问题。
				 */
				if (metrics.containsKey(name)) {
					LOG.warn("Name collision: Adding a metric subgroup with the same name as an existing metric: '" +
							name + "'. Metric might not get properly reported. " + Arrays.toString(scopeComponents));
				}

				AbstractMetricGroup newGroup = new GenericMetricGroup(registry, this, name);
				AbstractMetricGroup prior = groups.put(name, newGroup);
				if (prior == null) {
					// no prior group with that name
					// 没有同名注册过的group
					return newGroup;
				} else {
					// had a prior group with that name, add the prior group back
					// 有个同名注册过的group, 将prior设置回去
					groups.put(name, prior);
					return prior;
				}
			}
			else {
				// return a non-registered group that is immediately closed already
				// 返回一个注销的组, 它被立即close掉了
				GenericMetricGroup closedGroup = new GenericMetricGroup(registry, this, name);
				closedGroup.close();
				return closedGroup;
			}
		}
	}
}
