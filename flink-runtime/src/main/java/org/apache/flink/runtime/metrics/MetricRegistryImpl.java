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

package org.apache.flink.runtime.metrics;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Kill;
import akka.pattern.Patterns;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.View;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.metrics.dump.MetricQueryService;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.apache.flink.runtime.metrics.groups.FrontMetricGroup;
import org.apache.flink.runtime.metrics.scope.ScopeFormats;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A MetricRegistry keeps track of all registered {@link Metric Metrics}. It serves as the
 * connection between {@link MetricGroup MetricGroups} and {@link MetricReporter MetricReporters}.
 * 一个{@code MetricRegistry}跟踪所有注册的{@code Metric}。
 * 它作为{@code MetricGroup}和{@code MetricReporter}之间的桥梁
 */
public class MetricRegistryImpl implements MetricRegistry {
	static final Logger LOG = LoggerFactory.getLogger(MetricRegistryImpl.class);

	private final Object lock = new Object();

	private List<MetricReporter> reporters;
	private ScheduledExecutorService executor;

	@Nullable
	private ActorRef queryService;

	@Nullable
	private String metricQueryServicePath;

	private ViewUpdater viewUpdater;

	private final ScopeFormats scopeFormats;
	private final char globalDelimiter;
	private final List<Character> delimiters = new ArrayList<>();

	/**
	 * Creates a new MetricRegistry and starts the configured reporter.
	 * 构建一个新的{@code MetricRegistry}, 并启动配置的reporter
	 */
	public MetricRegistryImpl(MetricRegistryConfiguration config) {
		this.scopeFormats = config.getScopeFormats();
		this.globalDelimiter = config.getDelimiter();

		// second, instantiate any custom configured reporters
		// 第二步, 实例化任何用户配置的reporters
		this.reporters = new ArrayList<>();

		List<Tuple2<String, Configuration>> reporterConfigurations = config.getReporterConfigurations();

		this.executor = Executors.newSingleThreadScheduledExecutor(new ExecutorThreadFactory("Flink-MetricRegistry"));

		this.queryService = null;
		this.metricQueryServicePath = null;

		if (reporterConfigurations.isEmpty()) {
			// no reporters defined
			// by default, don't report anything
			// 没有定义reporters，默认的，就不report任何数据
			LOG.info("No metrics reporter configured, no metrics will be exposed/reported.");
		} else {
			// we have some reporters so
			// 配置了一些reporters
			/** 变量配置中配置的reporter的配置 */
			for (Tuple2<String, Configuration> reporterConfiguration: reporterConfigurations) {
				String namedReporter = reporterConfiguration.f0;
				/** reporterConfig是Configuration的子类DelegatingConfiguration，会肯定定义的前缀来找key */
				Configuration reporterConfig = reporterConfiguration.f1;

				/** 获取MetricReporter的具体实现子类的全限定类型，配置的key如：metrics.reporter.foo.class */
				final String className = reporterConfig.getString(ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, null);
				if (className == null) {
					LOG.error("No reporter class set for reporter " + namedReporter + ". Metrics might not be exposed/reported.");
					continue;
				}

				try {
					/** 获取配置的定期执行的时间间隔，key的格式如：metrics.reporter.foo.interval */
					String configuredPeriod = reporterConfig.getString(ConfigConstants.METRICS_REPORTER_INTERVAL_SUFFIX, null);
					TimeUnit timeunit = TimeUnit.SECONDS;
					long period = 10;

					if (configuredPeriod != null) {
						try {
							String[] interval = configuredPeriod.split(" ");
							period = Long.parseLong(interval[0]);
							timeunit = TimeUnit.valueOf(interval[1]);
						}
						catch (Exception e) {
							LOG.error("Cannot parse report interval from config: " + configuredPeriod +
									" - please use values like '10 SECONDS' or '500 MILLISECONDS'. " +
									"Using default reporting interval.");
						}
					}

					Class<?> reporterClass = Class.forName(className);
					MetricReporter reporterInstance = (MetricReporter) reporterClass.newInstance();

					/** 构造MetricConfig的实例，并把reporterConfig中的配置key-value都添加到metricConfig中 */
					MetricConfig metricConfig = new MetricConfig();
					reporterConfig.addAllToProperties(metricConfig);
					LOG.info("Configuring {} with {}.", reporterClass.getSimpleName(), metricConfig);
					/** 这里就是reporter进行初始化操作的地方 */
					reporterInstance.open(metricConfig);

					/** 如果reporter实现了Scheduled接口，则通过executor进行定期调度执行，执行时间间隔就是上面获取的时间间隔 */
					if (reporterInstance instanceof Scheduled) {
						LOG.info("Periodically reporting metrics in intervals of {} {} for reporter {} of type {}.", period, timeunit.name(), namedReporter, className);

						/** 将reporter封装成一个task，并调度定期更新执行 */
						executor.scheduleWithFixedDelay(
								new MetricRegistryImpl.ReporterTask((Scheduled) reporterInstance), period, period, timeunit);
					} else {
						LOG.info("Reporting metrics for reporter {} of type {}.", namedReporter, className);
					}
					/** 将reporter添加到集合中 */
					reporters.add(reporterInstance);

					/** 获取reporter定制化的分隔符，如果没有设置，则设置为全局分割符 */
					String delimiterForReporter = reporterConfig.getString(ConfigConstants.METRICS_REPORTER_SCOPE_DELIMITER, String.valueOf(globalDelimiter));
					if (delimiterForReporter.length() != 1) {
						LOG.warn("Failed to parse delimiter '{}' for reporter '{}', using global delimiter '{}'.", delimiterForReporter, namedReporter, globalDelimiter);
						delimiterForReporter = String.valueOf(globalDelimiter);
					}
					this.delimiters.add(delimiterForReporter.charAt(0));
				}
				catch (Throwable t) {
					LOG.error("Could not instantiate metrics reporter {}. Metrics might not be exposed/reported.", namedReporter, t);
				}
			}
		}
	}

	/**
	 * Initializes the MetricQueryService.
	 *
	 * @param actorSystem ActorSystem to create the MetricQueryService on
	 * @param resourceID resource ID used to disambiguate the actor name
     */
	public void startQueryService(ActorSystem actorSystem, ResourceID resourceID) {
		synchronized (lock) {
			Preconditions.checkState(!isShutdown(), "The metric registry has already been shut down.");

			try {
				queryService = MetricQueryService.startMetricQueryService(actorSystem, resourceID);
				metricQueryServicePath = AkkaUtils.getAkkaURL(actorSystem, queryService);
			} catch (Exception e) {
				LOG.warn("Could not start MetricDumpActor. No metrics will be submitted to the WebInterface.", e);
			}
		}
	}

	/**
	 * Returns the address under which the {@link MetricQueryService} is reachable.
	 *
	 * @return address of the metric query service
	 */
	@Override
	@Nullable
	public String getMetricQueryServicePath() {
		return metricQueryServicePath;
	}

	@Override
	public char getDelimiter() {
		return this.globalDelimiter;
	}

	@Override
	public char getDelimiter(int reporterIndex) {
		try {
			return delimiters.get(reporterIndex);
		} catch (IndexOutOfBoundsException e) {
			LOG.warn("Delimiter for reporter index {} not found, returning global delimiter.", reporterIndex);
			return this.globalDelimiter;
		}
	}

	@Override
	public int getNumberReporters() {
		return reporters.size();
	}

	@VisibleForTesting
	public List<MetricReporter> getReporters() {
		return reporters;
	}

	/**
	 * Returns whether this registry has been shutdown.
	 * 返回这个registry是否已经被关闭了
	 *
	 * @return true, if this registry was shutdown, otherwise false
	 */
	public boolean isShutdown() {
		synchronized (lock) {
			return reporters == null && executor.isShutdown();
		}
	}

	/**
	 * Shuts down this registry and the associated {@link MetricReporter}.
	 */
	public void shutdown() {
		synchronized (lock) {
			Future<Boolean> stopFuture = null;
			FiniteDuration stopTimeout = null;

			if (queryService != null) {
				stopTimeout = new FiniteDuration(1L, TimeUnit.SECONDS);

				try {
					stopFuture = Patterns.gracefulStop(queryService, stopTimeout);
				} catch (IllegalStateException ignored) {
					// this can happen if the underlying actor system has been stopped before shutting
					// the metric registry down
					// TODO: Pull the MetricQueryService actor out of the MetricRegistry
					LOG.debug("The metric query service actor has already been stopped because the " +
						"underlying ActorSystem has already been shut down.");
				}
			}

			if (reporters != null) {
				for (MetricReporter reporter : reporters) {
					try {
						reporter.close();
					} catch (Throwable t) {
						LOG.warn("Metrics reporter did not shut down cleanly", t);
					}
				}
				reporters = null;
			}
			shutdownExecutor();

			if (stopFuture != null) {
				boolean stopped = false;

				try {
					stopped = Await.result(stopFuture, stopTimeout);
				} catch (Exception e) {
					LOG.warn("Query actor did not properly stop.", e);
				}

				if (!stopped) {
					// the query actor did not stop in time, let's kill him
					queryService.tell(Kill.getInstance(), ActorRef.noSender());
				}
			}
		}
	}

	private void shutdownExecutor() {
		if (executor != null) {
			executor.shutdown();

			try {
				if (!executor.awaitTermination(1L, TimeUnit.SECONDS)) {
					executor.shutdownNow();
				}
			} catch (InterruptedException e) {
				executor.shutdownNow();
			}
		}
	}

	@Override
	public ScopeFormats getScopeFormats() {
		return scopeFormats;
	}

	// ------------------------------------------------------------------------
	//  Metrics (de)registration
	//  注册和注销
	// ------------------------------------------------------------------------

	@Override
	public void register(Metric metric, String metricName, AbstractMetricGroup group) {
		synchronized (lock) {
			if (isShutdown()) {
				LOG.warn("Cannot register metric, because the MetricRegistry has already been shut down.");
			} else {
				if (reporters != null) {
					/** 通知所有的reporters，注册了一个metric，以及对应的metricName，group */
					for (int i = 0; i < reporters.size(); i++) {
						MetricReporter reporter = reporters.get(i);
						try {
							if (reporter != null) {
								/**
								 * 这里会将group，以及这个reporter在reporters这个列表中的索引，一起封装到FrontMetricGroup这个代理类中
								 * 这里封装索引的目的，是可以通过 #getDelimiter 方法，获取这个reporter配置的特制分隔符
								 */
								FrontMetricGroup front = new FrontMetricGroup<AbstractMetricGroup<?>>(i, group);
								/** 然后调用reporter的接口方法，通知reporter */
								reporter.notifyOfAddedMetric(metric, metricName, front);
							}
						} catch (Exception e) {
							LOG.warn("Error while registering metric.", e);
						}
					}
				}
				try {
					/** 如果queryService不为null，则也通知它 */
					if (queryService != null) {
						MetricQueryService.notifyOfAddedMetric(queryService, metric, metricName, group);
					}
				} catch (Exception e) {
					LOG.warn("Error while registering metric.", e);
				}
				try {
					if (metric instanceof View) {
						if (viewUpdater == null) {
							viewUpdater = new ViewUpdater(executor);
						}
						viewUpdater.notifyOfAddedView((View) metric);
					}
				} catch (Exception e) {
					LOG.warn("Error while registering metric.", e);
				}
			}
		}
	}

	@Override
	public void unregister(Metric metric, String metricName, AbstractMetricGroup group) {
		synchronized (lock) {
			if (isShutdown()) {
				LOG.warn("Cannot unregister metric, because the MetricRegistry has already been shut down.");
			} else {
				if (reporters != null) {
					for (int i = 0; i < reporters.size(); i++) {
						try {
						MetricReporter reporter = reporters.get(i);
							if (reporter != null) {
								FrontMetricGroup front = new FrontMetricGroup<AbstractMetricGroup<?>>(i, group);
								reporter.notifyOfRemovedMetric(metric, metricName, front);
							}
						} catch (Exception e) {
							LOG.warn("Error while registering metric.", e);
						}
					}
				}
				try {
					if (queryService != null) {
						MetricQueryService.notifyOfRemovedMetric(queryService, metric);
					}
				} catch (Exception e) {
					LOG.warn("Error while registering metric.", e);
				}
				try {
					if (metric instanceof View) {
						if (viewUpdater != null) {
							viewUpdater.notifyOfRemovedView((View) metric);
						}
					}
				} catch (Exception e) {
					LOG.warn("Error while registering metric.", e);
				}
			}
		}
	}

	// ------------------------------------------------------------------------

	@VisibleForTesting
	@Nullable
	public ActorRef getQueryService() {
		return queryService;
	}

	// ------------------------------------------------------------------------

	/**
	 * This task is explicitly a static class, so that it does not hold any references to the enclosing
	 * MetricsRegistry instance.
	 * 这个task是个静态类，所以它不持有任何闭包MetricsRegistry实例的引用
	 *
	 * <p>This is a subtle difference, but very important: With this static class, the enclosing class instance
	 * may become garbage-collectible, whereas with an anonymous inner class, the timer thread
	 * (which is a GC root) will hold a reference via the timer task and its enclosing instance pointer.
	 * Making the MetricsRegistry garbage collectible makes the java.util.Timer garbage collectible,
	 * which acts as a fail-safe to stop the timer thread and prevents resource leaks.
	 *
	 * 这是一个细微的差别，
	 * 但是非常重要:
	 * 在这个静态类中，封闭的类实例可能变成垃圾收集，
	 * 而对于一个匿名的内部类，定时器线程(它是一个GC根)将通过定时器任务和它的封闭实例指针来保存一个引用。
	 * 使MetricsRegistry垃圾收集可以使java.util.Timer可被垃圾回收，
	 * 它作为一个故障保险来停止计时器线程并防止资源泄漏。
	 */
	private static final class ReporterTask extends TimerTask {

		private final Scheduled reporter;

		private ReporterTask(Scheduled reporter) {
			this.reporter = reporter;
		}

		@Override
		public void run() {
			try {
				reporter.report();
			} catch (Throwable t) {
				LOG.warn("Error while reporting metrics", t);
			}
		}
	}
}
