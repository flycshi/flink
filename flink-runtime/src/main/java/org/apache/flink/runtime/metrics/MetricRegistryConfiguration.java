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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DelegatingConfiguration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.runtime.metrics.scope.ScopeFormats;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Configuration object for {@link MetricRegistryImpl}.
 * 配置对象
 */
public class MetricRegistryConfiguration {

	private static final Logger LOG = LoggerFactory.getLogger(MetricRegistryConfiguration.class);

	private static volatile MetricRegistryConfiguration defaultConfiguration;

	/**
	 * regex pattern to split the defined reporters
	 * 分割定义的报告器的正则表达式
	 */
	private static final Pattern splitPattern = Pattern.compile("\\s*,\\s*");

	/**
	 * scope formats for the different components
	 * 不同组件的范围格式
	 */
	private final ScopeFormats scopeFormats;

	/**
	 * delimiter for the scope strings
	 * 字符串的分隔符
	 */
	private final char delimiter;

	/**
	 * contains for every configured reporter its name and the configuration object
	 * 包含了配置的每个reporter的名称和配置对象
	 */
	private final List<Tuple2<String, Configuration>> reporterConfigurations;

	public MetricRegistryConfiguration(
		ScopeFormats scopeFormats,
		char delimiter,
		List<Tuple2<String, Configuration>> reporterConfigurations) {

		this.scopeFormats = Preconditions.checkNotNull(scopeFormats);
		this.delimiter = delimiter;
		this.reporterConfigurations = Preconditions.checkNotNull(reporterConfigurations);
	}

	// ------------------------------------------------------------------------
	//  Getter
	// ------------------------------------------------------------------------

	public ScopeFormats getScopeFormats() {
		return scopeFormats;
	}

	public char getDelimiter() {
		return delimiter;
	}

	public List<Tuple2<String, Configuration>> getReporterConfigurations() {
		return reporterConfigurations;
	}

	// ------------------------------------------------------------------------
	//  Static factory methods
	// ------------------------------------------------------------------------

	/**
	 * Create a metric registry configuration object from the given {@link Configuration}.
	 * 从给定的{@code Configuration}构建一个metric注册配置对象
	 *
	 * @param configuration to generate the metric registry configuration from
	 * @return Metric registry configuration generated from the configuration
	 */
	public static MetricRegistryConfiguration fromConfiguration(Configuration configuration) {
		ScopeFormats scopeFormats;
		try {
			scopeFormats = ScopeFormats.fromConfig(configuration);
		} catch (Exception e) {
			LOG.warn("Failed to parse scope format, using default scope formats", e);
			scopeFormats = ScopeFormats.fromConfig(new Configuration());
		}

		char delim;
		try {
			delim = configuration.getString(MetricOptions.SCOPE_DELIMITER).charAt(0);
		} catch (Exception e) {
			LOG.warn("Failed to parse delimiter, using default delimiter.", e);
			delim = '.';
		}

		/** 获取MetricReporter相关的配置信息，MetricReporter的配置格式是 metrics.reporters = foo, bar */
		final String definedReporters = configuration.getString(MetricOptions.REPORTERS_LIST);
		List<Tuple2<String, Configuration>> reporterConfigurations;

		if (definedReporters == null) {
			reporterConfigurations = Collections.emptyList();
		} else {
			/** 按模式匹配分割，如上述的配置，则namedReporters={"foo", "bar"} */
			String[] namedReporters = splitPattern.split(definedReporters);

			reporterConfigurations = new ArrayList<>(namedReporters.length);

			for (String namedReporter: namedReporters) {
				/**
				 * 这里是获取一个代理配置对象，就是在原来配置对象的基础上，在查询key时，需要加上这里配置的前缀，
				 * 如 metrics.reporter.foo. ，这样就可以获取特定reporter的配置
				 */
				DelegatingConfiguration delegatingConfiguration = new DelegatingConfiguration(
					configuration,
					ConfigConstants.METRICS_REPORTER_PREFIX + namedReporter + '.');

				reporterConfigurations.add(Tuple2.of(namedReporter, (Configuration) delegatingConfiguration));
			}
		}

		return new MetricRegistryConfiguration(scopeFormats, delim, reporterConfigurations);
	}

	public static MetricRegistryConfiguration defaultMetricRegistryConfiguration() {
		// create the default metric registry configuration only once
		// 创建默认的, 只一次
		if (defaultConfiguration == null) {
			synchronized (MetricRegistryConfiguration.class) {
				if (defaultConfiguration == null) {
					defaultConfiguration = fromConfiguration(new Configuration());
				}
			}
		}

		return defaultConfiguration;
	}

}
