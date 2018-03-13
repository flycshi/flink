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

package org.apache.flink.api.common.restartstrategy;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.time.Time;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

/**
 * This class defines methods to generate RestartStrategyConfigurations. These configurations are
 * used to create RestartStrategies at runtime.
 * 定义了方法来产生 RestartStrategyConfigurations
 * 这些配置被用来在运行时创建 RestartStrategies
 *
 * The RestartStrategyConfigurations are used to decouple the core module from the runtime module.
 * RestartStrategyConfigurations 是用来将 核心模块与运行时模块 解耦的
 */
@PublicEvolving
public class RestartStrategies {

	/**
	 * Generates NoRestartStrategyConfiguration
	 *
	 * @return NoRestartStrategyConfiguration
	 */
	public static RestartStrategyConfiguration noRestart() {
		return new NoRestartStrategyConfiguration();
	}

	public static RestartStrategyConfiguration fallBackRestart() {
		return new FallbackRestartStrategyConfiguration();
	}

	/**
	 * Generates a FixedDelayRestartStrategyConfiguration.
	 *
	 * @param restartAttempts Number of restart attempts for the FixedDelayRestartStrategy
	 * @param delayBetweenAttempts Delay in-between restart attempts for the FixedDelayRestartStrategy
	 * @return FixedDelayRestartStrategy
	 */
	public static RestartStrategyConfiguration fixedDelayRestart(int restartAttempts, long delayBetweenAttempts) {
		return fixedDelayRestart(restartAttempts, Time.of(delayBetweenAttempts, TimeUnit.MILLISECONDS));
	}

	/**
	 * Generates a FixedDelayRestartStrategyConfiguration.
	 *
	 * @param restartAttempts Number of restart attempts for the FixedDelayRestartStrategy
	 * @param delayInterval Delay in-between restart attempts for the FixedDelayRestartStrategy
	 * @return FixedDelayRestartStrategy
	 */
	public static RestartStrategyConfiguration fixedDelayRestart(int restartAttempts, Time delayInterval) {
		return new FixedDelayRestartStrategyConfiguration(restartAttempts, delayInterval);
	}

	/**
	 * Generates a FailureRateRestartStrategyConfiguration.
	 *
	 * @param failureRate Maximum number of restarts in given interval {@code failureInterval} before failing a job
	 * @param failureInterval Time interval for failures
	 * @param delayInterval Delay in-between restart attempts
	 */
	public static FailureRateRestartStrategyConfiguration failureRateRestart(
			int failureRate, Time failureInterval, Time delayInterval) {
		return new FailureRateRestartStrategyConfiguration(failureRate, failureInterval, delayInterval);
	}

	/**
	 * ################################################################################################
	 * 重启策略配置的接口定义与实现
	 */

	public abstract static class RestartStrategyConfiguration implements Serializable {
		private static final long serialVersionUID = 6285853591578313960L;

		private RestartStrategyConfiguration() {}

		/**
		 * Returns a description which is shown in the web interface
		 * 返回web节点显示的重启策略配置的描述字符串
		 *
		 * @return Description of the restart strategy
		 */
		public abstract String getDescription();
	}

	final public static class NoRestartStrategyConfiguration extends RestartStrategyConfiguration {
		private static final long serialVersionUID = -5894362702943349962L;

		@Override
		public String getDescription() {
			return "Restart deactivated.";
		}
	}

	final public static class FixedDelayRestartStrategyConfiguration extends RestartStrategyConfiguration {
		private static final long serialVersionUID = 4149870149673363190L;

		private final int restartAttempts;
		private final Time delayBetweenAttemptsInterval;

		FixedDelayRestartStrategyConfiguration(int restartAttempts, Time delayBetweenAttemptsInterval) {
			this.restartAttempts = restartAttempts;
			this.delayBetweenAttemptsInterval = delayBetweenAttemptsInterval;
		}

		public int getRestartAttempts() {
			return restartAttempts;
		}

		public Time getDelayBetweenAttemptsInterval() {
			return delayBetweenAttemptsInterval;
		}

		@Override
		public int hashCode() {
			int result = restartAttempts;
			result = 31 * result + (delayBetweenAttemptsInterval != null ? delayBetweenAttemptsInterval.hashCode() : 0);
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof FixedDelayRestartStrategyConfiguration) {
				FixedDelayRestartStrategyConfiguration other = (FixedDelayRestartStrategyConfiguration) obj;

				return restartAttempts == other.restartAttempts && delayBetweenAttemptsInterval.equals(other.delayBetweenAttemptsInterval);
			} else {
				return false;
			}
		}

		@Override
		public String getDescription() {
			return "Restart with fixed delay (" + delayBetweenAttemptsInterval + "). #"
				+ restartAttempts + " restart attempts.";
		}
	}

	final public static class FailureRateRestartStrategyConfiguration extends RestartStrategyConfiguration {
		private static final long serialVersionUID = 1195028697539661739L;
		private final int maxFailureRate;

		private final Time failureInterval;
		private final Time delayBetweenAttemptsInterval;

		public FailureRateRestartStrategyConfiguration(int maxFailureRate, Time failureInterval, Time delayBetweenAttemptsInterval) {
			this.maxFailureRate = maxFailureRate;
			this.failureInterval = failureInterval;
			this.delayBetweenAttemptsInterval = delayBetweenAttemptsInterval;
		}

		public int getMaxFailureRate() {
			return maxFailureRate;
		}

		public Time getFailureInterval() {
			return failureInterval;
		}

		public Time getDelayBetweenAttemptsInterval() {
			return delayBetweenAttemptsInterval;
		}

		@Override
		public String getDescription() {
			return "Failure rate restart with maximum of " + maxFailureRate + " failures within interval " + failureInterval.toString()
					+ " and fixed delay " + delayBetweenAttemptsInterval.toString();
		}
	}

	/**
	 * Restart strategy configuration that could be used by jobs to use cluster level restart
	 * strategy. Useful especially when one has a custom implementation of restart strategy set via
	 * flink-conf.yaml.
	 * 使用集群级别的重启策略。
	 * 当通过`flink-conf.yaml`设置了一个用户实现的重启策略时会特别有用。
	 */
	final public static class FallbackRestartStrategyConfiguration extends RestartStrategyConfiguration{
		private static final long serialVersionUID = -4441787204284085544L;

		@Override
		public String getDescription() {
			return "Cluster level default restart strategy";
		}
	}
}
