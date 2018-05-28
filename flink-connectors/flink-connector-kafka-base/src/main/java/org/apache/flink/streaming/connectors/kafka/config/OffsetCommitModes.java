/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.flink.streaming.connectors.kafka.config;

/**
 * Utilities for {@link OffsetCommitMode}.
 */
public class OffsetCommitModes {

	/**
	 * Determine the offset commit mode using several configuration values.
	 * 通过几个配置项，综合决定offset commit模式
	 *
	 * @param enableAutoCommit whether or not auto committing is enabled in the provided Kafka properties.
	 * @param enableCommitOnCheckpoint whether or not committing on checkpoints is enabled.
	 * @param enableCheckpointing whether or not checkpoint is enabled for the consumer.
	 *
	 * @return the offset commmit mode to use, based on the configuration values.
	 */
	public static OffsetCommitMode fromConfiguration(
			boolean enableAutoCommit,
			boolean enableCommitOnCheckpoint,
			boolean enableCheckpointing) {

		if (enableCheckpointing) {
			// if checkpointing is enabled, the mode depends only on whether committing on checkpoints is enabled
			// 如果checkpoint开启了，模式只依赖，在产生checkpoint时提交offset是否开启
			return (enableCommitOnCheckpoint) ? OffsetCommitMode.ON_CHECKPOINTS : OffsetCommitMode.DISABLED;
		} else {
			// else, the mode depends only on whether auto committing is enabled in the provided Kafka properties
			// 如果checkpoint没有开启，模式只依赖，kafka的生产者属性中是否开启了自动提交
			return (enableAutoCommit) ? OffsetCommitMode.KAFKA_PERIODIC : OffsetCommitMode.DISABLED;
		}
	}
}
