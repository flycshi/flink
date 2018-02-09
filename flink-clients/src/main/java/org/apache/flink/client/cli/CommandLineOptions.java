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

package org.apache.flink.client.cli;

import org.apache.commons.cli.CommandLine;

import static org.apache.flink.client.cli.CliFrontendParser.ADDRESS_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.HELP_OPTION;

/**
 * Base class for all options parsed from the command line.
 * Contains options for printing help and the JobManager address.
 * 所有从命令行解析配置的基类
 */
public abstract class CommandLineOptions {

	private final CommandLine commandLine;

	private final String jobManagerAddress;

	private final boolean printHelp;

	protected CommandLineOptions(CommandLine line) {
		this.commandLine = line;
		this.printHelp = line.hasOption(HELP_OPTION.getOpt());
		this.jobManagerAddress = line.hasOption(ADDRESS_OPTION.getOpt()) ?
				line.getOptionValue(ADDRESS_OPTION.getOpt()) : null;
	}

	public CommandLine getCommandLine() {
		return commandLine;
	}

	public boolean isPrintHelp() {
		return printHelp;
	}

	public String getJobManagerAddress() {
		return jobManagerAddress;
	}
}
