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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

import org.apache.commons.cli.CommandLine;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.client.cli.CliFrontendParser.ARGS_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.CLASSPATH_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.CLASS_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.DETACHED_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.JAR_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.LOGGING_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PARALLELISM_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.SAVEPOINT_ALLOW_NON_RESTORED_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.SAVEPOINT_PATH_OPTION;

/**
 * Base class for command line options that refer to a JAR file program.
 * 引用jar文件程序的命令行选项的基类
 */
public abstract class ProgramOptions extends CommandLineOptions {

	/** jar路径 */
	private final String jarFilePath;

	/** 启动类 */
	private final String entryPointClass;

	private final List<URL> classpaths;

	private final String[] programArgs;

	private final int parallelism;

	private final boolean stdoutLogging;

	private final boolean detachedMode;

	private final SavepointRestoreSettings savepointSettings;

	protected ProgramOptions(CommandLine line) throws CliArgsException {
		super(line);

		String[] args = line.hasOption(ARGS_OPTION.getOpt()) ?
				line.getOptionValues(ARGS_OPTION.getOpt()) :
				line.getArgs();

		/**
		 * 用户的jar包路径
		 * 1、可以通过 -j、--jarfile 直接指定
		 * 2、通过 -a、--arguments 指定的多个value中的第一个value
		 * 3、上述都没有指定,则可以通过不带任何参数名的,直接给定jar包路径,但是必须是在第一个
		 */
		if (line.hasOption(JAR_OPTION.getOpt())) {
			this.jarFilePath = line.getOptionValue(JAR_OPTION.getOpt());
		}
		else if (args.length > 0) {
			jarFilePath = args[0];
			args = Arrays.copyOfRange(args, 1, args.length);
		}
		else {
			jarFilePath = null;
		}

		this.programArgs = args;

		List<URL> classpaths = new ArrayList<URL>();
		if (line.hasOption(CLASSPATH_OPTION.getOpt())) {
			for (String path : line.getOptionValues(CLASSPATH_OPTION.getOpt())) {
				try {
					classpaths.add(new URL(path));
				} catch (MalformedURLException e) {
					throw new CliArgsException("Bad syntax for classpath: " + path);
				}
			}
		}
		this.classpaths = classpaths;

		/**
		 * 通过 -c、--class 指定启动类
		 */
		this.entryPointClass = line.hasOption(CLASS_OPTION.getOpt()) ?
				line.getOptionValue(CLASS_OPTION.getOpt()) : null;

		/**
		 * 通过 -p、--parallelism 指定并行度
		 * 如果没有指定,则采用默认的
		 */
		if (line.hasOption(PARALLELISM_OPTION.getOpt())) {
			String parString = line.getOptionValue(PARALLELISM_OPTION.getOpt());
			try {
				parallelism = Integer.parseInt(parString);
				if (parallelism <= 0) {
					throw new NumberFormatException();
				}
			}
			catch (NumberFormatException e) {
				throw new CliArgsException("The parallelism must be a positive number: " + parString);
			}
		}
		else {
			parallelism = ExecutionConfig.PARALLELISM_DEFAULT;
		}

		stdoutLogging = !line.hasOption(LOGGING_OPTION.getOpt());
		detachedMode = line.hasOption(DETACHED_OPTION.getOpt());

		if (line.hasOption(SAVEPOINT_PATH_OPTION.getOpt())) {
			String savepointPath = line.getOptionValue(SAVEPOINT_PATH_OPTION.getOpt());
			boolean allowNonRestoredState = line.hasOption(SAVEPOINT_ALLOW_NON_RESTORED_OPTION.getOpt());
			this.savepointSettings = SavepointRestoreSettings.forPath(savepointPath, allowNonRestoredState);
		} else {
			this.savepointSettings = SavepointRestoreSettings.none();
		}
	}

	public String getJarFilePath() {
		return jarFilePath;
	}

	public String getEntryPointClassName() {
		return entryPointClass;
	}

	public List<URL> getClasspaths() {
		return classpaths;
	}

	public String[] getProgramArgs() {
		return programArgs;
	}

	public int getParallelism() {
		return parallelism;
	}

	public boolean getStdoutLogging() {
		return stdoutLogging;
	}

	public boolean getDetachedMode() {
		return detachedMode;
	}

	public SavepointRestoreSettings getSavepointRestoreSettings() {
		return savepointSettings;
	}
}
