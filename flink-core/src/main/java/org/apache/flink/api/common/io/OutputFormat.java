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

package org.apache.flink.api.common.io;

import org.apache.flink.annotation.Public;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.io.Serializable;

/**
 * The base interface for outputs that consumes records. The output format
 * describes how to store the final records, for example in a file.
 * 消费记录的输出的基本接口。
 * 输出格式描述了如何存储最终记录,比如存储到一个文件中。
 * <p>
 * The life cycle of an output format is the following:
 * 一个{@code OutputFormat}的生命周期如下:
 * <ol>
 *   <li>configure() is invoked a single time. The method can be used to implement initialization from
 *       the parameters (configuration) that may be attached upon instantiation.</li>
 *       {@link #configure(Configuration)}只能调用一次。这个方法可以被用来实现基于参数的初始化,这些参数可能是在安装时就有的。
 *   <li>Each parallel output task creates an instance, configures it and opens it.</li>
 *   	 每个并行输出任务创建一个实例,配置并打开它。
 *   <li>All records of its parallel instance are handed to the output format.</li>
 *   	 它的并行实例的所有记录都按{@code OutputFormat}处理
 *   <li>The output format is closed</li>
 *   	 {@code OutputFormat}被关闭
 * </ol>
 * 
 * @param <IT> The type of the consumed records. 消费的记录的类型
 */
@Public
public interface OutputFormat<IT> extends Serializable {
	
	/**
	 * Configures this output format. Since output formats are instantiated generically and hence parameterless, 
	 * this method is the place where the output formats set their basic fields based on configuration values.
	 * 配置{@code OutputFormat}。
	 * 因为{@code OutputFormat}实例化时一般是无参的,所有这个方法就是{@code OutputFormat}基于配置值进行基础字段设置的地方。
	 * <p>
	 * This method is always called first on a newly instantiated output format.
	 * 这个方法总是新实例化一个{@code OutputFormat}时首先被调用。
	 *  
	 * @param parameters The configuration with all parameters.
	 */
	void configure(Configuration parameters);
	
	/**
	 * Opens a parallel instance of the output format to store the result of its parallel instance.
	 * 打开{@code OutputFormat}的一个并行实例,用来存储它的并行实例的结果
	 * <p>
	 * When this method is called, the output format it guaranteed to be configured.
	 * 当这个方法被调用时, {@code OutputFormat}已经被配置过
	 * 
	 * @param taskNumber The number of the parallel instance.	并行实例的编号
	 * @param numTasks The number of parallel tasks.	并行任务的数量
	 * @throws IOException Thrown, if the output could not be opened due to an I/O problem.
	 */
	void open(int taskNumber, int numTasks) throws IOException;
	
	
	/**
	 * Adds a record to the output.
	 * 想输出中添加一个记录
	 * <p>
	 * When this method is called, the output format it guaranteed to be opened.
	 * 当这个方法被调用时, {@code OutputFormat}保证已经被打开
	 * 
	 * @param record The records to add to the output.
	 * @throws IOException Thrown, if the records could not be added to to an I/O problem.
	 */
	void writeRecord(IT record) throws IOException;
	
	/**
	 * Method that marks the end of the life-cycle of parallel output instance. Should be used to close
	 * channels and streams and release resources.
	 * After this method returns without an error, the output is assumed to be correct.
	 * 该方法标记达到了并行输出实例的生命周期的终点。
	 * 应该被用来关闭channel、stream、以及释放资源。
	 * 在该方法成功执行后, 输出可以认为是正确的。
	 * <p>
	 * When this method is called, the output format it guaranteed to be opened.
	 * 当这个方法被调用时, {@code OutputFormat}保障被打开
	 *  
	 * @throws IOException Thrown, if the input could not be closed properly.
	 */
	void close() throws IOException;
}

