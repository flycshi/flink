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

package org.apache.flink.api.java.functions;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.functions.Function;

import java.io.Serializable;

/**
 * The {@link KeySelector} allows to use deterministic objects for operations such as
 * reduce, reduceGroup, join, coGoup, etc. If invoked multiple times on the same object,
 * the returned key must be the same.
 * KeySelector 允许使用确定性对象进行操作,比如reduce、reduceGroup、join、coGroup等。
 * 在相同的对象上调用多次,返回结果必须是一样的。
 * 
 * The extractor takes an object and returns the deterministic key for that object.
 * 提取器获取一个对象,并返回一个确定的key
 *
 * @param <IN> Type of objects to extract the key from.
 * @param <KEY> Type of key.
 */
@Public
public interface KeySelector<IN, KEY> extends Function, Serializable {

	/**
	 * User-defined function that deterministically extracts the key from an object.
	 * 用户定义的函数,用来从对象中确定性的提取一个key
	 * 
	 * For example for a class:
	 * 例子:
	 * <pre>
	 * 	public class Word {
	 * 		String word;
	 * 		int count;
	 * 	}
	 * </pre>
	 * The key extractor could return the word as
	 * a key to group all Word objects by the String they contain.
	 * key提取器可以返回word字段作为key,用来聚合包含相同字符串的所有对象。
	 * 
	 * The code would look like this
	 * <pre>
	 * 	public String getKey(Word w) {
	 * 		return w.word;
	 * 	}
	 * </pre>
	 * 
	 * @param value The object to get the key from.
	 * @return The extracted key.
	 * 
	 * @throws Exception Throwing an exception will cause the execution of the respective task to fail,
	 *                   and trigger recovery or cancellation of the program. 
	 */
	KEY getKey(IN value) throws Exception;
}
