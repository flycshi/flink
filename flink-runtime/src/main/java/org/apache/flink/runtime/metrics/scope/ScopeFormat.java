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

package org.apache.flink.runtime.metrics.scope;

import org.apache.flink.metrics.CharacterFilter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class represents the format after which the "scope" (or namespace) of the various
 * component metric groups is built. Component metric groups are for example
 * "TaskManager", "Task", or "Operator".
 * 这个类表示各组件的{@code MetricGroup}的范围(或命名空间)被构建后的格式。
 * 组件的{@code MetricGroup}有"TaskManager", "Task", or "Operator"
 *
 * <p>User defined scope formats allow users to include or exclude
 * certain identifiers from the scope. The scope for metrics belonging to the "Task"
 * group could for example include the task attempt number (more fine grained identification), or
 * exclude it (continuity of the namespace across failure and recovery).
 * 用户定义的范围格式允许用户在范围内包含或排除某些标识符。
 * 属于“任务”组的度量的范围可以包括任务尝试号(更细粒度的标识)，或者排除它(在失败和恢复中命名空间的连续性)
 */
public abstract class ScopeFormat {

	private static CharacterFilter defaultFilter = new CharacterFilter() {
		@Override
		public String filterCharacters(String input) {
			return input;
		}
	};

	// ------------------------------------------------------------------------
	//  Scope Format Special Characters
	//  范围格式特殊字符
	// ------------------------------------------------------------------------

	/**
	 * If the scope format starts with this character, then the parent components scope
	 * format will be used as a prefix.
	 * 如果范围格式以这个字符开始, 那么父组件的范围格式将被它用作前缀
	 *
	 * <p>For example, if the TaskManager's job format is {@code "*.<job_name>"}, and the
	 * TaskManager format is {@code "<host>"}, then the job's metrics
	 * will have {@code "<host>.<job_name>"} as their scope.
	 * 比如, 如果{@code TaskManager}的job格式是{@code "*.<job_name>"}, 并且{@code TaskManager}格式是{@code "<host>"},
	 * 那么job的度量范围将是{@code "<host>.<job_name>"}
	 */
	public static final String SCOPE_INHERIT_PARENT = "*";

	public static final String SCOPE_SEPARATOR = ".";

	private static final String SCOPE_VARIABLE_PREFIX = "<";
	private static final String SCOPE_VARIABLE_SUFFIX = ">";

	// ------------------------------------------------------------------------
	//  Scope Variables
	//  范围变量
	// ------------------------------------------------------------------------

	public static final String SCOPE_HOST = asVariable("host");

	// ----- Task Manager ----

	public static final String SCOPE_TASKMANAGER_ID = asVariable("tm_id");

	// ----- Job -----

	public static final String SCOPE_JOB_ID = asVariable("job_id");
	public static final String SCOPE_JOB_NAME = asVariable("job_name");

	// ----- Task ----

	public static final String SCOPE_TASK_VERTEX_ID = asVariable("task_id");
	public static final String SCOPE_TASK_NAME = asVariable("task_name");
	public static final String SCOPE_TASK_ATTEMPT_ID = asVariable("task_attempt_id");
	public static final String SCOPE_TASK_ATTEMPT_NUM = asVariable("task_attempt_num");
	public static final String SCOPE_TASK_SUBTASK_INDEX = asVariable("subtask_index");

	// ----- Operator ----

	public static final String SCOPE_OPERATOR_ID = asVariable("operator_id");
	public static final String SCOPE_OPERATOR_NAME = asVariable("operator_name");


	// ------------------------------------------------------------------------
	//  Scope Format Base
	//  范围格式基础
	// ------------------------------------------------------------------------

	/**
	 * 比如:
	 * format = tm.<host>.<job_name>
	 *
	 * variables[0] = <job_name>
	 * variables[1] = <host>
	 *
	 * 则
	 * format = tm.<host>.<job_name>
	 *
	 * template[0] = tm
	 * template[1] = <host>
	 * template[2] = <job_name>
	 *
	 * 表示变量在template数组中的索引, 比如<host>的索引是1
	 * templatePos[0] = 1
	 * templatePos[1] = 2
	 *
	 * 表示变量在variables数组中的索引, 比如<host>的索引是1, <job_name>索引是0, 这样用户在定义时, 就很灵活了, 两边不用保持顺序一致
	 * valuePos[0] = 1
	 * valuePos[1] = 0
	 *
	 * 这样就建立了映射关系
	 */

	/**
	 * The scope format.
	 * 范围格式。这是原生的格式
	 * 比如 <host>.jobmanager ，如果为空，则是 <empty>
	 */
	private final String format;

	/**
	 * The format, split into components.
	 * format按照分割符分割后的数组，如 {"<host>", "jobmanager"}
	 * 被<>包裹的元素，是变量元素
	 */
	private final String[] template;

	/** 这是template数组中，变量元素的索引，如"<host>"是变量，在template中的索引是0，则 templatePos = {0} */
	private final int[] templatePos;

	/** 这个是template中变量元素对应的真实的值，在values数组中的位置，详见 构造函数 和 #bindVariables方法 */
	private final int[] valuePos;

	// ------------------------------------------------------------------------

	protected ScopeFormat(String format, ScopeFormat parent, String[] variables) {
		checkNotNull(format, "format is null");

		/** 将format这个字符串分割 */
		final String[] rawComponents = format.split("\\" + SCOPE_SEPARATOR);

		// compute the template array
		/**
		 * 根据是否有父组前缀, 计算模板数组
		 * 根据rawComponents的第一个元素是为"*"，来判断是否要继承父组的范围
		 */
		final boolean parentAsPrefix = rawComponents.length > 0 && rawComponents[0].equals(SCOPE_INHERIT_PARENT);
		if (parentAsPrefix) {
			/** 需要继承父组的范围，而父组有是null，则抛出异常 */
			if (parent == null) {
				throw new IllegalArgumentException("Component scope format requires parent prefix (starts with '"
					+ SCOPE_INHERIT_PARENT + "'), but this component has no parent (is root component).");
			}

			this.format = format.length() > 2 ? format.substring(2) : "<empty>";

			String[] parentTemplate = parent.template;
			int parentLen = parentTemplate.length;

			/** 将父组的范围和自身的范围，合并到一起 */
			this.template = new String[parentLen + rawComponents.length - 1];
			System.arraycopy(parentTemplate, 0, this.template, 0, parentLen);
			System.arraycopy(rawComponents, 1, this.template, parentLen, rawComponents.length - 1);
		}
		else {
			/** 不需要继承父组的范围，则直接赋值 */
			this.format = format.isEmpty() ? "<empty>" : format;
			this.template = rawComponents;
		}

		// --- compute the replacement matrix ---
		// a bit of clumsy Java collections code ;-)

		HashMap<String, Integer> varToValuePos = arrayToMap(variables);
		List<Integer> templatePos = new ArrayList<>();
		List<Integer> valuePos = new ArrayList<>();

		for (int i = 0; i < template.length; i++) {
			final String component = template[i];

			// check if that is a variable
			// 检查是否是一个变量
			if (component != null && component.length() >= 3 &&
					component.charAt(0) == '<' && component.charAt(component.length() - 1) == '>') {

				// this is a variable
				// 这是一个变量
				Integer replacementPos = varToValuePos.get(component);
				if (replacementPos != null) {
					templatePos.add(i);
					valuePos.add(replacementPos);
				}
			}
		}

		this.templatePos = integerListToArray(templatePos);
		this.valuePos = integerListToArray(valuePos);
	}

	// ------------------------------------------------------------------------

	public String format() {
		return format;
	}

	protected final String[] copyTemplate() {
		String[] copy = new String[template.length];
		System.arraycopy(template, 0, copy, 0, template.length);
		return copy;
	}

	protected final String[] bindVariables(String[] template, String[] values) {
		final int len = templatePos.length;
		for (int i = 0; i < len; i++) {
			template[templatePos[i]] = values[valuePos[i]];
		}
		return template;
	}

	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return "ScopeFormat '" + format + '\'';
	}

	// ------------------------------------------------------------------------
	//  Utilities
	//  工具
	// ------------------------------------------------------------------------

	/**
	 * Formats the given string to resemble a scope variable.
	 * 将给定字符格式化成一个范围变量的形式
	 *
	 * @param scope The string to format
	 * @return The formatted string
	 */
	public static String asVariable(String scope) {
		return SCOPE_VARIABLE_PREFIX + scope + SCOPE_VARIABLE_SUFFIX;
	}

	public static String concat(String... components) {
		return concat(defaultFilter, '.', components);
	}

	public static String concat(CharacterFilter filter, String... components) {
		return concat(filter, '.', components);
	}

	public static String concat(Character delimiter, String... components) {
		return concat(defaultFilter, delimiter, components);
	}

	/**
	 * Concatenates the given component names separated by the delimiter character. Additionally
	 * the character filter is applied to all component names.
	 * 使用分隔符连接给定的组件名称。
	 * 另外, 字符过滤器应用到所有的组件名称上。
	 *
	 * @param filter Character filter to be applied to the component names
	 * @param delimiter Delimiter to separate component names
	 * @param components Array of component names
	 * @return The concatenated component name
	 */
	public static String concat(CharacterFilter filter, Character delimiter, String... components) {
		StringBuilder sb = new StringBuilder();
		sb.append(filter.filterCharacters(components[0]));
		for (int x = 1; x < components.length; x++) {
			sb.append(delimiter);
			sb.append(filter.filterCharacters(components[x]));
		}
		return sb.toString();
	}

	protected static String valueOrNull(Object value) {
		return (value == null || (value instanceof String && ((String) value).isEmpty())) ?
				"null" : value.toString();
	}

	protected static HashMap<String, Integer> arrayToMap(String[] array) {
		HashMap<String, Integer> map = new HashMap<>(array.length);
		for (int i = 0; i < array.length; i++) {
			map.put(array[i], i);
		}
		return map;
	}

	private static int[] integerListToArray(List<Integer> list) {
		int[] array = new int[list.size()];
		int pos = 0;
		for (Integer i : list) {
			array[pos++] = i;
		}
		return array;
	}
}
