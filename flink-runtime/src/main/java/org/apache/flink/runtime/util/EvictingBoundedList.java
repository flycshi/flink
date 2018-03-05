/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.util;

import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * This class implements a list (array based) that is physically bounded in maximum size, but can virtually grow beyond
 * the bounded size. When the list grows beyond the size bound, elements are dropped from the head of the list (FIFO
 * order). If dropped elements are accessed, a default element is returned instead.
 * 这个类实现类一个列表(基于数组的), 它有一个最大的物理上界, 但是可以超出最大限制。
 * 当这个列表超出最大限制时, 元素会从列表头删除(FIFO顺序)。如果被删除的元素还可以被访问,则返回一个默认的元素
 * 
 * <p>The list by itself is serializable, but a full list can only be serialized if the values
 * are also serializable.
 *
 * @param <T> type of the list elements
 */
public class EvictingBoundedList<T> implements Iterable<T>, Serializable {

	private static final long serialVersionUID = -1863961980953613146L;

	@SuppressWarnings("NonSerializableFieldInSerializableClass")
	/**
	 * the default element returned for positions that were evicted
	 * 为已经丢弃位置, 返回的默认元素
	 */
	private final T defaultElement;

	@SuppressWarnings("NonSerializableFieldInSerializableClass")
	/**
	 * the array (viewed as a circular buffer) that holds the latest (= non-evicted) elements
	 * 数组(看做一个循环缓存), 持有最新的(没有丢弃的)元素
	 */
	private final Object[] elements;

	/**
	 * The next index to put an element in the array
	 * 向数组中添加一个元素的下一个位置
	 */
	private int idx;

	/**
	 * The current number of (virtual) elements in the list
	 * 列表中的元素(包括虚拟的)的当前数量
	 */
	private int count;

	/**
	 * Modification count for fail-fast iterators
	 * 用于快速失败迭代的修改次数记录
	 */
	private long modCount;

	// ------------------------------------------------------------------------

	public EvictingBoundedList(int sizeLimit) {
		this(sizeLimit, null);
	}

	public EvictingBoundedList(EvictingBoundedList<T> other) {
		Preconditions.checkNotNull(other);
		this.defaultElement = other.defaultElement;
		this.elements = other.elements.clone();
		this.idx = other.idx;
		this.count = other.count;
		this.modCount = 0L;
	}

	public EvictingBoundedList(int sizeLimit, T defaultElement) {
		this.elements = new Object[sizeLimit];
		this.defaultElement = defaultElement;
		this.idx = 0;
		this.count = 0;
		this.modCount = 0L;
	}

	// ------------------------------------------------------------------------

	public int size() {
		return count;
	}

	public boolean isEmpty() {
		return 0 == count;
	}

	public boolean add(T t) {
		elements[idx] = t;
		idx = (idx + 1) % elements.length;
		++count;
		++modCount;
		return true;
	}

	public void clear() {
		if (!isEmpty()) {
			for (int i = 0; i < elements.length; ++i) {
				elements[i] = null;
			}
			count = 0;
			idx = 0;
			++modCount;
		}
	}

	public T get(int index) {
		if (index >= 0 && index < count) {
			return isDroppedIndex(index) ? getDefaultElement() : accessInternal(index % elements.length);
		} else {
			throw new IndexOutOfBoundsException(String.valueOf(index));
		}
	}

	public int getSizeLimit() {
		return elements.length;
	}

	public T set(int index, T element) {
		Preconditions.checkArgument(index >= 0 && index < count);
		++modCount;
		if (isDroppedIndex(index)) {
			return getDefaultElement();
		} else {
			int idx = index % elements.length;
			T old = accessInternal(idx);
			elements[idx] = element;
			return old;
		}
	}

	public T getDefaultElement() {
		return defaultElement;
	}

	private boolean isDroppedIndex(int idx) {
		return idx < count - elements.length;
	}

	@SuppressWarnings("unchecked")
	private T accessInternal(int arrayIndex) {
		return (T) elements[arrayIndex];
	}

	@Override
	public Iterator<T> iterator() {
		return new Iterator<T>() {

			int pos = 0;
			final long oldModCount = modCount;

			@Override
			public boolean hasNext() {
				return pos < count;
			}

			@Override
			public T next() {
				if (oldModCount != modCount) {
					throw new ConcurrentModificationException();
				}
				if (pos < count) {
					return get(pos++);
				} else {
					throw new NoSuchElementException("Iterator exhausted.");
				}
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException("Read-only iterator");
			}
		};
	}

	/**
	 * Creates a new list that replaces its elements with transformed elements.
	 * The list retains the same size and position-to-element mapping.
	 * 构建一个新的list, 新list会将老list中的元素都替换为新的值。
	 * 新list保持相同的大小,以及元素的位置。
	 * 
	 * <p>Note that null values are automatically mapped to null values.
	 * 		null值自动转换为null值
	 * 
	 * @param transform The function used to transform each element
	 * @param <R> The type of the elements in the result list.
	 * 
	 * @return The list with the mapped elements
	 */
	public <R> EvictingBoundedList<R> map(Function<T, R> transform) {
		// map the default element
		final R newDefault = defaultElement == null ? null : transform.apply(defaultElement);

		// copy the list with the new default
		final EvictingBoundedList<R> result = new EvictingBoundedList<>(elements.length, newDefault);
		result.count = count;
		result.idx = idx;

		// map all the entries in the list
		final int numElements = Math.min(elements.length, count);
		for (int i = 0; i < numElements; i++) {
			result.elements[i] = transform.apply(accessInternal(i));
		}

		return result;
	}

	// ------------------------------------------------------------------------

	/**
	 * A simple unary function that can be used to transform elements via the
	 * {@link EvictingBoundedList#map(Function)} method.
	 * 一个简单的一元方程
	 */
	public interface Function<I, O> {

		/**
		 * Transforms the value.
		 * 
		 * @param value The value to transform
		 * @return The transformed value
		 */
		O apply(I value);
	}
}
