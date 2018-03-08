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

package org.apache.flink.streaming.api.transformations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.MissingTypeInfo;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.util.Preconditions;

import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@code StreamTransformation} represents the operation that creates a
 * {@link org.apache.flink.streaming.api.datastream.DataStream}. Every
 * {@link org.apache.flink.streaming.api.datastream.DataStream} has an underlying
 * {@code StreamTransformation} that is the origin of said DataStream.
 * 一个StreamTransformation描述了创建一个DataStream的操作。
 * 每一个DataStream都有一个关联的StreamTransformation,是该DataStream的起源
 *
 * <p>API operations such as {@link org.apache.flink.streaming.api.datastream.DataStream#map} create
 * a tree of {@code StreamTransformation}s underneath. When the stream program is to be executed
 * this graph is translated to a {@link StreamGraph} using
 * {@link org.apache.flink.streaming.api.graph.StreamGraphGenerator}.
 * 类似{@link org.apache.flink.streaming.api.datastream.DataStream#map}的API操作创建了一个{@code StreamTransformation}树。
 *
 * <p>A {@code StreamTransformation} does not necessarily correspond to a physical operation
 * at runtime. Some operations are only logical concepts. Examples of this are union,
 * split/select data stream, partitioning.
 * {@code StreamTransformation}不一定对应一个运行时的物理操作。
 * 某些操作只是逻辑概念。
 * 比如 union、split、select、分区等。
 *
 * <p>The following graph of {@code StreamTransformations}:
 * <pre>{@code
 *   Source              Source
 *      +                   +
 *      |                   |
 *      v                   v
 *  Rebalance          HashPartition
 *      +                   +
 *      |                   |
 *      |                   |
 *      +------>Union<------+
 *                +
 *                |
 *                v
 *              Split
 *                +
 *                |
 *                v
 *              Select
 *                +
 *                v
 *               Map
 *                +
 *                |
 *                v
 *              Sink
 * }</pre>
 *
 * <p>Would result in this graph of operations at runtime:
 * <pre>{@code
 *  Source              Source
 *    +                   +
 *    |                   |
 *    |                   |
 *    +------->Map<-------+
 *              +
 *              |
 *              v
 *             Sink
 * }</pre>
 *
 * <p>The information about partitioning, union, split/select end up being encoded in the edges
 * that connect the sources to the map operation.
 *
 * @param <T> The type of the elements that result from this {@code StreamTransformation}
 */
@Internal
public abstract class StreamTransformation<T> {

	// This is used to assign a unique ID to every StreamTransformation
	// 用来为每个{@code StreamTransformation}分配一个唯一的ID
	protected static Integer idCounter = 0;

	public static int getNewNodeId() {
		idCounter++;
		return idCounter;
	}


	protected final int id;

	protected String name;

	protected TypeInformation<T> outputType;

	// This is used to handle MissingTypeInfo. As long as the outputType has not been queried
	// it can still be changed using setOutputType(). Afterwards an exception is thrown when
	// trying to change the output type.
	/**
	 * 这个属性是用来处理{@link MissingTypeInfo}。
	 * {@link outputType}只要还没有被查询过, 就可以通过{@link #setOutputType(TypeInformation)}方法进行变更设置。
	 * 否则, 当尝试变更{@link outputType}时就会抛出异常。
	 */
	protected boolean typeUsed;

	private int parallelism;

	/**
	 * The maximum parallelism for this stream transformation. It defines the upper limit for
	 * dynamic scaling and the number of key groups used for partitioned state.
	 * 这个{@code StreamTransformation}的最大并行度。
	 * 它定义了动态扩展的上限, 以及用于分区状态的key-group的数量
	 */
	private int maxParallelism = -1;

	/**
	 *  The minimum resources for this stream transformation. It defines the lower limit for
	 *  dynamic resources resize in future plan.
	 *  这个{@code StreamTransformation}的最小资源。
	 *  它定义了在未来计划中资源动态调整的下限。
	 */
	private ResourceSpec minResources = ResourceSpec.DEFAULT;

	/**
	 *  The preferred resources for this stream transformation. It defines the upper limit for
	 *  dynamic resource resize in future plan.
	 *  这个{@code StreamTransformation}的最优资源。
	 *  它定义了资源动态调整的上限。
	 */
	private ResourceSpec preferredResources = ResourceSpec.DEFAULT;

	/**
	 * User-specified ID for this transformation. This is used to assign the
	 * same operator ID across job restarts. There is also the automatically
	 * generated {@link #id}, which is assigned from a static counter. That
	 * field is independent from this.
	 * {@code StreamTransformation}的用户自定义id。
	 * 这个是用来在job重启时分配相同的操作符id。
	 * 这里也有自动产生的id, 从一个静态的计数器来分配的。
	 * {@link idCounter}字段是独立的
	 */
	private String uid;

	/**
	 * 用户提供的节点hash值
	 */
	private String userProvidedNodeHash;

	protected long bufferTimeout = -1;

	private String slotSharingGroup;

	/**
	 * Creates a new {@code StreamTransformation} with the given name, output type and parallelism.
	 * 用给定的名称、输出类型、并行度构建一个新的{@code StreamTransformation}
	 *
	 * @param name The name of the {@code StreamTransformation}, this will be shown in Visualizations and the Log
	 * @param outputType The output type of this {@code StreamTransformation}
	 * @param parallelism The parallelism of this {@code StreamTransformation}
	 */
	public StreamTransformation(String name, TypeInformation<T> outputType, int parallelism) {
		this.id = getNewNodeId();
		this.name = Preconditions.checkNotNull(name);
		this.outputType = outputType;
		this.parallelism = parallelism;
		this.slotSharingGroup = null;
	}

	/**
	 * Returns the unique ID of this {@code StreamTransformation}.
	 */
	public int getId() {
		return id;
	}

	/**
	 * Changes the name of this {@code StreamTransformation}.
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * Returns the name of this {@code StreamTransformation}.
	 */
	public String getName() {
		return name;
	}

	/**
	 * Returns the parallelism of this {@code StreamTransformation}.
	 */
	public int getParallelism() {
		return parallelism;
	}

	/**
	 * Sets the parallelism of this {@code StreamTransformation}.
	 *
	 * @param parallelism The new parallelism to set on this {@code StreamTransformation}.
	 */
	public void setParallelism(int parallelism) {
		Preconditions.checkArgument(parallelism > 0, "Parallelism must be bigger than zero.");
		this.parallelism = parallelism;
	}

	/**
	 * Gets the maximum parallelism for this stream transformation.
	 *
	 * @return Maximum parallelism of this transformation.
	 */
	public int getMaxParallelism() {
		return maxParallelism;
	}

	/**
	 * Sets the maximum parallelism for this stream transformation.
	 *
	 * @param maxParallelism Maximum parallelism for this stream transformation.
	 */
	public void setMaxParallelism(int maxParallelism) {
		Preconditions.checkArgument(maxParallelism > 0
						&& maxParallelism <= StreamGraphGenerator.UPPER_BOUND_MAX_PARALLELISM,
				"Maximum parallelism must be between 1 and " + StreamGraphGenerator.UPPER_BOUND_MAX_PARALLELISM
						+ ". Found: " + maxParallelism);
		this.maxParallelism = maxParallelism;
	}

	/**
	 * Sets the minimum and preferred resources for this stream transformation.
	 *
	 * @param minResources The minimum resource of this transformation.
	 * @param preferredResources The preferred resource of this transformation.
	 */
	public void setResources(ResourceSpec minResources, ResourceSpec preferredResources) {
		this.minResources = checkNotNull(minResources);
		this.preferredResources = checkNotNull(preferredResources);
	}

	/**
	 * Gets the minimum resource of this stream transformation.
	 *
	 * @return The minimum resource of this transformation.
	 */
	public ResourceSpec getMinResources() {
		return minResources;
	}

	/**
	 * Gets the preferred resource of this stream transformation.
	 *
	 * @return The preferred resource of this transformation.
	 */
	public ResourceSpec getPreferredResources() {
		return preferredResources;
	}

	/**
	 * Sets an user provided hash for this operator. This will be used AS IS the create the
	 * JobVertexID.
	 * 为这个操作符设置一个用户提供的散列值。
	 * 这将用于创建{@link org.apache.flink.runtime.jobgraph.JobVertexID}
	 *
	 * <p>The user provided hash is an alternative to the generated hashes, that is considered when
	 * identifying an operator through the default hash mechanics fails (e.g. because of changes
	 * between Flink versions).
	 * 用户提供的散列值是用来产生散列值的另一种方式，
	 * 当通过默认的散列机制来标识一个操作符失败时(比如因为在{@code flink}的版本之间发生了变化)，会采用用户提供的值。
	 *
	 * <p><strong>Important</strong>: this should be used as a workaround or for trouble shooting.
	 * The provided hash needs to be unique per transformation and job. Otherwise, job submission
	 * will fail. Furthermore, you cannot assign user-specified hash to intermediate nodes in an
	 * operator chain and trying so will let your job fail.
	 * <strong>重要</strong>：这应该作为一种权益权宜方案或者问题排查。
	 * 提供的散列值需要对每个{@code StreamTransformation}和{@code job}都是唯一的。否则，提交{@code job}将失败。
	 * 另外，你不能给一个操作符链的中间节点分配用户指定的散列值，这样的尝试会让你的{@code job}失败。
	 *
	 * <p>A use case for this is in migration between Flink versions or changing the jobs in a way
	 * that changes the automatically generated hashes. In this case, providing the previous hashes
	 * directly through this method (e.g. obtained from old logs) can help to reestablish a lost
	 * mapping from states to their target operator.
	 * 一个使用场景是在{@code flink}版本间迁移或者改变{@code job}时会导致自动产生的散列值也变化。
	 * 在这种情况下，通过这个方法直接提供之前的散列值(从旧的日志中获取)，可以帮助从状态中重建一个丢失的到目标操作符的映射。
	 *
	 * @param uidHash The user provided hash for this operator. This will become the JobVertexID, which is shown in the
	 *                 logs and web ui.
	 *                用户为这个操作符提供的散列值。这会变成{@link org.apache.flink.runtime.jobgraph.JobVertexID}，
	 *                其会显示在日志和{@code web}界面中。
	 */
	public void setUidHash(String uidHash) {

		Preconditions.checkNotNull(uidHash);
		Preconditions.checkArgument(uidHash.matches("^[0-9A-Fa-f]{32}$"),
				"Node hash must be a 32 character String that describes a hex code. Found: " + uidHash);

		this.userProvidedNodeHash = uidHash;
	}

	/**
	 * Gets the user provided hash.
	 *
	 * @return The user provided hash.
	 */
	public String getUserProvidedNodeHash() {
		return userProvidedNodeHash;
	}

	/**
	 * Sets an ID for this {@link StreamTransformation}. This is will later be hashed to a uidHash which is then used to
	 * create the JobVertexID (that is shown in logs and the web ui).
	 * 为该 StreamTransformation 设置一个id。会被hash为一个hashid，用来构建 JobVertexID（用来在日志和web ui中显示）
	 *
	 * <p>The specified ID is used to assign the same operator ID across job
	 * submissions (for example when starting a job from a savepoint).
	 *
	 * <p><strong>Important</strong>: this ID needs to be unique per
	 * transformation and job. Otherwise, job submission will fail.
	 *
	 * @param uid The unique user-specified ID of this transformation.
	 */
	public void setUid(String uid) {
		this.uid = uid;
	}

	/**
	 * Returns the user-specified ID of this transformation.
	 *
	 * @return The unique user-specified ID of this transformation.
	 */
	public String getUid() {
		return uid;
	}

	/**
	 * Returns the slot sharing group of this transformation.
	 *
	 * @see #setSlotSharingGroup(String)
	 */
	public String getSlotSharingGroup() {
		return slotSharingGroup;
	}

	/**
	 * Sets the slot sharing group of this transformation. Parallel instances of operations that
	 * are in the same slot sharing group will be co-located in the same TaskManager slot, if
	 * possible.
	 *
	 * <p>Initially, an operation is in the default slot sharing group. This can be explicitly
	 * set using {@code setSlotSharingGroup("default")}.
	 *
	 * @param slotSharingGroup The slot sharing group name.
	 */
	public void setSlotSharingGroup(String slotSharingGroup) {
		this.slotSharingGroup = slotSharingGroup;
	}

	/**
	 * Tries to fill in the type information. Type information can be filled in
	 * later when the program uses a type hint. This method checks whether the
	 * type information has ever been accessed before and does not allow
	 * modifications if the type was accessed already. This ensures consistency
	 * by making sure different parts of the operation do not assume different
	 * type information.
	 * 尝试设置类型信息。
	 * 当程序使用一个类型提示时，可以稍晚些设置类型信息。
	 * 该方法检测类型信息是否已经被访问过，如果已经被访问过，则不允许修改。
	 * 通过确保操作的不同部分不会采用不同的类型信息，从而确保一致性。
	 *
	 * @param outputType The type information to fill in.
	 *
	 * @throws IllegalStateException Thrown, if the type information has been accessed before.
	 */
	public void setOutputType(TypeInformation<T> outputType) {
		if (typeUsed) {
			throw new IllegalStateException(
					"TypeInformation cannot be filled in for the type after it has been used. "
							+ "Please make sure that the type info hints are the first call after"
							+ " the transformation function, "
							+ "before any access to types or semantic properties, etc.");
		}
		this.outputType = outputType;
	}

	/**
	 * Returns the output type of this {@code StreamTransformation} as a {@link TypeInformation}. Once
	 * this is used once the output type cannot be changed anymore using {@link #setOutputType}.
	 * 将{@code StreamTransformation}的输出类型作为一个{@code TypeInformation}返回。
	 * 一旦调用该方法后，就不能再使用@link #setOutputType}方法对输出类型做任何的变化了。
	 *
	 * @return The output type of this {@code StreamTransformation}
	 */
	public TypeInformation<T> getOutputType() {
		if (outputType instanceof MissingTypeInfo) {
			MissingTypeInfo typeInfo = (MissingTypeInfo) this.outputType;
			throw new InvalidTypesException(
					"The return type of function '"
							+ typeInfo.getFunctionName()
							+ "' could not be determined automatically, due to type erasure. "
							+ "You can give type information hints by using the returns(...) "
							+ "method on the result of the transformation call, or by letting "
							+ "your function implement the 'ResultTypeQueryable' "
							+ "interface.", typeInfo.getTypeException());
		}
		typeUsed = true;
		return this.outputType;
	}

	/**
	 * Sets the chaining strategy of this {@code StreamTransformation}.
	 * 设置这个{@code StreamTransformation}的链接策略
	 */
	public abstract void setChainingStrategy(ChainingStrategy strategy);

	/**
	 * Set the buffer timeout of this {@code StreamTransformation}. The timeout is used when
	 * sending elements over the network. The timeout specifies how long a network buffer
	 * should be kept waiting before sending. A higher timeout means that more elements will
	 * be sent in one buffer, this increases throughput. The latency, however, is negatively
	 * affected by a higher timeout.
	 */
	public void setBufferTimeout(long bufferTimeout) {
		this.bufferTimeout = bufferTimeout;
	}

	/**
	 * Returns the buffer timeout of this {@code StreamTransformation}.
	 *
	 * @see #setBufferTimeout(long)
	 */
	public long getBufferTimeout() {
		return bufferTimeout;
	}

	/**
	 * Returns all transitive predecessor {@code StreamTransformation}s of this {@code StreamTransformation}. This
	 * is, for example, used when determining whether a feedback edge of an iteration
	 * actually has the iteration head as a predecessor.
	 * 返回该{@code StreamTransformation}的所有前置@code StreamTransformation}。
	 * 比如，当要决定一个迭代的反馈边的前置中是否有迭代头时，会使用。
	 *
	 * @return The list of transitive predecessors.
	 */
	public abstract Collection<StreamTransformation<?>> getTransitivePredecessors();

	@Override
	public String toString() {
		return getClass().getSimpleName() + "{" +
				"id=" + id +
				", name='" + name + '\'' +
				", outputType=" + outputType +
				", parallelism=" + parallelism +
				'}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof StreamTransformation)) {
			return false;
		}

		StreamTransformation<?> that = (StreamTransformation<?>) o;

		if (bufferTimeout != that.bufferTimeout) {
			return false;
		}
		if (id != that.id) {
			return false;
		}
		if (parallelism != that.parallelism) {
			return false;
		}
		if (!name.equals(that.name)) {
			return false;
		}
		return outputType != null ? outputType.equals(that.outputType) : that.outputType == null;
	}

	@Override
	public int hashCode() {
		int result = id;
		result = 31 * result + name.hashCode();
		result = 31 * result + (outputType != null ? outputType.hashCode() : 0);
		result = 31 * result + parallelism;
		result = 31 * result + (int) (bufferTimeout ^ (bufferTimeout >>> 32));
		return result;
	}
}
