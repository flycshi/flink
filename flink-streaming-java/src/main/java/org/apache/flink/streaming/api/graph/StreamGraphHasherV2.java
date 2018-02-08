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

package org.apache.flink.streaming.api.graph;

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.transformations.StreamTransformation;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;

import org.apache.flink.shaded.guava18.com.google.common.hash.HashFunction;
import org.apache.flink.shaded.guava18.com.google.common.hash.Hasher;
import org.apache.flink.shaded.guava18.com.google.common.hash.Hashing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import static org.apache.flink.util.StringUtils.byteToHexString;

/**
 * StreamGraphHasher from Flink 1.2. This contains duplicated code to ensure that the algorithm does not change with
 * future Flink versions.
 *
 * <p>DO NOT MODIFY THIS CLASS
 */
public class StreamGraphHasherV2 implements StreamGraphHasher {

	private static final Logger LOG = LoggerFactory.getLogger(StreamGraphHasherV2.class);

	/**
	 * Returns a map with a hash for each {@link StreamNode} of the {@link
	 * StreamGraph}. The hash is used as the {@link JobVertexID} in order to
	 * identify nodes across job submissions if they didn't change.
	 *
	 * <p>The complete {@link StreamGraph} is traversed. The hash is either
	 * computed from the transformation's user-specified id (see
	 * {@link StreamTransformation#getUid()}) or generated in a deterministic way.
	 * 整个StreamGraph都被遍历。
	 * hash值要么从用户指定的id计算得来,要么以确定的方式来产生。
	 *
	 * <p>The generated hash is deterministic with respect to:
	 * 		产生的确定的hash值,可以参考如下特征
	 * <ul>
	 *   <li>node-local properties (like parallelism, UDF, node ID),
	 *   		节点本地属性(比如并行度,udf,节点id)
	 *   <li>chained output nodes, and
	 *   		连接的输出节点
	 *   <li>input nodes hashes
	 *   		输入节点的hash值
	 * </ul>
	 *
	 * @return A map from {@link StreamNode#id} to hash as 16-byte array.
	 */
	@Override
	public Map<Integer, byte[]> traverseStreamGraphAndGenerateHashes(StreamGraph streamGraph) {
		// The hash function used to generate the hash
		final HashFunction hashFunction = Hashing.murmur3_128(0);
		final Map<Integer, byte[]> hashes = new HashMap<>();

		Set<Integer> visited = new HashSet<>();
		Queue<StreamNode> remaining = new ArrayDeque<>();

		// We need to make the source order deterministic. The source IDs are
		// not returned in the same order, which means that submitting the same
		// program twice might result in different traversal, which breaks the
		// deterministic hash assignment.
		/**
		 * 我们需要让源节点id的顺序是确定的。
		 * 源id集合是无序的,也就意味着提交相同程序两次可能会导致不同的遍历次序,这会导致hash值不是确定的
		 */
		List<Integer> sources = new ArrayList<>();
		for (Integer sourceNodeId : streamGraph.getSourceIDs()) {
			sources.add(sourceNodeId);
		}
		Collections.sort(sources);

		//
		// Traverse the graph in a breadth-first manner. Keep in mind that
		// the graph is not a tree and multiple paths to nodes can exist.
		//
		/**
		 * 以宽度优先的方式遍历graph。
		 * 需要谨记,graph不是一个树,到达一个节点可以有多条路径。
		 */

		// Start with source nodes
		for (Integer sourceNodeId : sources) {
			remaining.add(streamGraph.getStreamNode(sourceNodeId));
			visited.add(sourceNodeId);
		}

		StreamNode currentNode;
		while ((currentNode = remaining.poll()) != null) {
			// Generate the hash code. Because multiple path exist to each
			// node, we might not have all required inputs available to
			// generate the hash code.
			/**
			 * 产生hash值。
			 * 由于每个节点存在多条路径,在产生hash值时,可能会存在不具备所有的所需输入的情况
			 */
			if (generateNodeHash(currentNode, hashFunction, hashes, streamGraph.isChainingEnabled())) {
				// Add the child nodes
				for (StreamEdge outEdge : currentNode.getOutEdges()) {
					StreamNode child = outEdge.getTargetVertex();

					if (!visited.contains(child.getId())) {
						remaining.add(child);
						visited.add(child.getId());
					}
				}
			} else {
				// We will revisit this later.
				/**
				 * 当前条件不具备,后续再重新visit
				 */
				visited.remove(currentNode.getId());
			}
		}

		return hashes;
	}

	/**
	 * Generates a hash for the node and returns whether the operation was
	 * successful.
	 * 为节点产生一个hash值,并返回是否操作成功的标识
	 *
	 * @param node         The node to generate the hash for
	 * @param hashFunction The hash function to use
	 * @param hashes       The current state of generated hashes
	 * @return <code>true</code> if the node hash has been generated.
	 * <code>false</code>, otherwise. If the operation is not successful, the
	 * hash needs be generated at a later point when all input is available.
	 * @throws IllegalStateException If node has user-specified hash and is
	 *                               intermediate node of a chain
	 */
	private boolean generateNodeHash(
			StreamNode node,
			HashFunction hashFunction,
			Map<Integer, byte[]> hashes,
			boolean isChainingEnabled) {

		// Check for user-specified ID
		String userSpecifiedHash = node.getTransformationUID();

		if (userSpecifiedHash == null) {
			/**
			 * Check that all input nodes have their hashes computed
			 * 检查是否所有的输入节点的hash值都被计算好了
			 */
			for (StreamEdge inEdge : node.getInEdges()) {
				// If the input node has not been visited yet, the current
				// node will be visited again at a later point when all input
				// nodes have been visited and their hashes set.
				/** 如果输入节点还没有都被遍历过,当前节点会稍后等所有输入节点都遍历过,切hash被设置之后,再重新遍历 */
				if (!hashes.containsKey(inEdge.getSourceId())) {
					return false;
				}
			}

			Hasher hasher = hashFunction.newHasher();
			byte[] hash = generateDeterministicHash(node, hasher, hashes, isChainingEnabled);

			if (hashes.put(node.getId(), hash) != null) {
				// Sanity check
				throw new IllegalStateException("Unexpected state. Tried to add node hash " +
						"twice. This is probably a bug in the JobGraph generator.");
			}

			return true;
		} else {
			Hasher hasher = hashFunction.newHasher();
			byte[] hash = generateUserSpecifiedHash(node, hasher);

			for (byte[] previousHash : hashes.values()) {
				if (Arrays.equals(previousHash, hash)) {
					throw new IllegalArgumentException("Hash collision on user-specified ID. " +
							"Most likely cause is a non-unique ID. Please check that all IDs " +
							"specified via `uid(String)` are unique.");
				}
			}

			if (hashes.put(node.getId(), hash) != null) {
				// Sanity check
				throw new IllegalStateException("Unexpected state. Tried to add node hash " +
						"twice. This is probably a bug in the JobGraph generator.");
			}

			return true;
		}
	}

	/**
	 * Generates a hash from a user-specified ID.
	 */
	private byte[] generateUserSpecifiedHash(StreamNode node, Hasher hasher) {
		hasher.putString(node.getTransformationUID(), Charset.forName("UTF-8"));

		return hasher.hash().asBytes();
	}

	/**
	 * Generates a deterministic hash from node-local properties and input and
	 * output edges.
	 * 从节点本地属性、输入、以及输出边界,产生一个确定的散列值
	 */
	private byte[] generateDeterministicHash(
			StreamNode node,
			Hasher hasher,
			Map<Integer, byte[]> hashes,
			boolean isChainingEnabled) {

		// Include stream node to hash. We use the current size of the computed
		// hashes as the ID. We cannot use the node's ID, because it is
		// assigned from a static counter. This will result in two identical
		// programs having different hashes.
		/**
		 * 将流节点放入hash。
		 * 我们使用当前已经计算hash的节点数作为id。
		 * 不能使用节点的ID,因为它是从一个静态的自增器产生的。这会导致两个独立的程序具有不同的hash
		 */
		generateNodeLocalHash(node, hasher, hashes.size());

		// Include chained nodes to hash
		for (StreamEdge outEdge : node.getOutEdges()) {
			if (isChainable(outEdge, isChainingEnabled)) {
				StreamNode chainedNode = outEdge.getTargetVertex();

				// Use the hash size again, because the nodes are chained to
				// this node. This does not add a hash for the chained nodes.
				/**
				 * 再次使用hash的个数,因为节点被连接到该节点上了,也就是连接在一起的节点的id是一样的。
				 */
				generateNodeLocalHash(chainedNode, hasher, hashes.size());
			}
		}

		byte[] hash = hasher.hash().asBytes();

		// Make sure that all input nodes have their hash set before entering
		// this loop (calling this method).
		/**
		 * 进入该循环前,需要确保所有输入节点的hash已经被设置好
		 */
		for (StreamEdge inEdge : node.getInEdges()) {
			byte[] otherHash = hashes.get(inEdge.getSourceId());

			// Sanity check
			if (otherHash == null) {
				throw new IllegalStateException("Missing hash for input node "
						+ inEdge.getSourceVertex() + ". Cannot generate hash for "
						+ node + ".");
			}

			for (int j = 0; j < hash.length; j++) {
				hash[j] = (byte) (hash[j] * 37 ^ otherHash[j]);
			}
		}

		if (LOG.isDebugEnabled()) {
			String udfClassName = "";
			if (node.getOperator() instanceof AbstractUdfStreamOperator) {
				udfClassName = ((AbstractUdfStreamOperator<?, ?>) node.getOperator())
						.getUserFunction().getClass().getName();
			}

			LOG.debug("Generated hash '" + byteToHexString(hash) + "' for node " +
					"'" + node.toString() + "' {id: " + node.getId() + ", " +
					"parallelism: " + node.getParallelism() + ", " +
					"user function: " + udfClassName + "}");
		}

		return hash;
	}

	/**
	 * Applies the {@link Hasher} to the {@link StreamNode} (only node local
	 * attributes are taken into account). The hasher encapsulates the current
	 * state of the hash.
	 * 将hasher应用到StreamNode上(仅考虑节点本地属性)
	 *
	 * <p>The specified ID is local to this node. We cannot use the
	 * {@link StreamNode#id}, because it is incremented in a static counter.
	 * Therefore, the IDs for identical jobs will otherwise be different.
	 */
	private void generateNodeLocalHash(StreamNode node, Hasher hasher, int id) {
		// This resolves conflicts for otherwise identical source nodes. BUT
		// the generated hash codes depend on the ordering of the nodes in the
		// stream graph.
		hasher.putInt(id);
	}

	private boolean isChainable(StreamEdge edge, boolean isChainingEnabled) {
		StreamNode upStreamVertex = edge.getSourceVertex();
		StreamNode downStreamVertex = edge.getTargetVertex();

		StreamOperator<?> headOperator = upStreamVertex.getOperator();
		StreamOperator<?> outOperator = downStreamVertex.getOperator();

		/**
		 * 1、下游节点只有一个输入
		 * 2、下游节点的操作符不为null
		 * 3、上游节点的操作符不为null
		 * 4、上下游节点在一个槽位共享组内
		 * 5、下游节点的连接策略是 ALWAYS
		 * 6、上游节点的连接策略是 HEAD 或者 ALWAYS
		 * 7、edge 的分区函数是 ForwardPartitioner 的实例
		 * 8、上下游节点的并行度相等
		 * 9、可以进行节点连接操作
		 * 只有上述9条都未true时,才返回true
		 */
		return downStreamVertex.getInEdges().size() == 1
				&& outOperator != null
				&& headOperator != null
				&& upStreamVertex.isSameSlotSharingGroup(downStreamVertex)
				&& outOperator.getChainingStrategy() == ChainingStrategy.ALWAYS
				&& (headOperator.getChainingStrategy() == ChainingStrategy.HEAD ||
				headOperator.getChainingStrategy() == ChainingStrategy.ALWAYS)
				&& (edge.getPartitioner() instanceof ForwardPartitioner)
				&& upStreamVertex.getParallelism() == downStreamVertex.getParallelism()
				&& isChainingEnabled;
	}
}
