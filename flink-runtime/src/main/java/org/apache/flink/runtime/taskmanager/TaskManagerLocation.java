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

package org.apache.flink.runtime.taskmanager;

import java.net.InetAddress;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.util.NetUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class encapsulates the connection information of a TaskManager.
 * It describes the host where the TaskManager operates and its server port
 * for data exchange. This class also contains utilities to work with the
 * TaskManager's host name, which is used to localize work assignments.
 * 这个类封装了一个 TaskManager 的连接信息。
 * 描述了 TaskManager 的主机名，以及用于数据交换的服务端口。
 * 这个类也包含了用于处理 TaskManager 主机名的工具，该主机名用于本地化工作分配。
 */
public class TaskManagerLocation implements Comparable<TaskManagerLocation>, java.io.Serializable {

	private static final long serialVersionUID = -8254407801276350716L;

	private static final Logger LOG = LoggerFactory.getLogger(TaskManagerLocation.class);

	// ------------------------------------------------------------------------

	/**
	 * The ID of the resource in which the TaskManager is started. This can be for example
	 * the YARN container ID, Mesos container ID, or any other unique identifier.
	 * TaskManager 启动所在的 resource ID。
	 * 这可以是 yarn 容器 id，mesos 容器 id，或者任何其他唯一标识。
	 */
	private final ResourceID resourceID;

	/**
	 * The network address that the TaskManager binds its sockets to
	 * TaskManager 绑定其 sockets 的网络地址
	 */
	private final InetAddress inetAddress;

	/**
	 * The fully qualified host name of the TaskManager
	 * TaskManager 的全限定主机名
	 */
	private final String fqdnHostName;

	/**
	 * The pure hostname, derived from the fully qualified host name.
	 * 从全限定主机名中腿短出的纯主机名
	 */
	private final String hostName;
	
	/**
	 * The port that the TaskManager receive data transport connection requests at
	 * TaskManager 接收 数据传输连接请求 的端口
	 */
	private final int dataPort;

	/**
	 * The toString representation, eagerly constructed and cached to avoid repeated string building
	 * toString 表示，快速构建并缓存，以防重复的字符串构建。
	 */
	private final String stringRepresentation;

	/**
	 * Constructs a new instance connection info object. The constructor will attempt to retrieve the instance's
	 * host name and domain name through the operating system's lookup mechanisms.
	 * 构建一个新的连接信息对象的实例。
	 * 构建函数会尝试从操作系统的查询机制中提取出实例的主机名和域名
	 * 
	 * @param inetAddress
	 *        the network address the instance's task manager binds its sockets to
	 * @param dataPort
	 *        the port instance's task manager expects to receive transfer envelopes on
	 */
	public TaskManagerLocation(ResourceID resourceID, InetAddress inetAddress, int dataPort) {
		// -1 indicates a local instance connection info
		// -1 表示一个本地连接信息的实例
		checkArgument(dataPort > 0 || dataPort == -1, "dataPort must be > 0, or -1 (local)");

		this.resourceID = checkNotNull(resourceID);
		this.inetAddress = checkNotNull(inetAddress);
		this.dataPort = dataPort;

		// get FQDN hostname on this TaskManager.
		// 获取该 TaskManager 的全限定主机名
		String fqdnHostName;
		try {
			fqdnHostName = this.inetAddress.getCanonicalHostName();
		}
		catch (Throwable t) {
			LOG.warn("Unable to determine the canonical hostname. Input split assignment (such as " +
					"for HDFS files) may be non-local when the canonical hostname is missing.");
			LOG.debug("getCanonicalHostName() Exception:", t);
			fqdnHostName = this.inetAddress.getHostAddress();
		}
		this.fqdnHostName = fqdnHostName;

		if (this.fqdnHostName.equals(this.inetAddress.getHostAddress())) {
			// this happens when the name lookup fails, either due to an exception,
			// or because no hostname can be found for the address
			// take IP textual representation
			/**
			 * 这种情况发生在主机名查询失败，或者由于发生一个异常，或者因为无法找到该地址对应的主机名
			 * 用 IP 的字符串形式表示
			 */
			this.hostName = this.fqdnHostName;
			LOG.warn("No hostname could be resolved for the IP address {}, using IP address as host name. "
					+ "Local input split assignment (such as for HDFS files) may be impacted.",
					this.inetAddress.getHostAddress());
		}
		else {
			this.hostName = NetUtils.getHostnameFromFQDN(this.fqdnHostName);
		}

		this.stringRepresentation = String.format(
				"%s @ %s (dataPort=%d)", resourceID, fqdnHostName, dataPort);
	}

	// ------------------------------------------------------------------------
	//  Getters
	// ------------------------------------------------------------------------

	/**
	 * Gets the ID of the resource in which the TaskManager is started. The format of this depends
	 * on how the TaskManager is started:
	 * <ul>
	 *     <li>If the TaskManager is started via YARN, this is the YARN container ID.</li>
	 *     <li>If the TaskManager is started via Mesos, this is the Mesos container ID.</li>
	 *     <li>If the TaskManager is started in standalone mode, or via a MiniCluster, this is a random ID.</li>
	 *     <li>Other deployment modes can set the resource ID in other ways.</li>
	 * </ul>
	 * 
	 * @return The ID of the resource in which the TaskManager is started
	 */
	public ResourceID getResourceID() {
		return resourceID;
	}

	/**
	 * Returns the port instance's task manager expects to receive transfer envelopes on.
	 * 
	 * @return the port instance's task manager expects to receive transfer envelopes on
	 */
	public int dataPort() {
		return dataPort;
	}

	/**
	 * Returns the network address the instance's task manager binds its sockets to.
	 * 
	 * @return the network address the instance's task manager binds its sockets to
	 */
	public InetAddress address() {
		return inetAddress;
	}

	/**
	 * Gets the IP address where the TaskManager operates.
	 *
	 * @return The IP address.
	 */
	public String addressString() {
		return inetAddress.toString();
	}

	/**
	 * Returns the fully-qualified domain name the TaskManager. If the name could not be
	 * determined, the return value will be a textual representation of the TaskManager's IP address.
	 * 
	 * @return The fully-qualified domain name of the TaskManager.
	 */
	public String getFQDNHostname() {
		return fqdnHostName;
	}

	/**
	 * Gets the hostname of the TaskManager. The hostname derives from the fully qualified
	 * domain name (FQDN, see {@link #getFQDNHostname()}):
	 * <ul>
	 *     <li>If the FQDN is the textual IP address, then the hostname is also the IP address</li>
	 *     <li>If the FQDN has only one segment (such as "localhost", or "host17"), then this is
	 *         used as the hostname.</li>
	 *     <li>If the FQDN has multiple segments (such as "worker3.subgroup.company.net"), then the first
	 *         segment (here "worker3") will be used as the hostname.</li>
	 * </ul>
	 *
	 * @return The hostname of the TaskManager.
	 */
	public String getHostname() {
		return hostName;
	}

	// --------------------------------------------------------------------------------------------
	// Utilities
	// 工具
	// --------------------------------------------------------------------------------------------

	@Override
	public String toString() {
		return stringRepresentation;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		else if (obj != null && obj.getClass() == TaskManagerLocation.class) {
			TaskManagerLocation that = (TaskManagerLocation) obj;
			return this.resourceID.equals(that.resourceID) &&
					this.inetAddress.equals(that.inetAddress) &&
					this.dataPort == that.dataPort;
		}
		else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return resourceID.hashCode() + 
				17 * inetAddress.hashCode() +
				129 * dataPort;
	}

	@Override
	public int compareTo(@Nonnull TaskManagerLocation o) {
		// decide based on resource ID first
		// 首先基于 resource ID 判断
		int resourceIdCmp = this.resourceID.getResourceIdString().compareTo(o.resourceID.getResourceIdString());
		if (resourceIdCmp != 0) {
			return resourceIdCmp;
		}

		// decide based on ip address next
		// 在基于 ip 地址判断
		byte[] thisAddress = this.inetAddress.getAddress();
		byte[] otherAddress = o.inetAddress.getAddress();

		if (thisAddress.length < otherAddress.length) {
			return -1;
		} else if (thisAddress.length > otherAddress.length) {
			return 1;
		} else {
			for (int i = 0; i < thisAddress.length; i++) {
				byte tb = thisAddress[i];
				byte ob = otherAddress[i];
				if (tb < ob) {
					return -1;
				} else if (tb > ob) {
					return 1;
				}
			}
		}

		// addresses are identical, decide based on ports.
		// 地址相同时，基于端口判断
		if (this.dataPort < o.dataPort) {
			return -1;
		} else if (this.dataPort > o.dataPort) {
			return 1;
		} else {
			return 0;
		}
	}
}
