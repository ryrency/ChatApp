package message.server.config;


/**
 * Copyright 2012 Gash.
 *
 * This file and intellectual content is protected under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Routing information for the server - internal use only
 * 
 * @author gash
 * 
 */
@XmlRootElement(name = "conf")
@XmlAccessorType(XmlAccessType.FIELD)
public class NodeConf {
	private int nodeId;
	private String host;
	private boolean internalNode = true;
	private int heartbeatDt = 2000;
	
	private int internalPort;
	private int clientPort;
	private int networkDiscoveryPort;
	private int mongoPort;
	
	private String groupTag;
	private String secret;
	
	private List<Integer> monitorConnections;

	public int getNodeId() {
		return nodeId;
	}

	public void setNodeId(int nodeId) {
		this.nodeId = nodeId;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public boolean isInternalNode() {
		return internalNode;
	}

	public void setInternalNode(boolean internalNode) {
		this.internalNode = internalNode;
	}

	public int getHeartbeatDt() {
		return heartbeatDt;
	}

	public void setHeartbeatDt(int heartbeatDt) {
		this.heartbeatDt = heartbeatDt;
	}

	public int getInternalPort() {
		return internalPort;
	}

	public void setInternalPort(int internalPort) {
		this.internalPort = internalPort;
	}

	public int getClientPort() {
		return clientPort;
	}

	public void setClientPort(int clientPort) {
		this.clientPort = clientPort;
	}

	public int getNetworkDiscoveryPort() {
		return networkDiscoveryPort;
	}

	public void setNetworkDiscoveryPort(int networkDiscoveryPort) {
		this.networkDiscoveryPort = networkDiscoveryPort;
	}

	public int getMongoPort() {
		return mongoPort;
	}

	public void setMongoPort(int mongoPort) {
		this.mongoPort = mongoPort;
	}

	public List<Integer> getMonitorConnections() {
		return monitorConnections;
	}

	public void setMonitorConnections(List<Integer> monitorConnections) {
		this.monitorConnections = monitorConnections;
	}

	public String getGroupTag() {
		return groupTag;
	}

	public void setGroupTag(String groupTag) {
		this.groupTag = groupTag;
	}

	public String getSecret() {
		return secret;
	}

	public void setSecret(String secret) {
		this.secret = secret;
	}
	
	public String getInternalSocketServerAddress() {
		return host + ":" + internalPort;
	}
	
	public String getClientSocketServerAddress() {
		return host + ":" + clientPort;
	}
	
	public String getDiscoverySocketServerAddress() {
		return host + ":" + networkDiscoveryPort;
	}
	
	public String getMongoServerAddress() {
		return host + ":" + mongoPort;
	}
}
