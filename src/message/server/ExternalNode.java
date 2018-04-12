package message.server;

import io.netty.channel.Channel;

public class ExternalNode {
	
	Channel channel;
	String host;
	int port;
	int NodeId;
	String groupTag;

	public ExternalNode(String groupTag, String host, int port) {	
		setGroupTag(groupTag);
		setHost(host);
		setPort(port);
	}

	public Channel getChannel() {
		return channel;
	}

	public void setChannel(Channel channel) {
		this.channel = channel;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public int getNodeId() {
		return NodeId;
	}

	public void setNodeId(int nodeId) {
		NodeId = nodeId;
	}

	public String getGroupTag() {
		return groupTag;
	}

	public void setGroupTag(String groupTag) {
		this.groupTag = groupTag;
	}
	
	public boolean isActive() {
		return channel != null && channel.isActive() && channel.isWritable();
	}
	
	

}
