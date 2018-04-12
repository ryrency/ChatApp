package message.server;

import io.netty.channel.Channel;
import message.server.config.NodeConf;
import message.server.config.RoutingConf.RoutingEntry;

public class RemoteNode {
	
	private NodeConf nodeConf;

	
	//state variables
	Channel channel;
	 
	 
	 RemoteNode(NodeConf conf){
		 nodeConf = conf;
	 }
 
	public boolean isActive() {
		return channel != null && channel.isActive() && channel.isWritable();
	}
	
	public Channel getChannel() {
		return channel;
	}
	public void setChannel(Channel channel) {
		this.channel = channel;
	}
	
	public NodeConf getNodeConf() {
		return nodeConf;
	}
}
