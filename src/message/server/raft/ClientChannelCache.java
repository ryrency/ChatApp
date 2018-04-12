package message.server.raft;

import java.util.HashMap;
import java.util.logging.Logger;

import io.netty.channel.Channel;

public class ClientChannelCache {


	public static HashMap<String,ClientChannel> clientChannelCacheMap = new HashMap<String, ClientChannel>();
	private static ClientChannelCache clientChannelCache;
	
	public static ClientChannelCache getInstance() {
		if (clientChannelCache == null) clientChannelCache = new ClientChannelCache();
		return clientChannelCache;
	}
	
	public ClientChannelCache() {
		// TODO Auto-generated constructor stub
	}
	
	public void addClientChannelToMap(String clientName, Channel clientChannel) {
		Logger.getGlobal().info("Adding " + clientName + "client channel in cache");
		if (!clientChannelCacheMap.containsKey(clientName) || !clientChannelCacheMap.get(clientName).isActive()) {
			clientChannelCacheMap.put(clientName, new ClientChannel(clientName, clientChannel));
		}
	}
	
	public void deleteClientChannelFromMap(String clientName) {
		if (clientChannelCacheMap.containsKey(clientName)) {
			clientChannelCacheMap.remove(clientName);
		}
	}
	public HashMap<String,ClientChannel> getClientChannelMap() {
		return clientChannelCacheMap;
	}

	public class ClientChannel{
		String clientName;
		Channel clientChannel;
		
		public ClientChannel(String pClientName,Channel pClientChannel) {
			// TODO Auto-generated constructor stub
			clientName = pClientName;
			clientChannel = pClientChannel;
		}

		public boolean isActive() {
			return clientChannel != null && clientChannel.isActive() && clientChannel.isWritable();
		}
		public String getClientName() {
			return clientName;
		}
		public void setClientName(String pClientName) {
			clientName = pClientName;
		}
		public void setClientChannel(Channel pClientChannel) {
			clientChannel = pClientChannel;
		}
		public Channel getClienChannel() {
			return clientChannel;
		}
	}
}
