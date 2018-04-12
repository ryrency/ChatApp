package message.server.resources;

import message.server.config.RoutingConf;
import message.server.raft.MessageBuilder;
import message.server.raft.RaftNode;
import routing.Pipe.Route;

public class NetworkDiscoveryResource implements RouteResource {

	@Override
	public String getPath() {
		return "/NetworkDiscovery";
	}

	@Override
	public Route process(Route msg){
		Route response = null;
		if(msg.hasNetworkDiscoveryPacket()) {
			
		response = MessageBuilder.buildNetworkDiscoveryResponse(msg,RaftNode.getInstance().getState().getNodeConf());
		}
		return response;
	}
}
