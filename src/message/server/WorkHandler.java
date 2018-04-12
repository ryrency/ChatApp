package message.server;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import message.server.raft.RaftNode;

import java.util.logging.Logger;

import raft.proto.Internal.InternalPacket;

public class WorkHandler extends SimpleChannelInboundHandler<InternalPacket> {

	WorkHandler() {
		
	}

	public void handleMessage(InternalPacket packet, Channel channel) {
		
		if (packet.hasAppendEntriesRequest()) {
			RaftNode.getInstance().handleAppendEntriesRequest(packet.getAppendEntriesRequest());
			
		} else if (packet.hasAppendEntriesResponse()) {
			RaftNode.getInstance().handleAppendEntriesResponse(packet.getAppendEntriesResponse());
			
		} else if (packet.hasVoteRequest()) {
			RaftNode.getInstance().handleVoteRequest(packet.getVoteRequest());
			
		} else if (packet.hasVoteResponse()) {
			RaftNode.getInstance().handleVoteResponse(packet.getVoteResponse());
			
		} else if (packet.hasConnectionActiveAck()) {
			int nodeId = packet.getConnectionActiveAck().getNodeId();
			Logger.getGlobal().info("received connection alive ack from node: " + nodeId);
			
			NodeMonitor
			.getInstance()
			.getNodeMap()
			.get(nodeId)
			.setChannel(channel);
		} else if (packet.hasForwardMessageRequest()) {
			if (packet.getForwardMessageRequest().hasMessage()) {
				RaftNode.getInstance().addMessage(packet.getForwardMessageRequest().getMessage());

			} else if (packet.getForwardMessageRequest().hasUser()) {
				RaftNode.getInstance().addUser(packet.getForwardMessageRequest().getUser());

			} else if (packet.getForwardMessageRequest().hasMessageReadPayload()) {
				String uname = packet.getForwardMessageRequest().getMessageReadPayload().getUname();

				RaftNode.getInstance().markMessagesRead(uname);
			}
		}
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, InternalPacket packet) throws Exception {
		handleMessage(packet, ctx.channel());

	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		
		System.out.println("Unexpected exception");
		System.out.println(cause.getMessage());

		ctx.close();
	}

}
