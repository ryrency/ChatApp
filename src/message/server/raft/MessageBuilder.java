package message.server.raft;

import java.util.ArrayList;

import org.bson.Document;

import com.mongodb.client.FindIterable;
import com.sun.corba.se.impl.ior.NewObjectKeyTemplateBase;

import message.server.NodeMonitor;
import message.server.config.NodeConf;
import message.server.config.RoutingConf;
import message.utility.NetworkUtility;
import raft.proto.AppendEntries;
import raft.proto.AppendEntries.*;
import raft.proto.AppendEntries.AppendEntriesResponse.IsUpdated;
import raft.proto.HeartBeat.*;
import raft.proto.Vote.*;
import raft.proto.Vote.ResponseVote.IsVoteGranted;
import raft.proto.Work.WorkMessage;
import routing.Pipe;
import routing.Pipe.Message;
import routing.Pipe.NetworkDiscoveryPacket;
import routing.Pipe.Route;
import raft.proto.InternalNodeAdd.*;
import routing.Pipe.MessagesRequest;

public class MessageBuilder {

	
	public static WorkMessage prepareRequestVote() {
		WorkMessage.Builder work = WorkMessage.newBuilder();
		work.setUnixTimeStamp(TimerRoutine.getCurrentUnixTimeStamp());

		RequestVote.Builder requestVote = RequestVote.newBuilder();
	
		requestVote.setCandidateId(NodeMonitor.getInstance().getNodeConf().getNodeId());
		requestVote.setTerm(NodeState.currentTerm);
		System.out.println("MssageBuilder: Current term "+NodeState.currentTerm);

		requestVote.setTimeStampOnLatestUpdate(NodeState.getTimeStampOnLatestUpdate());
	
		VotePacket.Builder voteRPCPacket = VotePacket.newBuilder();
		voteRPCPacket.setUnixTimestamp(TimerRoutine.getCurrentUnixTimeStamp());
		voteRPCPacket.setRequestVote(requestVote);
		
		work.setVoteRPCPacket(voteRPCPacket);

		return work.build();
	}

	public static WorkMessage prepareAppendEntriesResponse(String response) {
		WorkMessage.Builder work = WorkMessage.newBuilder();
		work.setUnixTimeStamp(TimerRoutine.getCurrentUnixTimeStamp());

		AppendEntriesPacket.Builder appendEntriesPacket = AppendEntriesPacket.newBuilder();
		appendEntriesPacket.setUnixTimeStamp(TimerRoutine.getCurrentUnixTimeStamp());

		AppendEntriesResponse.Builder appendEntriesResponse = AppendEntriesResponse.newBuilder();
		if(response == "YES")
			appendEntriesResponse.setIsUpdated(IsUpdated.YES);
		else
			appendEntriesResponse.setIsUpdated(IsUpdated.NO);
		appendEntriesPacket.setAppendEntriesResponse(appendEntriesResponse);

		work.setAppendEntriesPacket(appendEntriesPacket);

		return work.build();

	}

	public static WorkMessage prepareHeartBeat() {
		WorkMessage.Builder work = WorkMessage.newBuilder();
		work.setUnixTimeStamp(TimerRoutine.getCurrentUnixTimeStamp());

		HeartBeatMsg.Builder heartbeat = HeartBeatMsg.newBuilder();
		heartbeat.setLeaderId(NodeMonitor.getInstance().getNodeConf().getNodeId());
		heartbeat.setTerm(NodeState.currentTerm);
		heartbeat.setTimeStampOnLatestUpdate(NodeState.getTimeStampOnLatestUpdate());

		HeartBeatPacket.Builder heartBeatPacket = HeartBeatPacket.newBuilder();
		heartBeatPacket.setUnixTimestamp(TimerRoutine.getCurrentUnixTimeStamp());
		heartBeatPacket.setHeartbeat(heartbeat);

		work.setHeartBeatPacket(heartBeatPacket);

		return work.build();
	}

	public static WorkMessage prepareResponseVote(IsVoteGranted decision) {
		WorkMessage.Builder work = WorkMessage.newBuilder();
		work.setUnixTimeStamp(TimerRoutine.getCurrentUnixTimeStamp());

		VotePacket.Builder voteRPCPacket = VotePacket.newBuilder();
		voteRPCPacket.setUnixTimestamp(TimerRoutine.getCurrentUnixTimeStamp());

		ResponseVote.Builder responseVoteRPC = ResponseVote.newBuilder();
		responseVoteRPC.setTerm(NodeState.currentTerm);
		responseVoteRPC.setIsVoteGranted(decision);

		voteRPCPacket.setResponseVote(responseVoteRPC);

		work.setVoteRPCPacket(voteRPCPacket);

		return work.build();
	}
	
	public static WorkMessage prepareInternalNodeAddRequest(int id, String host, int port) {
		WorkMessage.Builder work = WorkMessage.newBuilder();
		work.setUnixTimeStamp(TimerRoutine.getCurrentUnixTimeStamp());
		
		InternalNodeAddPacket.Builder internalNodeAddPacket = InternalNodeAddPacket.newBuilder();
		
		InternalNodeAddRequest.Builder internalNodeAddRequest = InternalNodeAddRequest.newBuilder();
		internalNodeAddRequest.setId(id);
		internalNodeAddRequest.setHost(host);
		internalNodeAddRequest.setPort(port);
		
		internalNodeAddPacket.setInternalNodeAddRequest(internalNodeAddRequest);
		work.setInternalNodeAddPacket(internalNodeAddPacket);
		return work.build();
		
	}
	
	public static WorkMessage prepareAppendEntriesPacket(Route clientMsg, String timestamp) {

        WorkMessage.Builder work = WorkMessage.newBuilder();
        work.setUnixTimeStamp(TimerRoutine.getCurrentUnixTimeStamp());

        AppendEntriesPacket.Builder appendEntriesPacket = AppendEntriesPacket.newBuilder();
        appendEntriesPacket.setUnixTimeStamp(TimerRoutine.getCurrentUnixTimeStamp());
        AppendEntries.ClientMessage.Builder clientMsgBuild = AppendEntries.ClientMessage.newBuilder();


        AppendEntriesMsg.Builder appendEntries = AppendEntriesMsg.newBuilder();
        appendEntries.setTimeStampOnLatestUpdate(timestamp);
        appendEntries.setLeaderId(NodeMonitor.getInstance().getNodeConf().getNodeId());
        appendEntries.setTermid(NodeState.getInstance().currentTerm);
        

        clientMsgBuild.setSender(clientMsg.getMessage().getSenderId());
        clientMsgBuild.setPayload(clientMsg.getMessage().getPayload());
        clientMsgBuild.setTo(clientMsg.getMessage().getReceiverId());
        clientMsgBuild.setTimestamp(timestamp);
        
        clientMsgBuild.setType(clientMsg.getMessage().getType().getNumber());
        clientMsgBuild.setStatus(clientMsg.getMessage().getStatus().getNumber());
        appendEntries.setMessage(clientMsgBuild);
        
        appendEntriesPacket.setAppendEntries(appendEntries);
        
        work.setAppendEntriesPacket(appendEntriesPacket);

        return work.build();

    }
	public static Route prepareMessage(String Error) {
		Route.Builder routeMsg = Route.newBuilder();
		routeMsg.setId(1); // NEED TO CHECK WHAT ID TO PASS
		routeMsg.setPath(routeMsg.getPath().MESSAGES_RESPONSE);
		
		Pipe.Response.Builder response = Pipe.Response.newBuilder();
		response.setSuccess(false);
		response.setMessage(Error);
		return routeMsg.build();
	}
	
	public static Route prepareMessageResponse(MessagesRequest.Type type ,FindIterable<Document> dbresult) {
		Route.Builder routeMsg = Route.newBuilder();
		routeMsg.setId(1); // NEED TO CHECK WHAT ID TO PASS
		routeMsg.setPath(routeMsg.getPath().MESSAGES_RESPONSE);

		Pipe.MessagesResponse.Builder msgResponse = Pipe.MessagesResponse.newBuilder();
		if(type == MessagesRequest.Type.GROUP) {
			msgResponse.setType(Pipe.MessagesResponse.Type.GROUP);
		}
		else {
			msgResponse.setType(Pipe.MessagesResponse.Type.USER);
		}

		ArrayList<Message> messageList = new ArrayList<Message>();

		Pipe.Message.Builder msg = Pipe.Message.newBuilder();
		for (Document documentRow : dbresult) {
			documentRow.getString("receiverID");
			msgResponse.setId(documentRow.getString("senderId"));

			msg.setAction(msg.getAction().POST);
			msg.setType(msg.getType().SINGLE);
			msg.setStatus(msg.getStatus().ACTIVE);
			msg.setSenderId(documentRow.getString("senderId"));
			msg.setPayload(documentRow.getString("payload"));
			msg.setReceiverId(documentRow.getString("receiverID"));
			msg.setTimestamp(documentRow.getString("timestamp"));
			messageList.add(msg.build());
		}
		msgResponse.addAllMessages(messageList);
		routeMsg.setMessagesResponse(msgResponse);
		routeMsg.setMessage(msg);
		return routeMsg.build();
	}

	public static Route buildNetworkDiscoveryResponse(Route msg,NodeConf nodeConf) {
		Route.Builder rb = Route.newBuilder();
		try {
		NetworkDiscoveryPacket.Builder ndpb = NetworkDiscoveryPacket.newBuilder();
        ndpb.setMode(NetworkDiscoveryPacket.Mode.RESPONSE);
        ndpb.setSender(msg.getNetworkDiscoveryPacket().getSender());
        ndpb.setGroupTag(nodeConf.getGroupTag());
        //ndpb.setGroupTag("weCAN");
        ndpb.setNodeAddress(NetworkUtility.getLocalHostAddress());
        ndpb.setNodePort(nodeConf.getClientPort());
        //ndpb.setNodePort(8887);
        ndpb.setSecret(nodeConf.getSecret());
        //ndpb.setSecret("secret");

        
//        rb.setId(nextId());
        rb.setId(NodeMonitor.getInstance().getNodeConf().getNodeId());
        rb.setPath(Route.Path.NETWORK_DISCOVERY);
        rb.setNetworkDiscoveryPacket(ndpb);
        
	}
	
	catch(Exception e) {
		e.printStackTrace();
	}
		return rb.build();
	}
	

}