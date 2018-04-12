package message.server.raft;

import io.netty.channel.Channel;
import message.database.MessageMongoDB;
import message.database.NodeStateMongoDB;
import message.database.UserMongoDB;
import message.router.discovery.DiscoveryServer;
import message.server.NodeMonitor;
import message.server.RemoteNode;
import message.server.config.NodeConf;
import message.server.config.RoutingConf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import raft.proto.Internal;
import raft.proto.Internal.AppendEntriesRequest;
import raft.proto.Internal.AppendEntriesResponse;
import raft.proto.Internal.InternalPacket;
import raft.proto.Internal.LogEntry;
import raft.proto.Internal.MessagePayLoad;
import raft.proto.Internal.MessageReadPayload;
import raft.proto.Internal.UserPayload;
import raft.proto.Internal.VoteRequest;
import raft.proto.Internal.VoteResponse;
import routing.Pipe;
import routing.Pipe.Message;
import routing.Pipe.Message.Status;
import routing.Pipe.User;

import com.google.protobuf.InvalidProtocolBufferException;


//todo: all nodes must be present in remote nodes so that we know how many votes to expect
//todo: whenever a request/response is received, do the term check and become follower if needed
//todo: update last log entry whenever a new log is added to the db
//todo: should monitor remove channels which have already been removed?
//todo: should we count votes only for active nodes? or all the nodes even if they are down?
//todo: add mongodb ports into node conf

public class RaftNode {
	
	private static final int DEFAULT_LOG_TERM = 1;
	private static final int DEFAULT_LOG_INDEX = 1;
	
	private static RaftNode instance;
	
	public static RaftNode getInstance() {
		if (instance == null) instance = new RaftNode();
		return instance;
	}
	
	private enum NodeType {
		Follower,
		Candidate,
		Leader
	}
	
	private NodeType nodeType;
	private State state;
	private LogEntry lastLogEntry = null;
	private ScheduledExecutor executor = new ScheduledExecutor(3);
	private Map<Integer, Runnable> appendEntriesTask = new ConcurrentHashMap<Integer, Runnable>();
	private Map<Integer, ScheduledFuture<Void>> appendEntriesFutures = new ConcurrentHashMap<Integer, ScheduledFuture<Void>>();
	
	private ScheduledFuture<Void> followerTaskFuture = null;
	private ScheduledFuture<Void> candidateTaskFuture = null;

	private Thread discoveryThread; 
	
	public NodeType getNodeType() {
		return nodeType;
	}
	
	private RaftNode() {
		
	}
	
	public synchronized void init(RoutingConf conf, NodeConf nodeConf) {
		state = new State(conf, nodeConf);
		restoreState();
		setNodeType(NodeType.Follower);
		scheduleLogCommitTask();
	}
	
	public State getState() {
		return state;
	}
	
	/********************************************************************************/
	/* Switch roles */
	/********************************************************************************/
	private synchronized void setNodeType(NodeType type) {
		if (type != nodeType) {
			nodeType = type;
			if (type == NodeType.Follower) {
				stopCandidate();
				stopLeader();
				startFollower();
			} else if (type == NodeType.Candidate) {
				stopFollower();
				stopLeader();
				startCandidate();
			} else {
				stopFollower();
				stopCandidate();
				startLeader();
			}
		}
	}
	
	private void startFollower() {
		Logger.getGlobal().info("Starting as follower");
		rescheduleFollowerTask();
	}
	
	@SuppressWarnings("unchecked")
	private void rescheduleFollowerTask() {
		if (followerTaskFuture != null) {
			
			followerTaskFuture.cancel(true);
		}
		
		followerTaskFuture = (ScheduledFuture<Void>) executor.schedule(
				followerTask, 
				TimerRoutine.getFollowerTimeOut(), 
				TimeUnit.MILLISECONDS
		);
	}
	
	@SuppressWarnings("unchecked")
	private void rescheduleAppendEntriesTask(int nodeId, long delay) {
		ScheduledFuture<Void> future = null;
		if (appendEntriesFutures.containsKey(nodeId)) future = appendEntriesFutures.get(nodeId);
		if (future != null) future.cancel(true);
		
		future = (ScheduledFuture<Void>) executor.schedule(
				appendEntriesTask.get(nodeId), 
				delay, 
				TimeUnit.MILLISECONDS
		);
		
		appendEntriesFutures.put(nodeId, future);
	}
	
	private void stopFollower() {
		executor.remove(followerTask);
	}
	
	private void startCandidate() {
		Logger.getGlobal().info("Starting as candidate");
		executor.submit(new Runnable() {
			
			@Override
			public void run() {
				// TODO Auto-generated method stub
				requestVotes();
			}
		});
	}
	
	private void stopCandidate() {
		if (candidateTaskFuture != null) {
			candidateTaskFuture.cancel(true);
		}
		
		executor.remove(candidateTask);
	}
	
	private void startLeader() {
		Logger.getGlobal().info("Starting as leader");
		
		//reset state variables
		state.resetNextIndices();
		state.resetmatchIndices();
		state.resetLogIndexTermMap();
		state.setCurrentLeader(state.getNodeConf().getNodeId());
		
		sendAppendEntriesRequests();
		
		//todo: start discovery servers
		startDiscoveryServer();
	}
	
	private void stopLeader() {
		cancelAppendEntriesRequests();
		//todo: stop discovery servers
		stopDiscoveryServer();
	}
	
	private void scheduleLogCommitTask() {
		executor.schedule(logCommitTask, TimerRoutine.getLogCommitInterval(), TimeUnit.MILLISECONDS);
	}
 	
	/********************************************************************************/
	/* Saving and restoring states */
	/********************************************************************************/
	private void saveState() {
		NodeStateMongoDB.getInstance().saveNodeState(state);
	}
	
	private void restoreState() {
		NodeStateMongoDB.getInstance().restoreNodeState(state);
	}
	
	/********************************************************************************/
	/* Ask for votes Requests */
	/********************************************************************************/
	@SuppressWarnings("unchecked")
	private void requestVotes() {
		Logger.getGlobal().info("starting to request votes");
		// start election, vote to self
		voteToSelf();
		
		// request votes from all servers
		InternalPacket packet = prepareVoteRequest();
		broadcastVoteRequestPacket(packet);
		
		// check if it has already won the election
		if (isWinner()) {
			setNodeType(NodeType.Leader);
		} else {
			candidateTaskFuture = (ScheduledFuture<Void>) executor.schedule(
					candidateTask, 
					TimerRoutine.getCandidateElectionTimeout(), 
					TimeUnit.MILLISECONDS
			);
		}
	}
	
	private void voteToSelf() {
		state.setVotesExpected(1);
		state.setVotesReceived(1);
		state.setYesVotes(1);
		state.incrementCurrentTerm();
		state.setLastVotedTerm(state.getCurrentTerm());
		saveState();
	}
	
	private LogEntry getLastLogEntry() {
		if (lastLogEntry == null) initLastLogEntry();
		return lastLogEntry;
	}
	
	
	// this method is synchronized
	private synchronized void initLastLogEntry() {
		lastLogEntry = NodeStateMongoDB.getInstance().getLastLogEntry();
	}
	
	private synchronized void setLastLogEntry(LogEntry logEntry) {
		lastLogEntry = logEntry;
	}
	
	private InternalPacket prepareVoteRequest() {
		LogEntry entry = getLastLogEntry();
		int lastLogTerm = entry != null ? entry.getTerm() : DEFAULT_LOG_TERM;
		int lastLogIndex = entry != null ? entry.getIndex() : DEFAULT_LOG_INDEX;
				
		//prepare vote request
		VoteRequest request = 
				VoteRequest
				.newBuilder()
				.setTerm(state.getCurrentTerm())
				.setCandidateId(state.getNodeConf().getNodeId())
				.setLastLogTerm(lastLogTerm)
				.setLastLogIndex(lastLogIndex)
				.build();
				
		InternalPacket packet = 
				InternalPacket
				.newBuilder()
				.setVoteRequest(request)
				.build();
				
		
		return packet;
	}
	
	private void broadcastVoteRequestPacket(InternalPacket packet) {
		
		for (RemoteNode rm : NodeMonitor.getInstance().getNodeMap().values()) {
			state.incrementVotesExpected();
			
			if (rm.isActive() && rm.getChannel() != null) {
				Logger.getGlobal().info("Sent VoteRequestRPC to " + rm.getNodeConf().getNodeId() + "request: " + packet.toString());
				rm.getChannel().writeAndFlush(packet);
			} else {
				Logger.getGlobal().info("Channel not active ,server  is down");
			}
		}
	}
	
	/********************************************************************************/
	/* Respond to vote requests */
	/********************************************************************************/
	public synchronized void handleVoteRequest(VoteRequest request) {
		
		executor.submit(new Runnable() {
			
			@Override
			public void run() {
				// TODO Auto-generated method stub
				Logger.getGlobal().info("vote request received from: " + request.getCandidateId() + ", request: " + request.toString());
				
				if (request.getTerm() > state.getCurrentTerm()) {
					state.setCurrentTerm(request.getTerm());
					saveState();
					setNodeType(NodeType.Follower);
				}
				
				if (nodeType == NodeType.Follower) {
					rescheduleFollowerTask();
				}
				
				boolean isVoteGranted = isVoteGranted(request);
				if (isVoteGranted) Logger.getGlobal().info("granted vote to nodeId: " + request.getCandidateId());
				
				VoteResponse response = 
						VoteResponse
						.newBuilder()
						.setVoteGranted(isVoteGranted)
						.setTerm(state.getCurrentTerm())
						.build();
				
				InternalPacket packet = 
						InternalPacket
						.newBuilder()
						.setVoteResponse(response)
						.build();
				
				sendVoteResponse(packet, request.getCandidateId());
			}
		});
	}
	
	private boolean isVoteGranted(VoteRequest request) {
		int candidateTerm = request.getTerm(); 
		int candidateLastLogIndex = request.getLastLogIndex();
		
		LogEntry entry = getLastLogEntry();
		int lastLogIndex = entry != null ? entry.getIndex() : DEFAULT_LOG_INDEX;
		
		boolean granted = 
				nodeType == NodeType.Follower && 
				candidateTerm >= state.getCurrentTerm() && 
				candidateTerm > state.getLastVotedTerm() && 
				candidateLastLogIndex >= lastLogIndex;
				
		Logger.getGlobal().info("vote reasons: type: " + nodeType + ", currTerm: " + state.getCurrentTerm() + ", lastLogIndex: " + lastLogIndex + ", lastVotedTerm: " + state.getLastVotedTerm());
				
		if (granted) {
			state.setLastVotedTerm(candidateTerm);
			saveState();
		}
		return granted;
	}
	
	private void sendVoteResponse(InternalPacket packet, int nodeId) {
		Logger.getGlobal().info("sending vote response to node id: " + nodeId);
		
		NodeMonitor
		.getInstance()
		.getNodeMap()
		.get(nodeId)
		.getChannel()
		.writeAndFlush(packet);
	}
	
	/********************************************************************************/
	/* Handling Voting Responses */
	/********************************************************************************/
	public void handleVoteResponse(VoteResponse response) {
		executor.submit(new Runnable() {
			
			@Override
			public void run() {
				// TODO Auto-generated method stub
				Logger.getLogger(RaftNode.class.getSimpleName()).info("vote received: " + response.toString());
				
				if (nodeType != NodeType.Candidate) return;
				
				if (response.getTerm() > state.getCurrentTerm()) {
					state.setCurrentTerm(response.getTerm());
					saveState();
					setNodeType(NodeType.Follower);
				} else {
					state.incrementVotesReceived();
					if (response.getVoteGranted()) state.incrementYesVotes();
					
					//todo: also check election result after the voting timeout
					if (state.getVotesExpected() == state.getVotesReceived()) checkElectionResult();
				}
			}
		});
	}
	
	private synchronized void checkElectionResult() {
		Logger.getGlobal().info("checking election results");
		if (isWinner()) {
			Logger.getGlobal().info(state.getNodeConf().getNodeId() + " has won the election.");
			setNodeType(NodeType.Leader);
		} else {
			Logger.getGlobal().info(state.getNodeConf().getNodeId() + " has lost the election.");
			setNodeType(NodeType.Follower);
		}
	}
	
	private boolean isWinner() {
		Logger.getGlobal().info("Total number of votes = " + state.getVotesReceived());
		Logger.getGlobal().info("Expected number of votes = " + state.getVotesExpected());
		Logger.getGlobal().info("Total number of YES votes = " + state.getYesVotes());

		if (state.getYesVotes() > state.getVotesExpected()/2) {
			return true;
		}
		
		return false;
	}
	
	/********************************************************************************/
	/* Leader functions: append entries, send heart beats */
	/********************************************************************************/
	@SuppressWarnings("unchecked")
	private void sendAppendEntriesRequests() {
		Logger.getGlobal().info("scheduling append entries requests for all nodes");
		
		//send append entries to all servers, in parallel on thread pool
		for (RemoteNode node: NodeMonitor.getInstance().getNodeMap().values()) {
			final int nodeId = node.getNodeConf().getNodeId();
			Runnable task = null;
			ScheduledFuture<Void> future = null;
			if (appendEntriesTask.containsKey(nodeId)) task = appendEntriesTask.get(nodeId);
			if (appendEntriesFutures.containsKey(nodeId)) future = appendEntriesFutures.get(nodeId);
			
			if (future != null) future.cancel(true);
			
			if (task == null) {
				task = getSendAppendEntriesTask(nodeId);
				appendEntriesTask.put(nodeId, task);
			}
			
			future = (ScheduledFuture<Void>) executor.submit(task);
			appendEntriesFutures.put(nodeId, future);
		}
	}
	
	private void cancelAppendEntriesRequests() {
		Logger.getGlobal().info("cancelling append entries requests for all nodes");
		
		for (ScheduledFuture<Void> future : appendEntriesFutures.values()) {
			if (future != null) future.cancel(true);
		}
	}
	
	private InternalPacket prepareAppendEntriesRequest(int nodeId) {
		int fromIndex = state.getNextIndices().get(nodeId) - 1;
		List<LogEntry> entries = NodeStateMongoDB.getInstance().getLogEntriesFromIndex(fromIndex);
		List<LogEntry> newEntries = new ArrayList<LogEntry>();
		
		int prevLogIndex = 0;
		int prevLogTerm = 0;
		
		if (fromIndex > 0) {
			if (entries.size() > 0) {
				prevLogIndex = entries.get(0).getIndex();
				prevLogTerm = entries.get(0).getTerm();
				newEntries = entries.subList(1, entries.size());
			}
		} else {
			newEntries = entries;
		}
		
		int currentNodeId = state.getNodeConf().getNodeId();
		int currentTerm = state.getCurrentTerm();
		
		AppendEntriesRequest request = 
				AppendEntriesRequest
				.newBuilder()
				.setLeaderId(currentNodeId)
				.setTerm(currentTerm)
				.setPrevLogIndex(prevLogIndex)
				.setPrevLogTerm(prevLogTerm)
				.setLeaderCommit(state.getCommitIndex())
				.addAllEntries(newEntries)
				.build();
		
		InternalPacket packet = 
				InternalPacket
				.newBuilder()
				.setAppendEntriesRequest(request)
				.build();
		
		Logger.getGlobal().info("request packet prepared for node: " + nodeId);
		return packet;
		
	}
	
	public synchronized void handleAppendEntriesRequest(AppendEntriesRequest request) {
		executor.submit(new Runnable() {
			
			@Override
			public void run() {
				// TODO Auto-generated method stub
				Logger.getGlobal().info("received append entry request: " + request.toString());
				
				if (request.getTerm() > state.getCurrentTerm()) {
					state.setCurrentTerm(request.getTerm());
					setNodeType(NodeType.Follower);
					saveState();
				}
				
				//1. in raft paper
				if (request.getTerm() < state.getCurrentTerm()) {
					sendAppendEntriesResponse(false, request.getLeaderId());
					return;
				}
				
				state.setCurrentLeader(request.getLeaderId());
				rescheduleFollowerTask(); // since we just received a request from leader
				
				//2, 3. in raft paper
				boolean success = true;
				if (request.getPrevLogIndex() != 0) {
					LogEntry existingLogEntry = NodeStateMongoDB.getInstance().getLogEntry(request.getPrevLogIndex());
					if (existingLogEntry != null) {
						success = existingLogEntry.getTerm() == request.getPrevLogTerm();
						if (!success) {
							NodeStateMongoDB.getInstance().deleteLogEntries(existingLogEntry.getIndex());
							sendAppendEntriesResponse(success, request.getLeaderId());
							return;
						}
					} else {
						success = false;
						sendAppendEntriesResponse(success, request.getLeaderId());
						return;
					}
				}
				
				//4. in raft paper
				if (request.getEntriesCount() > 0) {
					NodeStateMongoDB.getInstance().commitLogEntries(request.getEntriesList());
					setLastLogEntry(request.getEntriesList().get(request.getEntriesCount() - 1));
				}
				
				//5. in raft paper
				int lastEntryCommitIndex = 
						request.getEntriesCount() > 0 ? 
						request.getEntriesList().get(0).getIndex() : request.getLeaderCommit();
						
				int newCommitIndex = Math.min(request.getLeaderCommit(), lastEntryCommitIndex);
				Logger.getGlobal().info("updating follower's commit index to: " + newCommitIndex);
				state.setCommitIndex(newCommitIndex);
				
				sendAppendEntriesResponse(true, request.getLeaderId());
			}
		});
	}
	
	private void sendAppendEntriesResponse(final boolean success, final int nodeId) {
		executor.submit(new Runnable() {
			
			@Override
			public void run() {
				// TODO Auto-generated method stub
				LogEntry entry = getLastLogEntry();
				int matchIndex = entry != null ? entry.getIndex() : 0;
				
				AppendEntriesResponse response = 
						AppendEntriesResponse
						.newBuilder()
						.setSuccess(success)
						.setTerm(state.getCurrentTerm())
						.setNodeId(state.getNodeConf().getNodeId())
						.setMatchIndex(matchIndex)
						.build();
				
				InternalPacket packet = 
						InternalPacket
						.newBuilder()
						.setAppendEntriesResponse(response)
						.build();
				
				//Logger.getGlobal().info("sending append entries response: " + response.toString());
				
				Channel channel = NodeMonitor.getInstance().getNodeMap().get(nodeId).getChannel();
				channel.writeAndFlush(packet);
			}
		});
	}
	
	public void handleAppendEntriesResponse(AppendEntriesResponse response) {
		//Logger.getGlobal().info("received append entry response: " + response.toString());
		
		int matchIndex = response.getMatchIndex();
		int nodeId = response.getNodeId();
		
		if (response.getSuccess()) {
			state.getNextIndices().put(nodeId, matchIndex + 1);
			state.getMatchIndices().put(nodeId, matchIndex);
			
			rescheduleAppendEntriesTask(nodeId, TimerRoutine.getHeartbeatSendDelay());
			
		} else {
			if (response.getTerm() > state.getCurrentTerm()) {
				state.setCurrentTerm(response.getTerm());
				setNodeType(NodeType.Follower);
				saveState();
			} else {
				int oldNextIndex = state.getNextIndices().get(nodeId);
				state.getNextIndices().put(nodeId, oldNextIndex - 1);
				rescheduleAppendEntriesTask(nodeId, 0);
			}
		}
	}
	
	/********************************************************************************/
	/* Leader functions: start and stop discovery */
	/********************************************************************************/
	private void startDiscoveryServer() {
		
		DiscoveryServer udpDiscoveryServer = new DiscoveryServer(state.getConf(), state.getNodeConf());
		if (discoveryThread == null) {
			Logger.getGlobal().info("UDP discovery server start");
			discoveryThread = new Thread(udpDiscoveryServer);
			discoveryThread.start();
		}else {
			Logger.getGlobal().info("UDP discovery server is already started, No action required");
		}
	}
	
	private void stopDiscoveryServer() {
		//todo implement this
		Logger.getGlobal().info("UDP discovery server stop");
		if (discoveryThread != null) {
			discoveryThread.interrupt();
		}

	}
	
	/********************************************************************************/
	/* Background tasks for follower, candidate, and leader */
	/********************************************************************************/
	private Runnable followerTask = new Runnable() {
		
		@Override
		public void run() {
			// TODO Auto-generated method stub
			//no heart beat received, become candidate
			if (followerTaskFuture.isCancelled()) return;
			Logger.getGlobal().info("follower heartbeat timed out");
			setNodeType(NodeType.Candidate);
		}
	};
	
	private Runnable candidateTask = new Runnable() {
		
		@Override
		public void run() {
			// TODO Auto-generated method stub
			if(candidateTaskFuture.isCancelled()) return;
			
			if (nodeType == NodeType.Candidate) 
				checkElectionResult();
			
		}
	};
	
	private Runnable logCommitTask = new Runnable() {
		
		@Override
		public void run() {
			// TODO Auto-generated method stub
			
			// 1. update commit indices for the leader
			
			if (nodeType == NodeType.Leader) {
				
//				generateDummyUsers();
				
				List<Integer> matchIndices = new ArrayList<Integer>(state.getMatchIndices().values());
				Collections.sort(matchIndices);
				int N = matchIndices.get(matchIndices.size()/2);
				
				Logger.getGlobal().info("match indices: " + matchIndices.toString());
				Logger.getGlobal().info("new value of N: " + N);
				
				state.setCommitIndex(N);
			}
			
			//2. apply logs to new commit index
			if (state.getLastApplied() < state.getCommitIndex()) {
				Logger.getGlobal().info("going to apply logs");
				// apply logs here
				List<LogEntry> entries = 
						NodeStateMongoDB
						.getInstance()
						.getLogEntriesBetween(state.getLastApplied(), state.getCommitIndex());
				
				if (entries.size() > 0) {
					applyLogs(entries);
					state.setLastApplied(entries.get(entries.size() - 1).getIndex());
					saveState();
					Logger.getGlobal().info("logs applied successfully, size: " + entries.size());
				} else {
					Logger.getGlobal().info("no logs to apply");
				}
			}
			
			// reschedule task
			scheduleLogCommitTask();
		}
	};
	
	private Runnable getSendAppendEntriesTask(final int nodeId) {
		Runnable task = new Runnable() {
			
			@Override
			public void run() {
				
				// TODO Auto-generated method stub
				Logger.getGlobal().info("going to send append entries to node: " + nodeId);
				if (appendEntriesFutures.get(nodeId).isCancelled()) {
					Logger.getGlobal().info("append entries future has been cancelled for node: " + nodeId);
					return;
				}
				
				RemoteNode remoteNode = NodeMonitor.getInstance().getNodeMap().get(nodeId);
				
				if (remoteNode.getChannel().isActive()) {
					InternalPacket packet = prepareAppendEntriesRequest(nodeId);
					remoteNode.getChannel().writeAndFlush(packet);
					Logger.getGlobal().info("sent append entries request to nodeId: " + nodeId + "request: " + packet);
				} else {
					Logger.getGlobal().info("channel not active, cannot send append entries to nodeId: " + nodeId);
				}
				
				rescheduleAppendEntriesTask(nodeId, TimerRoutine.getHeartbeatSendDelay());
			}
		};
		
		return task;
	}

	/********************************************************************************/
	/* send unread messages to client */
	/********************************************************************************/
	public void pushUnreadMessagesToClient(String uname) {
		executor.submit(new Runnable() {
			@Override
			public void run() {
				List<Message> messages = MessageMongoDB.getInstance().getUnreadMessages(uname);
				Channel channel = null;
				if (ClientChannelCache.getInstance().getClientChannelMap().containsKey(uname)) {
					channel = ClientChannelCache.getInstance().getClientChannelMap().get(uname).getClienChannel();
				}

				if (messages.size() > 0 && channel != null && channel.isActive()) {
					Pipe.MessagesResponse response
							= Pipe
							.MessagesResponse
							.newBuilder()
							.addAllMessages(messages)
							.setType(Pipe.MessagesResponse.Type.USER)
							.build();

					Pipe.Route route
							= Pipe
							.Route
							.newBuilder()
							.setId(0)
							.setPath(Pipe.Route.Path.MESSAGES_RESPONSE)
							.setMessagesResponse(response)
							.build();

					channel.writeAndFlush(route);

					//also mark messages read
					markMessagesRead(uname);
				}
			}
		});
	}
	
	/********************************************************************************/
	/* public apis to commit messages, users, and mark messages as read */
	/********************************************************************************/
	public void addUser(User user) {
		if (nodeType == NodeType.Leader) {
			Logger.getGlobal().info("going to write user entry to logs");
			UserPayload payload =
					UserPayload
							.newBuilder()
							.setPayload(user.toByteString())
							.build();

			int nextIndex = getNextLogIndex();

			LogEntry logEntry = LogEntry
					.newBuilder()
					.setIndex(nextIndex)
					.setTerm(state.getCurrentTerm())
					.setUserPayload(payload)
					.build();

			addLog(logEntry);
		} else {
			Logger.getGlobal().info("forwarding add user request to leader");
			Internal.ForwardMessageRequest request =
					Internal.ForwardMessageRequest
							.newBuilder()
							.setUser(user)
							.build();

			InternalPacket packet =
					InternalPacket
							.newBuilder()
							.setForwardMessageRequest(request)
							.build();

			RemoteNode leaderRemoteNode = NodeMonitor.getInstance().getNodeMap().get(state.getCurrentLeader());
			if (leaderRemoteNode.isActive()) leaderRemoteNode.getChannel().writeAndFlush(packet);
		}
	}
	
	public void addMessage(Message message) {
		if (nodeType == NodeType.Leader) {
			MessagePayLoad payload =
					MessagePayLoad
							.newBuilder()
							.setPayload(message.toByteString())
							.build();

			int nextIndex = getNextLogIndex();

			LogEntry logEntry = LogEntry
					.newBuilder()
					.setIndex(nextIndex)
					.setTerm(state.getCurrentTerm())
					.setMessagePayload(payload)
					.build();

			addLog(logEntry);
		} else {
			Logger.getGlobal().info("forwarding add user request to leader");
			Internal.ForwardMessageRequest request =
					Internal.ForwardMessageRequest
							.newBuilder()
							.setMessage(message)
							.build();

			InternalPacket packet =
					InternalPacket
							.newBuilder()
							.setForwardMessageRequest(request)
							.build();

			RemoteNode leaderRemoteNode = NodeMonitor.getInstance().getNodeMap().get(state.getCurrentLeader());
			if (leaderRemoteNode.isActive()) leaderRemoteNode.getChannel().writeAndFlush(packet);
		}
	}
	
	public void markMessagesRead(String uname) {
		MessageReadPayload payload =
				MessageReadPayload
						.newBuilder()
						.setUname(uname)
						.build();

		if (nodeType == NodeType.Leader) {

			int nextIndex = getNextLogIndex();

			LogEntry logEntry = LogEntry
					.newBuilder()
					.setIndex(nextIndex)
					.setTerm(state.getCurrentTerm())
					.setMessageReadPayload(payload)
					.build();

			addLog(logEntry);

		} else {
			Logger.getGlobal().info("forwarding read message request to leader");
			Internal.ForwardMessageRequest request =
					Internal.ForwardMessageRequest
							.newBuilder()
							.setMessageReadPayload(payload)
							.build();

			InternalPacket packet =
					InternalPacket
							.newBuilder()
							.setForwardMessageRequest(request)
							.build();

			RemoteNode leaderRemoteNode = NodeMonitor.getInstance().getNodeMap().get(state.getCurrentLeader());
			if (leaderRemoteNode.isActive()) leaderRemoteNode.getChannel().writeAndFlush(packet);
		}
	}
	
	/********************************************************************************/
	/* apis to commit messages, users, and mark messages as read */
	/********************************************************************************/
	private void generateDummyUsers() {
		Logger.getGlobal().info("going to create dummy users");
		Message m1 = 
				Message
				.newBuilder()
				.setAction(routing.Pipe.Message.ActionType.POST)
				.setPayload("test message")
				.setReceiverId("recId")
				.setSenderId("senId")
				.setStatus(Status.ACTIVE)
				.setTimestamp("timestamp")
				.setType(Message.Type.SINGLE)
				.build();
		
		Message m2 = 
				Message
				.newBuilder()
				.setAction(routing.Pipe.Message.ActionType.POST)
				.setPayload("test message")
				.setReceiverId("recId")
				.setSenderId("senId")
				.setStatus(Status.ACTIVE)
				.setTimestamp("timestamp")
				.setType(Message.Type.SINGLE)
				.build();
		
		addMessage(m1);
		addMessage(m2);
	}
	
	private void applyLog(LogEntry entry) {
		Logger.getGlobal().info("going to apply log");
		try {
			if (entry.hasUserPayload()) {
				commitUser(User.parseFrom(entry.getUserPayload().getPayload()));
				
			} else if (entry.hasMessagePayload()) {
				commitMessage(Message.parseFrom(entry.getMessagePayload().getPayload()));
				
			} else if (entry.hasMessageReadPayload()) {
				commitMessagesRead(
						entry.getMessageReadPayload().getUname()
				);
			}
			
			Logger.getGlobal().info("log applied to db: " + entry.toString());
			
		} catch(InvalidProtocolBufferException e) {
			Logger.getGlobal().info("failed to apply log to db: " + entry.toString());
			e.printStackTrace();
		}
	}
	
	private void applyLogs(List<LogEntry> entries) {
		for (LogEntry logEntry : entries) {
			applyLog(logEntry);
		}
	}
	
	private void commitUser(User user) {
		UserMongoDB.getInstance().commitUser(user);
	}
	
	private void commitMessage(Message message) {
		MessageMongoDB.getInstance().commitMessage(message);
		
		if (ClientChannelCache.getInstance().getClientChannelMap().containsKey(message.getReceiverId())) {
			Channel channel = ClientChannelCache.getInstance().getClientChannelMap().get(message.getReceiverId()).getClienChannel();
			if (channel.isActive()) {
				Logger.getGlobal().info("sending message to receiver: " + message.toString());

				Pipe.Route route
						= Pipe
						.Route
						.newBuilder()
						.setId(0)
						.setPath(Pipe.Route.Path.MESSAGE)
						.setMessage(message)
						.build();

				channel.writeAndFlush(route);
				
				markMessagesRead(message.getReceiverId());
			} else {
				Logger.getGlobal().info("channel closed, cannot flush to client");
			}
		}
	}
	
	private void commitMessagesRead(String uname) {
		MessageMongoDB.getInstance().markMessagesRead(uname);
	}
	
	/********************************************************************************/
	/* add logs */
	/********************************************************************************/
	// get next index for creating new logs
	private synchronized int getNextLogIndex() {
		LogEntry entry = getLastLogEntry();
		int nextLogIndex = entry != null ? entry.getIndex() + 1 : DEFAULT_LOG_INDEX;
		return nextLogIndex;
	}
	
	private synchronized void addLog(LogEntry logEntry) {
		Logger.getGlobal().info("adding log: " + logEntry.toString());
		List<LogEntry> entries = new ArrayList<LogEntry>();
		entries.add(logEntry);
		addLogs(entries);
		Logger.getGlobal().info("added log");
	}
	
	private void addLogs(List<LogEntry> entries) {
		Logger.getGlobal().info("adding new entries to logs, size: " + entries.size());
		
		if (nodeType == NodeType.Leader && entries.size() > 0) {
			NodeStateMongoDB.getInstance().commitLogEntries(entries);
			lastLogEntry = entries.get(entries.size() - 1);
			
			for (LogEntry entry : entries) {
				state.getLogIndexTermMap().put(entry.getIndex(), entry.getTerm());
			}
		}
	}
	
	/********************************************************************************/
	/* Node state */
	/********************************************************************************/
	
	public class State {
		// persistent variables
		private int currentTerm = 0;
		private int lastApplied = 0;
		private int lastVotedTerm = 0;
		
		// volatile variables 
		private RoutingConf conf;
		private NodeConf nodeConf;
		private int commitIndex = 0;
		private int currentLeader = 0;
		
		// leader variables
		private Map<Integer, Integer> nextIndices = new ConcurrentHashMap<Integer, Integer>();
		private Map<Integer, Integer> matchIndices = new ConcurrentHashMap<Integer, Integer>();
		private Map<Integer, Integer> logIndexTermMap = new ConcurrentHashMap<Integer, Integer>();
		
		// election variables
		private int votesReceived;
		private int votesExpected;
		private int yesVotes;
		
		public int getCurrentTerm() {
			return currentTerm;
		}
		
		public synchronized void setCurrentTerm(int currentTerm) {
			this.currentTerm = currentTerm;
		}
		
		public int getLastApplied() {
			return lastApplied;
		}
		
		public synchronized void setLastApplied(int lastApplied) {
			this.lastApplied = lastApplied;
		}
		
		public RoutingConf getConf() {
			return conf;
		}
		
		public synchronized void setConf(RoutingConf conf) {
			this.conf = conf;
		}
		
		public NodeConf getNodeConf() {
			return nodeConf;
		}
		
		public synchronized void setNodeConf(NodeConf conf) {
			this.nodeConf = conf;
		}
		
		public int getCommitIndex() {
			return commitIndex;
		}
		
		public synchronized void setCommitIndex(int commitIndex) {
			this.commitIndex = commitIndex;
		}
		
		public int getLastVotedTerm() {
			return lastVotedTerm;
		}
		
		public synchronized void setLastVotedTerm(int term) {
			lastVotedTerm = term;
		}
		
		public State(RoutingConf conf, NodeConf nodeConf) {
			this.conf = conf;
			this.nodeConf = nodeConf;
		}
		
		public int getVotesReceived() {
			return votesReceived;
		}
		
		public synchronized void setVotesReceived(int votesReceived) {
			this.votesReceived = votesReceived;
		}
		
		public int getVotesExpected() {
			return votesExpected;
		}
		
		public synchronized void setVotesExpected(int votesExpected) {
			this.votesExpected = votesExpected;
		}
		
		public int getYesVotes() {
			return yesVotes;
		}
		
		public synchronized void setYesVotes(int yesVotes) {
			this.yesVotes = yesVotes;
		}
		
		public int getCurrentLeader() {
			return currentLeader;
		}

		public synchronized void setCurrentLeader(int currentLeader) {
			this.currentLeader = currentLeader;
		}

		public Map<Integer, Integer> getNextIndices() {
			return nextIndices;
		}
		
		public Map<Integer, Integer> getMatchIndices() {
			return matchIndices;
		}
		
		public Map<Integer, Integer> getLogIndexTermMap() {
			return logIndexTermMap;
		}

		public synchronized void incrementCurrentTerm() {
			currentTerm++;
		}
		
		public synchronized void incrementVotesExpected() {
			votesExpected++;
		}
		
		public synchronized void incrementVotesReceived() {
			votesReceived++;
		}
		
		public synchronized void incrementYesVotes() {
			yesVotes++;
		}
		
		public void resetNextIndices() {
			LogEntry entry = getLastLogEntry();
			int nextIndex = entry != null ? entry.getIndex() + 1: DEFAULT_LOG_INDEX;
			for (RemoteNode node : NodeMonitor.getInstance().getNodeMap().values()) {
				nextIndices.put(node.getNodeConf().getNodeId(), nextIndex);
			}
		}
		
		public void resetmatchIndices() {
			for (RemoteNode node : NodeMonitor.getInstance().getNodeMap().values()) {
				matchIndices.put(node.getNodeConf().getNodeId(), 0);
			}
		}
		
		public void resetLogIndexTermMap() {
			logIndexTermMap.clear();
		}
	}

}
