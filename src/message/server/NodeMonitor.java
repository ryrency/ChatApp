package message.server;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import message.server.config.NodeConf;
import message.server.config.RoutingConf;
import message.server.raft.MessageBuilder;
import message.server.raft.RaftNode;
import message.server.raft.ScheduledExecutor;
import raft.proto.Internal.ConnectionActiveAck;
import raft.proto.Internal.InternalPacket;
import raft.proto.Work.WorkMessage;

public class NodeMonitor {

	private static final long RECONNECT_DELAY = 1000;
	
	static ConcurrentHashMap<Integer, RemoteNode> nodeMap = new ConcurrentHashMap<Integer, RemoteNode>();
	static ConcurrentHashMap<String,ExternalNode> externalNodeMap = new ConcurrentHashMap<String,ExternalNode>();
//	EventLoopGroup group = null;
//	static Bootstrap b;
	NodeConf nodeConf;
	private EventLoopGroup workerGroup = new NioEventLoopGroup();
	private ScheduledExecutor executor = new ScheduledExecutor(1);

	public static NodeMonitor instance;

	public static NodeMonitor getInstance() {
		if (instance == null) {
			instance = new NodeMonitor();
		}
		return instance;
	}

	private NodeMonitor() {
		
	}
	
	public void init(RoutingConf conf, NodeConf nodeConf) {
		this.nodeConf = nodeConf;
		
		for (NodeConf node : conf.getNodes()) {
			if (node != nodeConf) {
				RemoteNode rm = new RemoteNode(node);
				addNode(rm);
			}
		}
	}

	public void start() {
		// TODO Auto-generated method stub
		List<Integer> monitorNodes = nodeConf.getMonitorConnections();
		for (RemoteNode rm : nodeMap.values()) {
			if (monitorNodes.contains(rm.getNodeConf().getNodeId())) scheduleConnect(rm, 0);
		}
		
		// Creating TCP connection with externalNodes
		for(ExternalNode externalNode : getExternalNodeMap().values()) {
			scheduleExternalConnect(externalNode,0);
			
		}
		
		
	}

	public synchronized void connectWithNode(RemoteNode rm) {
		try {
			Logger.getGlobal().info("starting to connect with node --> " + rm.getNodeConf().getInternalSocketServerAddress());
			Bootstrap b = new Bootstrap();
				
			b.group(workerGroup).channel(NioSocketChannel.class).handler(new WorkInit());
			b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
			b.option(ChannelOption.TCP_NODELAY, true);
			b.option(ChannelOption.SO_KEEPALIVE, true);

			ChannelFuture cf = b.connect(rm.getNodeConf().getHost(), rm.getNodeConf().getInternalPort()).syncUninterruptibly();
			Logger.getGlobal().info("remote node status--> " + rm.getNodeConf().getInternalSocketServerAddress() + " - channel status: open: " + cf.channel().isOpen() + ", active: " + cf.channel().isActive() + ", isWritable: " + cf.channel().isWritable());
				
			rm.setChannel(cf.channel());
				
			ChannelClosedListener listener = new ChannelClosedListener(rm, this);
			cf.channel().closeFuture().addListener(listener);
			
			sendAddRequestToExistingNode(rm);
			
		} catch (Exception ex) {
			Logger.getGlobal().info("channel failed to connect with --> " + rm.getNodeConf().getInternalSocketServerAddress());
			scheduleConnect(rm, RECONNECT_DELAY);
		} 
	}

	public synchronized void scheduleConnect(final RemoteNode rm, long millis) {
		Logger.getGlobal().info("scheduling connect with " + rm.getNodeConf().getInternalSocketServerAddress() + " in " + millis + "ms");
		executor.schedule(new Runnable() {
			
			@Override
			public void run() {
				// TODO Auto-generated method stub
				connectWithNode(rm);
				
			}
		}, millis, TimeUnit.MILLISECONDS);
	}
	
	public synchronized void connectWithExternalNode(ExternalNode externalNode) {
		try {
			Logger.getGlobal().info("Starting to connect with node --->" +externalNode.getHost());
			Bootstrap b = new Bootstrap();
			
			b.group(workerGroup).channel(NioSocketChannel.class).handler(new ServerInit(RaftNode.getInstance().getState().getConf()));
			b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
			b.option(ChannelOption.TCP_NODELAY, true);
			b.option(ChannelOption.SO_KEEPALIVE, true);
			
			ChannelFuture cf = b.connect(externalNode.getHost(),externalNode.getPort()).syncUninterruptibly();
			Logger.getGlobal().info("External Node Status--> " + externalNode.getHost() + " - channel status: open: " + cf.channel().isOpen() + ", active: " + cf.channel().isActive() + ", isWritable: " + cf.channel().isWritable());
		
			externalNode.setChannel(cf.channel());
			cf.channel().closeFuture().addListener(new ExternalChannelClosedListener(externalNode, this));
		} catch(Exception ex) {
			Logger.getGlobal().info("channel failed to connect with --> " + externalNode.getHost());
			scheduleExternalConnect(externalNode, RECONNECT_DELAY);
		}
		
	}
	
	public synchronized void scheduleExternalConnect(final ExternalNode externalNode, long millis) {
		Logger.getGlobal().info("scheduling connect with " + externalNode.getHost() + " in " + millis + "ms");
		
		executor.schedule(new Runnable() {
			
			@Override
			public void run() {
				NodeMonitor.this.connectWithExternalNode(externalNode);
			}
			
		},millis, TimeUnit.MILLISECONDS);
	}
	
	public void resetConnection(RemoteNode rm) {
		
	}

	public void sendAddRequestToExistingNode(RemoteNode rm) {
		try {
			Logger.getGlobal().info("Generated request to add adjacent node: "  + rm.getNodeConf().getInternalSocketServerAddress());
			
			ConnectionActiveAck ack = 
					ConnectionActiveAck
					.newBuilder()
					.setNodeId(nodeConf.getNodeId())
					.build();
			
			InternalPacket packet = InternalPacket.newBuilder().setConnectionActiveAck(ack).build();

			ChannelFuture cf = rm.getChannel().writeAndFlush(packet);

			if (cf.isDone() && !cf.isSuccess()) {
				System.out.println("Send failed: " + cf.cause());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public ConcurrentHashMap<Integer, RemoteNode> getNodeMap() {
		return nodeMap;
	}

	public synchronized void addNode(RemoteNode rm) {
		Logger.getGlobal().info("adding new node in config " + rm.getNodeConf().getInternalSocketServerAddress());
		int nodeId = rm.getNodeConf().getNodeId();
		if (!nodeMap.containsKey(nodeId) || !nodeMap.get(nodeId).isActive()) {
			nodeMap.put(nodeId, rm);
		}
	}

	public NodeConf getNodeConf() {
		return this.nodeConf;
	}

	public void printNodeMap() {
		System.out.println("***Printing node map****");
		for (RemoteNode rm : nodeMap.values()) {

			System.out.println("TOPO remote node :" + rm.getNodeConf().getInternalSocketServerAddress() + "--" + rm.isActive() + "------"
					+ rm.getChannel());
		}
	}
	
	
	public static class ChannelClosedListener implements ChannelFutureListener {
		RemoteNode rm;
		NodeMonitor nm;

		public ChannelClosedListener(RemoteNode node, NodeMonitor monitor) {
			rm = node;
			nm = monitor;
		}

		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			// we lost the connection or have shutdown.
			Logger.getGlobal().info("channel closed with server: " + rm.getNodeConf().getInternalSocketServerAddress());
			nm.scheduleConnect(rm, RECONNECT_DELAY);
		}
	}
	
	public static class ExternalChannelClosedListener implements ChannelFutureListener {
		ExternalNode externalNode;
		NodeMonitor nodeMonitor;
		
		public ExternalChannelClosedListener(ExternalNode node, NodeMonitor monitor) {
			externalNode = node;
			nodeMonitor = monitor;
		}
		
		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			Logger.getGlobal().info("channel closed with server: " + externalNode.getHost());
			nodeMonitor.scheduleExternalConnect(externalNode, RECONNECT_DELAY);
		}
		
	}

	public ConcurrentHashMap<String, ExternalNode> getExternalNodeMap() {
		return externalNodeMap;
	}

	public synchronized void addExternalNode(ExternalNode em) {
		Logger.getGlobal().info("adding new node to external node map" + em.getHost() + ":" +em.getPort());
		if (!externalNodeMap.containsKey(em.getGroupTag()) || externalNodeMap.get(em.getGroupTag()).getHost().equals(em.getHost())) {
			externalNodeMap.put(em.getGroupTag(), em);
			
		}
	}
	
	public void printExternalMap() {
		System.out.println("***Printing external node map****");
		for (ExternalNode em : externalNodeMap.values()) {

			System.out.println("Remote Node :" + em.getHost() + "--" + em.getPort() + "--" + em.isActive() + "------"
					+ em.getChannel());
		}
	}
	
	

}
