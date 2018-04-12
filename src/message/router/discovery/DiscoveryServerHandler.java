/**
 * Copyright 2016 Gash.
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
package message.router.discovery;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import io.netty.util.CharsetUtil;
import io.netty.util.internal.SocketUtils;
import jdk.internal.dynalink.beans.StaticClass;
import message.server.ExternalNode;
import message.server.NodeMonitor;
import message.server.RemoteNode;
import message.server.config.NodeConf;
import message.server.config.RoutingConf;
import message.server.resources.NetworkDiscoveryResource;
import message.server.resources.RouteResource;
import message.utility.NetworkUtility;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

import routing.Pipe;
import routing.Pipe.Route;

import java.beans.Beans;
import java.util.HashMap;
import routing.Pipe.NetworkDiscoveryPacket;
import java.util.logging.*;

/**
 * The networkDiscovery handler processes json messages that are delimited by a 'newline'
 *
 * TODO replace println with logging!
 *
 * @author gash
 *
 */
public class DiscoveryServerHandler extends SimpleChannelInboundHandler<Route> {
	//protected static Logger logger = LoggerFactory.getLogger("discovery");
	RoutingConf conf;
	NodeConf nodeConf;
	private HashMap<String, String> routing;
    static int count = 0; 

	public DiscoveryServerHandler(RoutingConf conf, NodeConf nodeConf) {
		this.conf = conf;
		this.nodeConf = nodeConf;
		if (conf != null)
			routing = conf.asHashMap();
	}

	/**
	 * override this method to provide processing behavior. This implementation
	 * mimics the routing we see in annotating classes to support a RESTful-like
	 * behavior (e.g., jax-rs).
	 *
	 * @param msg
	 */
	public void handleMessage(Route msg, Channel channel) {
		if (msg == null) {
			// TODO add logging
			Logger.getGlobal().info("JSingh: ERROR: Unexpected content - " + msg);
			return;
		}

		Logger.getGlobal().info("JSingh:" + msg.getId() + ": " + msg.getPath());

		try {
			Logger.getGlobal().info("/" + msg.getPath().toString().toLowerCase());
			//if(!msg.getNetworkDiscoveryPacket().getNodeAddress().equals(NetworkUtility.getLocalHostAddress()) && 
					if(msg.getNetworkDiscoveryPacket().getMode()==NetworkDiscoveryPacket.Mode.REQUEST && msg.getNetworkDiscoveryPacket().getSecret().equals(nodeConf.getSecret())) {
			String clazz = routing.get("/" + msg.getPath().toString().toLowerCase());
			if (clazz != null) {
				RouteResource rsc = (RouteResource) Beans.instantiate(RouteResource.class.getClassLoader(), clazz);
				String hostIP = null;
				int port = 0;
//				NetworkDiscoveryResource rsc = new NetworkDiscoveryResource();
				try {
					
					if(msg.hasNetworkDiscoveryPacket()) {
						if(msg.getNetworkDiscoveryPacket().getSender()==NetworkDiscoveryPacket.Sender.EXTERNAL_SERVER_NODE) {
							NetworkDiscoveryPacket networkDiscoveryPacket = msg.getNetworkDiscoveryPacket();
							Logger.getGlobal().info("JSingh: External server node discovery -" + networkDiscoveryPacket.getNodeAddress());
							NodeMonitor.getInstance().addExternalNode(new ExternalNode(networkDiscoveryPacket.getGroupTag(),networkDiscoveryPacket.getNodeAddress(), (int)networkDiscoveryPacket.getNodePort()));
							NodeMonitor.getInstance().printExternalMap();
							hostIP =  NodeMonitor.getInstance().getNodeConf().getHost();
							port = NodeMonitor.getInstance().getNodeConf().getClientPort();
						}else {
							int NoOfActiveNodes = NodeMonitor.getInstance().getNodeMap().size() + 1;//1 for leader node
						    int selectedId;
						    
						    Logger.getGlobal().info("JSingh: No of active nodes -" + NoOfActiveNodes);
						    selectedId = count % NoOfActiveNodes + 1;
						    count++;

						    Logger.getGlobal().info("JSingh: Selected node ID - " + selectedId);   
						  
						    if (NodeMonitor.getInstance().getNodeConf().getNodeId() == selectedId) {
							   Logger.getGlobal().info("JSingh: Node Detail -" + NodeMonitor.getInstance().getNodeConf().getHost() + "--" + NodeMonitor.getInstance().getNodeConf().getInternalPort());
							   hostIP =  NodeMonitor.getInstance().getNodeConf().getHost();
							   port = NodeMonitor.getInstance().getNodeConf().getClientPort();
						    }else {
							   RemoteNode rm = NodeMonitor.getInstance().getNodeMap().get(selectedId);
							   while(!rm.isActive()) {
								   count++;
								   selectedId=selectedId+1;
								   rm = NodeMonitor.getInstance().getNodeMap().get(selectedId);
							   }
							   Logger.getGlobal().info("JSingh: Node Detail -" + rm.getNodeConf().getInternalSocketServerAddress() + "--" + rm.isActive() + "------"
										+ rm.getChannel());
							   hostIP =  rm.getNodeConf().getHost();
							   port = rm.getNodeConf().getClientPort();
							   }
						   }
					}
				    
		            NetworkDiscoveryPacket.Builder ndpb = NetworkDiscoveryPacket.newBuilder();
		            ndpb.setMode(NetworkDiscoveryPacket.Mode.RESPONSE);
		            ndpb.setSender(msg.getNetworkDiscoveryPacket().getSender());
		            ndpb.setGroupTag("groupJPRS");
		            ndpb.setNodeAddress(hostIP);
		            ndpb.setNodePort(port);
		            ndpb.setSecret("secret");

		            Route.Builder responseMsg = Route.newBuilder();
		            responseMsg.setId(0);
		            responseMsg.setPath(Route.Path.NETWORK_DISCOVERY);
		            responseMsg.setNetworkDiscoveryPacket(ndpb);
		            
					Logger.getGlobal().info("JSingh: Discovered node - " + hostIP + "," +  port);
					Route response = rsc.process(responseMsg.build());
					Logger.getGlobal().info("JSingh: " + responseMsg.build());
					Logger.getGlobal().info("JSingh: ---> reply: " + response + " to: " + msg.getNetworkDiscoveryPacket().getNodeAddress());
					Logger.getGlobal().info("JSingh: " + msg.getNetworkDiscoveryPacket().getNodeAddress());
					
					if (response != null) {

						channel.writeAndFlush(new DatagramPacket(
								Unpooled.copiedBuffer(responseMsg.build().toByteArray()),
								SocketUtils.socketAddress(
										msg.getNetworkDiscoveryPacket().getNodeAddress(), 
										nodeConf.getNetworkDiscoveryPort())))
								.sync();

					}
				} catch (Exception e) {
					Logger.getGlobal().info("JSingh: Failed to read route." + e);
				}
			} else {
				// TODO add logging
				Logger.getGlobal().info("JSingh: JSingh: ERROR: unknown path - " + msg.getPath());
			}
			}
			else {
				Logger.getGlobal().info("JSingh: ****Discarding Above Message****");
			}
		} catch (Exception ex) {
			// TODO add logging
			Logger.getGlobal().info("JSingh: ERROR: processing request - " + ex.getMessage());
		}

		System.out.flush();
	}

	/**
	 * a message was received from the server. Here we dispatch the message to
	 * the client's thread pool to minimize the time it takes to process other
	 * messages.
	 *
	 * @param ctx
	 *            The channel the message was received from
	 * @param msg
	 *            The message
	 */
	@Override
	protected void channelRead0(ChannelHandlerContext ctx, Route msg) throws Exception {
		System.out.println("------------");
		handleMessage(msg, ctx.channel());
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		//logger.error("Unexpected exception from downstream.", cause);
		ctx.close();
	}

}