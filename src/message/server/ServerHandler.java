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
package message.server;

import java.beans.Beans;
import java.util.HashMap;
import java.util.logging.Logger;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import message.server.config.RoutingConf;
import message.server.raft.ClientChannelCache;
import message.server.raft.RaftNode;
import message.server.resources.RouteResource;
import raft.proto.Internal.ForwardMessageRequest;
import raft.proto.Internal.InternalPacket;
import routing.Pipe.Route;
/**
 * The message handler processes json messages that are delimited by a 'newline'
 * 
 * TODO replace println with logging!
 * 
 * @author gash
 * 
 */
public class ServerHandler extends SimpleChannelInboundHandler<Route> {

	private HashMap<String, String> routing;

	public ServerHandler(RoutingConf conf) {
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
			System.out.println("ERROR: Unexpected content - " + msg);
			return;
		}

		if(msg.hasUser()) {
			Logger.getGlobal().info("server received user register request" + msg.getUser());

			//save the channel in cache
			ClientChannelCache
					.getInstance()
					.addClientChannelToMap(msg.getUser().getUname(), channel);

			RaftNode.getInstance().addUser(msg.getUser());

			RaftNode.getInstance().pushUnreadMessagesToClient(msg.getUser().getUname());

		} else if(msg.hasMessage()) {
			Logger.getGlobal().info("server received message from client" + msg.getMessage());
			RaftNode.getInstance().addMessage(msg.getMessage());

		} else {
		System.out.println("---> " + msg.getId() + ": " + msg.getPath());

		
//		try {
//			String clazz = routing.get("/"+msg.getPath().toString().toLowerCase());
//			if(clazz != null) {
//				RouteResource routeResource = (RouteResource) Beans.instantiate(RouteResource.class.getClassLoader(), clazz);
//				Route response = routeResource.process(msg);
//				if(response!=null) {
//					channel.writeAndFlush(response).sync();
//				}
//				
//			}
//			else {
//				// TODO add logging
//				System.out.println("ERROR: unknown path - " + msg.getPath());
//			}
//			
//		}
//		catch (Exception ex) {
//			// TODO add logging
//			System.out.println("ERROR: processing request - " + ex.getMessage());
//		}

		System.out.flush();
		}


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
		handleMessage(msg, ctx.channel());
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		ctx.close();
	}

}