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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.codehaus.jackson.map.ObjectMapper;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import message.server.config.NodeConf;
import message.server.config.RoutingConf;
import message.server.raft.RaftNode;
public class MessageServer {
	protected static Logger logger = Logger.getGlobal();

	protected static HashMap<Integer, ServerBootstrap> bootstrap = new HashMap<Integer, ServerBootstrap>();

	public static final String sPort = "port";
	public static final String sPoolSize = "pool.size";

	protected RoutingConf conf;
	protected NodeConf nodeconf;
	protected boolean background = false;
	
	private boolean writeLogsToFile = true;

	public void release() {
	}

	public void startServer() {
		if (writeLogsToFile) enablesLogsToFile("log/node_" + nodeconf.getNodeId() + ".log");
		
		logger.info("Communication starting");
		RaftNode.getInstance().init(conf, nodeconf);
		
		StartCommunication comm;
		
		// start socket server for internal communication
		comm =  new StartCommunication(nodeconf.getInternalPort(), new WorkInit());
		//Always need to run in background
//		comm.run();
		Thread cthread = new Thread(comm);
		cthread.start();
		
		//start the monitor to monitor other nodes  
		NodeMonitor.getInstance().init(conf, nodeconf);
		NodeMonitor.getInstance().start();
		
		
		//Starting Consumer for client services e.g. 4267(routing.conf)
		comm =  new StartCommunication(nodeconf.getClientPort(), new ServerInit(conf));
		if (background) {
			cthread = new Thread(comm);
			cthread.start();
		} else {
			comm.run();
		}
		

	}

	/**
	 * static because we need to get a handle to the factory from the shutdown
	 * resource
	 */
	public static void shutdown() {
		logger.info("Server shutdown");
		System.exit(0);
	}

	/**
	 * initialize the server with a configuration of it's resources
	 * 
	 * @param cfg
	 */
	public MessageServer(File cfg, int currentNodeId) {
		//init(cfg);
		System.out.println("init ***");
		init(cfg, currentNodeId);
	}

	private void init(File cfg, int currentNodeId) {
		if (!cfg.exists()) throw new RuntimeException(cfg.getAbsolutePath() + " not found");
		
		// resource initialization - how message are processed
		BufferedInputStream br = null;
		try {
			byte[] raw = new byte[(int) cfg.length()];
			br = new BufferedInputStream(new FileInputStream(cfg));
			br.read(raw);
			conf = JsonUtil.decode(new String(raw), RoutingConf.class);
			br.close();
			if (conf == null)
				throw new RuntimeException("verification of configuration failed");
			
			nodeconf = conf.getNodeConf(currentNodeId);
			
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}


	/**
	 * initialize netty communication
	 * 
	 * @param port
	 *            The port to listen to
	 */
	private static class StartCommunication implements Runnable {
		int port;
		ChannelInitializer<SocketChannel> channelInit;

		public StartCommunication(int port, ChannelInitializer<SocketChannel> channelInit) {
			this.port = port;
			this.channelInit = channelInit;
		}

		public void run() {
			// construct boss and worker threads (num threads = number of cores)
			System.out.println("***MessageServer.StartCommunication***fn:run()***");
			System.out.println(this.port);

			EventLoopGroup bossGroup = new NioEventLoopGroup();
			EventLoopGroup workerGroup = new NioEventLoopGroup();

			try {
				ServerBootstrap b = new ServerBootstrap();
				bootstrap.put(this.port, b);

				b.group(bossGroup, workerGroup);
				b.channel(NioServerSocketChannel.class);
				b.option(ChannelOption.SO_BACKLOG, 100);
				b.option(ChannelOption.TCP_NODELAY, true);
				b.option(ChannelOption.SO_KEEPALIVE, true);
				// b.option(ChannelOption.MESSAGE_SIZE_ESTIMATOR);
				
				b.childHandler(channelInit);

				// Start the server.
				logger.info("Starting server, listening on port = " + this.port);
				ChannelFuture f = b.bind(this.port).syncUninterruptibly();

				logger.info(f.channel().localAddress() + " -> open: " + f.channel().isOpen() + ", write: "
						+ f.channel().isWritable() + ", act: " + f.channel().isActive());

				// block until the server socket is closed.
				f.channel().closeFuture().sync();

			} catch (Exception ex) {
				// on bind().sync()
				logger.info("Failed to setup handler.");
				ex.printStackTrace();
			} finally {
				// Shut down all event loops to terminate all threads.
				bossGroup.shutdownGracefully();
				workerGroup.shutdownGracefully();
			}
						


			
		}
	}

	/**
	 * help with processing the configuration information
	 * 
	 * @author gash
	 *
	 */
	public static class JsonUtil {
		private static JsonUtil instance;

		public static void init(File cfg) {

		}

		public static JsonUtil getInstance() {
			if (instance == null)
				throw new RuntimeException("Server has not been initialized");

			return instance;
		}

		public static String encode(Object data) {
			try {
				ObjectMapper mapper = new ObjectMapper();
				return mapper.writeValueAsString(data);
			} catch (Exception ex) {
				return null;
			}
		}

		public static <T> T decode(String data, Class<T> theClass) {
			try {
				ObjectMapper mapper = new ObjectMapper();
				return mapper.readValue(data.getBytes(), theClass);
			} catch (Exception ex) {
				return null;
			}
		}
	}
	
	private void enablesLogsToFile(String fileName) {  
	    FileHandler fh;  

	    try {  

	        // This block configure the logger with handler and formatter  
	        fh = new FileHandler(fileName);  
	        logger.addHandler(fh);
	        SimpleFormatter formatter = new SimpleFormatter();  
	        fh.setFormatter(formatter);    

	    } catch (SecurityException e) {  
	        e.printStackTrace();  
	    } catch (IOException e) {  
	        e.printStackTrace();  
	    }
	}

}
