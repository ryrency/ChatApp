package message.server.raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import message.server.config.RoutingConf;

public class NodeState {

	public static final int LEADER = 0;
	public static final int CANDIDATE = 1;
	public static final int FOLLOWER = 2;

	private static int state = FOLLOWER;

	public static int currentTerm = 0;
	
	protected static Logger logger = (Logger) LoggerFactory.getLogger("NODESTATE");

	// private static Timestamp timeStampOnLatestUpdate=null;
	private static Long timeStampOnLatestUpdate = null;

	// private static long noTaskProcessed = 0;
	
	public static RoutingConf conf;

	public static Long getTimeStampOnLatestUpdate() {
		if (timeStampOnLatestUpdate == null) {
			// timeStampOnLatestUpdate = new Timestamp(System.currentTimeMillis());
			timeStampOnLatestUpdate = System.currentTimeMillis();

			// DatabaseService.getInstance().getDb().getCurrentTimeStamp(); -
		}
		return timeStampOnLatestUpdate;
	}

	public static void setTimeStampOnLatestUpdate(Long timeStampOnLatestUpdate) {
		NodeState.timeStampOnLatestUpdate = timeStampOnLatestUpdate;
	}


	private static NodeState instance = null;

	public static NodeState getInstance() {
		if (instance == null) {
			instance = new NodeState();
		}
		return instance;
	}


	public synchronized int getState() {
		return state;
	}
	
	public static RoutingConf getConf() {
		return conf;
	}

	public static void setConf(RoutingConf conf) {
		NodeState.conf = conf;
	}
}
