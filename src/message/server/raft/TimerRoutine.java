package message.server.raft;

import java.util.Random;

public class TimerRoutine {

	public static long getCurrentUnixTimeStamp() {
		long unixTime = System.currentTimeMillis() / 1000L;
		return unixTime;

	}

	public static long getElectionTimeout() {
		Random random = new Random();
		long variableTimeout = random.nextInt((int) ((5000L - 1000L) + 1L));
		long currentTimeout = 28000L + (long) variableTimeout;
		return currentTimeout;

	}

	int randomWithRange(int min, int max) {
		int range = (max - min) + 1;
		return (int) (Math.random() * range) + min;
	}
	
	public static long getFixedTimeout() {
		return 10000L;
	}
	
	public static long getFollowerTimeOut() {
		// 4s (fixed delay) + 1s(variable delay)
		Random random = new Random();
		long variableTimeout = random.nextInt((int)(500L));
		long currentTimeout = 1500L + (long) variableTimeout;
		return currentTimeout;
	}
	
	public static long getHeartbeatSendDelay() {
		return 500L;
	}
	
	public static long getLogCommitInterval() {
		return 500L;
	}
	
	public static long getCandidateElectionTimeout() {
		return 500L;
	}
}
