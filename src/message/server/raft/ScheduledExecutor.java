package message.server.raft;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.logging.Logger;

public class ScheduledExecutor extends ScheduledThreadPoolExecutor{

	public ScheduledExecutor(int corePoolSize) {
		super(corePoolSize);
		// TODO Auto-generated constructor stub
	}
	
	@Override
	protected void afterExecute(Runnable r, Throwable t) {

	      super.afterExecute(r, t);
	      if (t == null && r instanceof Future<?>) {
	        try {
	          Future<?> future = (Future<?>) r;
	          if (future.isDone()) {
	            future.get();
	          }
	        } catch (CancellationException ce) {
	            t = ce;
	        } catch (ExecutionException ee) {
	            t = ee.getCause();
	        } catch (InterruptedException ie) {
	            Thread.currentThread().interrupt(); // ignore/reset
	        }
	      }
	      
	      if (t != null && !(t instanceof CancellationException)) {
	    	  t.printStackTrace();
	      }
	 }

}
