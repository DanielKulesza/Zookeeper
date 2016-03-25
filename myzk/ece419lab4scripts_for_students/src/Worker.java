import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.Watcher.Event.EventType;

import java.io.IOException;
import java.io.net.Socket;
import java.io.net.ServerSocket;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;

public class Worker{

	static Socket fileServerSocket = null;
	static ZkConnector zkc = null;
	static String FSpath = "/fileserver";
	static String jobPath = "/jobs";
	static String finishedPath = "/finished";
	static Watcher watcher = null;
	static String hash = null;

	public Worker(String hosts) {
		zkc = new ZkConnector();
		try {
		  zkc.connect(hosts);
		} catch(Exception e) {
		  System.out.println("Zookeeper connect "+ e.getMessage());
		}

		watcher = new Watcher() { // Anonymous Watcher
		    @Override
		    public void process(WatchedEvent event) {
		      handleEvent(event);
		                    
		    }
		};
		
	}

	private static void handleEvent(WatchedEvent event) {
		String path = event.getPath();
		System.out.println(path);
		EventType type = event.getType();
		if (path.equalsIgnoreCase(myPath)) {

		}
	}


	public static void main (String[] args){

		if(args.length != 1) {
			System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. WorkerPool zkServer:clientPort\n");
      		return;
		}
	 	
		Worker worker = new worker(args[0]);

}
