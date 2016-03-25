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
	static String jobPath = "/jobs";
	static String finishedPath = "/finished";
	static Watcher watcher = null;
	static String hash = null;

	public Worker;


	 public static void main (String[] args){
	 	

}