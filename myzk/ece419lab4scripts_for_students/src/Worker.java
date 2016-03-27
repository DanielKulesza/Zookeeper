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
import java.util.List;

public class Worker{

	static Socket fileServerSocket = null;
	static ZkConnector zkc = null;
	static String fsPath = "/fileserver";
	static String jobPath = "/jobs";
	static String finishedPath = "/finished";
	static Watcher watcher = null;
	static String hash = null;
    static fsSocket = null;
    static ObjectOutputStream fsOut = null;
    static ObjectInputStream fsIn = null;

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
    
    public void connectToFS(String hostname, int port) {
        fsSocket = new Socket(hostname, port);
        fsOut = new ObjectOutputStream(fsSocket.getOutputStream());
        fsIn = new ObjectInputStream(fsSocket.getInputStream());
    }


	public static void main (String[] args){

		if(args.length != 1) {
			System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. WorkerPool zkServer:clientPort\n");
      		return;
		}
	 	
		Worker worker = new worker(args[0]);
        
        Stat stat = zkc.exists(fsPath, watcher);
        while (stat == null) {
            try {Thread.sleep(5);} catch (Exception e){}
            stat = zkc.exists(FSpath, watcher);
        }
        
        stat = zkc.exists(jobPath, watcher);
        while (stat == null) {
            try {Thread.sleep(5);} catch (Exception e){}
            stat = zkc.exists(jobPath, watcher);
        }
        
        byte[] data = zkc.getData(fsPath, watcher, stat);
        String hostname = new String(data).split(":")[0];
        int port = Integer.parseInt(new String(data).split(":")[1]);
        connectToFS(hostname, port);

        while(true) {
            List<String> jobs = zkc.getChildren(jobPath, watcher);
            while(job.isEmpty()) {
                try {Thread.sleep(10000);} catch (Exception e){}
                jobs = zkc.getChildren(jobPath, watcher);
            }
            
//            Stat selectedJobStat = zkc.exists(jobPath + "/" + jobs.get(0), watcher);
//            while (selectedJobStat == null) {
//                try {Thread.sleep(5);} catch (Exception e){}
//                selectedJobStat = zkc.exists(jobPath + jobs.get(0), watcher);
//                
//            }
            int i = 0, j = 0;
            boolean found = false;
            while(!found && i < jobs.length) {
                List<String> partitions = zkc.getChildren(jobPath + jobs.get(i), watcher);
                
                j = 0;
                byte[] data = zkc.getData(jobPath + "/" + jobs.get(i) + "/" + partitions.get(j), watcher, stat);
                String status = new String(data);
            
                while(status.equals("working") && j < partitions.length
) {
                    j++;
                    data = zkc.getData(jobPath + "/" + jobs.get(i) + "/" + partitions.get(j), watcher, stat);
                    status = new String(data);
                }
            
                if(j < partitions.length) {
                    zkc.setData(jobPath + jobs.get(i), "working", -1);
                    found = true;
//                    String[] taskSplitted = jobData.split("-");
//                    int partitionID = Integer.parseInt(taskSplitted[0]);
//                    String goldenHash = taskSplitted[1];
                }
            }
            
            if(i < jobs.length) {
                String jobHash = jobs.get(i);
                String partitionID = partitions.get(j);
                String password = null;
                
                try {
                    fsOut.writeObject(partitionID);
                } catch (Exception e) {
                    System.out.println("Primary File Server has disconnected");
                    // watcher will update info when backup becomes the new boss
                    while (zkc.exists(fsPath, watcher) == null){}
                    try {Thread.sleep(1000);} catch (Exception exception){}
                    
                    byte[] data = zkc.getData(fsPath, watcher, stat);
                    String hostname = new String(data).split(":")[0];
                    int port = Integer.parseInt(new String(data).split(":")[1]);
                    connectToFS(hostname, port);
                    
                    fsOut.writeObject(packetToNode);
                }
                
                ArrayList<String> dictPartition = fsIn.readObject();
                
                MessageDigest md5 = MessageDigest.getInstance("MD5");
                for (String word : dictPartition) {
                    byte [] hashraw = md5.digest(word.getBytes());
                    BigInteger hashInt = new BigInteger(1,hashraw);
                    String hash = hashInt.toString(16);
                    while (hash.length() < 32) {
                        hash = "0" + hash;
                    }
                    if (hash.equals(jobHash)) {
                        password = word;
                        break;
                    }
                    
                }
                
                String path = jobPath + "/" + jobHash + "/" + partitionID;
                Stat stat = zkc.exist(path, watcher);
                if(stat != null) zkc.delete(path, -1);
                
                if(password != null) {
                    stat = zkc.exists(finishedPath + "/" + jobHash, watcher);
                    if(stat == null) {
                        System.out.println("Creating " + finishedPath + "/" + jobHash);
                        Code ret = zkc.create(
                                              finishedPath + "/" + jobHash,
                                              password,
                                              CreateMode.PERSISTENT
                                              );
                        
                        if (ret == Code.OK) {
                            System.out.println("Created " + finishedPath + "/" + jobHash);
                        }
                    }
                }    
            }
        }
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        

}
