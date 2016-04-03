import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.KeeperException.Code;


import java.io.IOException;
import java.net.Socket;
import java.net.ServerSocket;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.util.*;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.math.BigInteger;


public class Worker{

	static Socket fileServerSocket = null;
	static ZkConnector zkc = null;
    static ZooKeeper zk = null;
	static String myPath = null;
	static String fsPath = "/FileServer";
	static String jobPath = "/jobs";
	static String finishedPath = "/finished";
	static String workerPath = "/worker";
	static String currentJob = null;
	static Watcher watcher = null;
	static String hash = null;
    static Socket fsSocket = null;
    private static ObjectOutputStream fsOut = null;
    private static ObjectInputStream fsIn = null;

	public Worker(String hosts) {
		zkc = new ZkConnector();
		try {
		  zkc.connect(hosts);
		} catch(Exception e) {
		  System.out.println("Zookeeper connect "+ e.getMessage());
		}

        zk = zkc.getZooKeeper();

		watcher = new Watcher() { // Anonymous Watcher
		    @Override
		    public void process(WatchedEvent event) {
		      handleEvent(event);
		                    
		    }
		};
        this.fsOut = null;
        this.fsOut = null;
		
	}

	private static void handleEvent(WatchedEvent event) {
		String path = event.getPath();
		System.out.println(path);
		EventType type = event.getType();
		if (path.equalsIgnoreCase(finishedPath)) {

		}
	}
    
    public void connectToFS(String hostname, int port) {
        try{
			System.out.println("Connecting to: " + hostname + ":"  + port);
            fsSocket = new Socket(hostname, port);
            fsOut = new ObjectOutputStream(fsSocket.getOutputStream());
            fsIn = new ObjectInputStream(fsSocket.getInputStream());
        }catch (Exception e){

        }
    }



	public static void main (String[] args){

		if(args.length != 1) {
			System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. WorkerPool zkServer:clientPort\n");
      		return;
		}
	 	
		Worker worker = new Worker(args[0]);
        System.out.println("Connected to Zookeeper");

        
        Stat stat = zkc.exists(fsPath, watcher);
        while (stat == null) {
            try {Thread.sleep(5);} catch (Exception e){}
            stat = zkc.exists(fsPath, watcher);
        }
        System.out.println("File Server is up.");

		//create main worker node
		stat = zkc.exists(workerPath, watcher);
		if(stat == null) {
		    System.out.println("Creating " + workerPath);
		    Code ret = zkc.create(
		                      workerPath,
		                      null,
		                      CreateMode.PERSISTENT
		                      );
		
		    if (ret == Code.OK) {
		        System.out.println("Created " + workerPath);
		    }
		}
		
		List<String> workers = null;
        String id = null;
		try { workers = zk.getChildren(jobPath, watcher);} catch (Exception e){}
        if (id ==null) id = "0";
        else id = String.valueOf(workers.size());
		
		stat = zkc.exists(workerPath + "/" + id, watcher);
		while(stat != null) {
			id = String.valueOf(Integer.parseInt(id) + 1);
			stat = zkc.exists(workerPath + "/" + id, watcher);
		}
		if(stat == null) {
		    System.out.println("Creating " + workerPath + "/" + id);
		    Code ret = zkc.create(
		                      workerPath + "/" + id,
		                      currentJob,
		                      CreateMode.EPHEMERAL
		                      );
		
		    if (ret == Code.OK) {
		        System.out.println("Created " + workerPath + "/" + id);
		    }
		}

		myPath = workerPath + "/" + id;
        
        stat = zkc.exists(jobPath, watcher);
        while (stat == null) {
            try {Thread.sleep(5);} catch (Exception e){}
            stat = zkc.exists(jobPath, watcher);
        }
        System.out.println("Job Path is up.");
        
        try{
            byte[] data = zk.getData(fsPath, watcher, stat);
            String hostname = new String(data).split(":")[0];
            int port = Integer.parseInt(new String(data).split(":")[1]);
            worker.connectToFS(hostname, port);
            System.out.println("Connected to Primary File Server");
        }catch (Exception e){
			e.printStackTrace();
		}


        List<String> jobs = null;
        while(true) {
            

            try { jobs = zk.getChildren(jobPath, watcher);} catch (Exception e){}
            while(jobs.isEmpty()) {
                try {
                    Thread.sleep(1000);
                    System.out.println("No jobs. Sleeping.");
                } catch (Exception e){}
                try {jobs = zk.getChildren(jobPath, watcher);} catch (Exception e){}
            }
            
//            Stat selectedJobStat = zkc.exists(jobPath + "/" + jobs.get(0), watcher);
//            while (selectedJobStat == null) {
//                try {Thread.sleep(5);} catch (Exception e){}
//                selectedJobStat = zkc.exists(jobPath + jobs.get(0), watcher);
//                
//            }
            int i = 0, j = 0;
            boolean found = false;
            List<String> partitions = null;

            while(!found && i < jobs.size()) {
                System.out.println("Looking for job");
                try {
					partitions = zk.getChildren(jobPath + "/" + jobs.get(i), watcher);
                    System.out.println(partitions + " " + partitions.size());
                    j = 0;


                    String status = "";
                    byte [] data;
                    while(j < partitions.size()){
                        String working_path = jobPath + "/" + jobs.get(i) + "/" + partitions.get(j);
                        data = zk.getData(working_path, watcher, stat);
                        if (data != null) status = new String(data);
                        else status = "";
                        
                        if(status.equals("working")){
                            j++;
                            System.out.println(working_path + " is already being worked on.");
                        }
                        else{
                            break;
                        }
                    }
                
                    if(!status.equals("working")){
                        System.out.println("Found a job!");
                        zk.setData(jobPath + "/" + jobs.get(i) + "/" + partitions.get(j), "working".getBytes(), -1);
                        found = true;
                        break;
//                    String[] taskSplitted = jobData.split("-");
//                    int partitionID = Integer.parseInt(taskSplitted[0]);
//                    String goldenHash = taskSplitted[1];
                    }
                } catch (Exception e){
                    e.printStackTrace();
                }
                i++;
            }
            
            if(found == true) {
                String jobHash = jobs.get(i);
                String partitionID = partitions.get(j);
                String password = null;
				currentJob = jobHash + ":" + partitionID;
				
				//set  myPath data to currentJob
				try{ 
					zk.setData(myPath, currentJob.getBytes(), -1);

				} catch (Exception e){
                    e.printStackTrace();
                }                


                try {
                    System.out.println("Sending partition " + partitionID);
                    fsOut.writeObject(partitionID);
                } catch (Exception e) {
                    System.out.println("Primary File Server has disconnected");
                    // watcher will update info when backup becomes the new boss
                    while (zkc.exists(fsPath, watcher) == null){}
                    try {Thread.sleep(1000);} catch (Exception f){}
                    
                        
                    try{    
                        byte[] data = zk.getData(fsPath, watcher, stat);
                        String hostname = new String(data).split(":")[0];
                        int port = Integer.parseInt(new String(data).split(":")[1]);
                        worker.connectToFS(hostname, port);
                        fsOut.writeObject(partitionID);
                    }catch (Exception f){}
                }
                


                ArrayList<String> dictPartition = null;
                try { dictPartition = (ArrayList<String>) fsIn.readObject();} catch (Exception e ){}    
                
                try {
                    MessageDigest md5 = MessageDigest.getInstance("MD5");
                    for (String word : dictPartition) {
                        BigInteger hashInt = new BigInteger(1, md5.digest(word.getBytes()));
                        String hash = hashInt.toString(16);
                        while (hash.length() < 32) {
                            hash = "0" + hash;
                        }
                        if (hash.equals(jobHash)) {
                            password = word;
                            break;
                        }   
                    }
                } catch (NoSuchAlgorithmException nsae) {
                    // ignore
                }
                
                String path = jobPath + "/" + jobHash + "/" + partitionID;
                System.out.println("Finished working on path " + path);
                stat = zkc.exists(path, watcher);
                if(stat != null) try{zk.delete(path, -1);}catch (Exception e){}
                

                List<String> partitionsLeft = null;
                try { partitionsLeft = zk.getChildren(jobPath + "/" + jobHash, watcher);}catch (Exception e){};

                if(partitionsLeft.size()==0){
                    if(password == null) {
                        stat = zkc.exists(jobPath + "/" + jobHash, watcher);
                        if(stat != null) try{zk.delete(jobPath + "/" + jobHash, -1);}catch (Exception e){}

                        try { stat = zk.exists(finishedPath + "/" + jobHash, watcher);}catch (Exception e){}
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

                if(password != null) {
                    try { stat = zk.exists(finishedPath + "/" + jobHash, watcher);}catch (Exception e){}
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
                    //delete job directory for hash
                    while(partitionsLeft.size() > 0){
                        try { zk.delete(jobPath + "/" + jobHash + "/" + partitionsLeft.get(0),-1); }catch (Exception e){}
                        partitionsLeft.remove(0);
                    }
                    try { zk.delete(jobPath + "/" + jobHash,-1); }catch (Exception e){}
                }    
            }
        }
    }
}
        

        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        


