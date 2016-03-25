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

public class JobHandlerThread extends Runnable{

	static Socket socket = null;
	static ZkConnector zkc = null;
	static String jobPath = "/jobs";
	static String finishedPath = "/finished";
	static Watcher watcher = null;
	static String hash = null;
	static 

	public JobHandlerThread(Socket s, String connection){
		this.socket = s;
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
                        
                            } };
	}

	void createNodes(){
		Stat stat = zkc.exists(jobPath, watcher);
		if(stat == null){
			System.out.println("Creating " + jobPath);
			Code ret = zkc.create(
                        jobPath,         // Path of znode
                        null,           // Data not needed.
                        CreateMode.PERSISTENT   // Znode type, set to PERSISTENT.
                        );
			if (ret == Code.OK) {
				System.out.println("Created jobs");
			}
		}

		stat = zkc.exists(finishedPath, watcher);
		if(stat == null){
			System.out.println("Creating " + finishedPath);
			Code ret = zkc.create(
                        finishedPath,         // Path of znode
                        null,           // Data not needed.
                        CreateMode.PERSISTENT   // Znode type, set to PERSISTENT.
                        );
			if (ret == Code.OK) {
				System.out.println("Created jobs");
			}
		}

		String hashPath = jobPath + "/" + hash;
		stat = zkc.exists(hashPath, watcher);
		if(stat == null){
			System.out.println("Creating " + hashPath);
			Code ret = zkc.create(
                        hashPath,         // Path of znode
                        null,           // Data not needed.
                        CreateMode.PERSISTENT   // Znode type, set to PERSISTENT.
                        );
			if (ret == Code.OK) {
				System.out.println("Created job for "+ hashPath);
			}
		}

		int i; //partition number = i
		for(int i=0; i<100; i++){
			taskPath = jobPath + "/" + hash + "/" + i;
			stat = zkc.exists(taskPath, watcher);
			if (stat == null){
				System.out.println("Creating " + taskPath);
			Code ret = zkc.create(
                        taskPath,         // Path of znode
                        null,           // Data not needed.
                        CreateMode.PERSISTENT   // Znode type, set to PERSISTENT.
                        );
			if (ret == Code.OK) {
				System.out.println("Created task " + taskPath);
				}
			}
		}


	}


	public void run(){
		try{
			
			clientOut = new ObjectOutputStream(socket.getOutputStream());
    		clientIn = new ObjectInputStream(socket.getInputStream());
    		
    		//get request String
    		String request = (String)clientIn.readObject();

    		String cmd = request.split(":")[0];
    		hash = request.split(":")[1]; 

    		if(cmd.equals("job")){
    			Stat stat = zkc.exists(jobPath + "/" + hash);
    			if (stat == null){
    				createNodes();
    				String message = "Job submitted succesfully.";
    				clientOut.writeObject(message);

    			}
    			else{
    				String message = "Job in progress."
    				clientOut.writeObject(message);
    			}

    		}
    		else(cmd.equals("status")){
    			
    		}


    	}catch (IOException e) {
		e.printStackTrace();
	} catch (ClassNotFoundException e) {
		e.printStackTrace();
	}
