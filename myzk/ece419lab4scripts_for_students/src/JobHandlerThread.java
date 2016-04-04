import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.net.Socket;
import java.net.ServerSocket;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;

public class JobHandlerThread implements Runnable{

	static Socket socket = null;
	static ZkConnector zkc = null;
	static String jobPath = "/jobs";
	static String finishedPath = "/finished";
	static Watcher watcher = null;
	static String hash = null;
	private ObjectInputStream clientIn = null;
	private ObjectOutputStream clientOut = null;
	static ZooKeeper zk = null;
    static String connection = null;


	public JobHandlerThread(Socket s, String connection){
		this.socket = s;
        this.connection = connection;
		zkc = new ZkConnector();
		try {
            zkc.connect(connection);
        } catch(Exception e) {
            System.out.println("Zookeeper connect "+ e.getMessage());
        }

        zk = zkc.getZooKeeper();

        watcher = new Watcher() { // Anonymous Watcher
                            @Override
                            public void process(WatchedEvent event) {
                                handleEvent(event);
                        
                            } };

        System.out.println("Connected to Zookeeper " + connection);

        try{
            this.clientOut = new ObjectOutputStream(this.socket.getOutputStream());
            this.clientIn = new ObjectInputStream(this.socket.getInputStream());
        }catch (Exception e){
            e.printStackTrace();
        }
	}

	private void handleEvent(WatchedEvent event) {
  		String path = event.getPath();
        EventType type = event.getType();
        if(path.equalsIgnoreCase(jobPath)) {

        }
        if (type == EventType.NodeCreated) {

        }
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
		for(i=0; i<100; i++){
			String taskPath = jobPath + "/" + hash + "/" + i;
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
			

    		
    		//get request String
    		String request = (String)this.clientIn.readObject();

            System.out.println("Received request:  " + request);

    		String cmd = request.split(":")[0];
    		hash = request.split(":")[1]; 

            System.out.println(cmd);

    		if(cmd.equals("job")){
    			Stat stat = zkc.exists(jobPath + "/" + hash,watcher);
    			if (stat == null){
    				createNodes();
    				String message = "Job submitted succesfully.";
    				this.clientOut.writeObject(message);
                    System.out.println(message);

    			}
    			else{
                    createNodes();
    				String message = "In progress.";
    				this.clientOut.writeObject(message);
                    System.out.println(message);
    			}

    		}
    		else{
    			Stat stat = zkc.exists(jobPath + "/" + hash,watcher);
    			if (stat != null){
    				String message = "In progress";
    				this.clientOut.writeObject(message);
                    System.out.println(message);
    			}
    			else{
    				stat = zkc.exists(finishedPath + "/" + hash, watcher);
    				String failed = "Failed: ";
                    String password = "";
    				if (stat != null){
    					byte[] data = zk.getData(finishedPath + "/" + hash,watcher,stat);
    					
                        if (data != null) password = new String(data);
    					
                        if (password == ""){
    						String passNotFound = "Password not found";
    						this.clientOut.writeObject(failed + passNotFound);
                            System.out.println(failed + passNotFound);
    					}
    					else{
    						String passFound = "Password found: ";
                            System.out.println(passFound + password);
    						this.clientOut.writeObject(passFound + password);
                            System.out.println(passFound + password);
    					}
    				}	
    				else{

  						//implement "Failed to complete job"

    					String notFound = "Job not found";
    					this.clientOut.writeObject(failed + notFound);
                        System.out.println(failed + notFound);
    				}
    			}
    		}
            System.out.println("Sent message");
            zkc.close();
            this.socket.close();


    	}catch (Exception e) {
			e.printStackTrace();
		} 
	}
}
	