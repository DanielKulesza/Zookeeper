import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.KeeperException;



import java.io.IOException;
import java.net.Socket;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;

public class ClientDriver{


	String jobTrackerPath = "/JobTracker";

    static Watcher watcher = null;
    static ZooKeeper zk = null;
    static ZkConnector zkc = null;
    static String connection = null;
    static Socket jobTrackerSocket = null;
    static ObjectOutputStream jobTrackerOut = null;
    static ObjectInputStream jobTrackerIn = null;

    public ClientDriver(){

 		zkc = new ZkConnector();
 		try{
 			zkc.connect(connection);
 		}catch(Exception e) {
            System.out.println("Zookeeper connect "+ e.getMessage());
        }

        zk = zkc.getZooKeeper();

        watcher = new Watcher() { // Anonymous Watcher
                            @Override
                            public void process(WatchedEvent event) {
                                handleEvent(event);
                        
                            } };


	}

  	private void handleEvent(WatchedEvent event) {
  		String path = event.getPath();
        EventType type = event.getType();
        if(path.equalsIgnoreCase(jobTrackerPath)) {
        	//connectJobTracker if jobtracker deleted or created
        }
        if (type == EventType.NodeCreated) {

        }
        if (type == EventType.NodeDeleted){

        }

    }


    private void connectJobTracker(){
        Stat stat = null;
        try{
    	   stat = zk.exists(jobTrackerPath, watcher);
           if(stat == null){
            System.err.println("Job Tracker is not running");
            System.exit(1);
            }



    	    byte[] data = zk.getData(jobTrackerPath,watcher,stat);
    	    String hostName = new String(data).split(":")[0];
    	    String port = new String(data).split(":")[1];

    		jobTrackerSocket = new Socket(hostName, Integer.parseInt(port));
    		jobTrackerOut = new ObjectOutputStream(jobTrackerSocket.getOutputStream());
    		jobTrackerIn = new ObjectInputStream(jobTrackerSocket.getInputStream());

    	}catch (Exception e){

    	}
    }


    public static void main(String[] args) {

        String command = null;
        String hash = null;

    	if(args.length != 3){
    		System.err.println("Missing Arguments");
            System.exit(1);
    	}
    	else{
    		connection = args[0];
    		command = args[1];
    		hash = args[2];	
    	}

    	ClientDriver cD =  new ClientDriver();
    	cD.connectJobTracker();


        try{
        	if(command.equals("job")){
        		String job = new String("job:" + hash);
        		jobTrackerOut.writeObject(job);
        	}
        	if(command.equals("status")){
        		String status = new String("status:" + hash);
        		jobTrackerOut.writeObject(status);

        	}

    	   String output = (String)jobTrackerIn.readObject();
    	   System.out.println(output);
        }catch (IOException e){

        }catch (ClassNotFoundException e){

        }

    }
}