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
    private static ObjectOutputStream jobTrackerOut = null;
    private static ObjectInputStream jobTrackerIn = null;
    private static String command = null;
    private static String hash = null;


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

        //System.out.println("Client connected to Zookeeper.");                   
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
            this.connectJobTracker();

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

           zkc.close();
           jobTrackerSocket.close();

           System.exit(0);

        }catch (Exception e){
            e.printStackTrace();
        }
        }

    }


    private void connectJobTracker(){
        Stat stat = null;
        try{
    	   stat = zk.exists(jobTrackerPath, watcher);
           System.out.println("STAT ------------------------------" + stat);
           while(stat == null){
            System.out.println("Job Tracker is not running");
            stat = zk.exists(jobTrackerPath, watcher);
            }



    	    byte[] data = zk.getData(jobTrackerPath,watcher,stat);
    	    String hostName = new String(data).split(":")[0];
    	    String port = new String(data).split(":")[1];

            System.out.println("hostname ----------------------" + hostName);
            System.out.println("port ----------------------" + port);

    		jobTrackerSocket = new Socket(hostName, Integer.parseInt(port));
    		jobTrackerOut = new ObjectOutputStream(jobTrackerSocket.getOutputStream());
    		jobTrackerIn = new ObjectInputStream(jobTrackerSocket.getInputStream());

            //System.out.println("Client connected to JobTracker.");

    	}catch (Exception e){
            e.printStackTrace();
    	}
    }


    public static void main(String[] args) {


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

           zkc.close();
           jobTrackerSocket.close();

        }catch (Exception e){
            e.printStackTrace();
        }
    }
}