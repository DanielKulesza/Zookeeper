import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.Watcher.Event.EventType;

import java.io.IOException;
import java.io.net.Socket;
import java.io.net.ServerSocket;

public class JobTracker {
    
    String jobTrackerPath = "/JobTracker";
    Watcher watcher;
    static ZooKeeper zk = null;
    static ZkConnector zkc = null;
    static String connection = null;
    static String task_path = "/tasks"
    static String hosts = null;
    static String connectionData = null;
    static int port = 9000;
    static ServerSocket serverSocket = null;



    public JobTracker() {
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



        //Create server socket for clients
        connectionData =                     


    }
    
    private void checkpath() {
        Stat stat = zkc.exists(jobTrackerPath, watcher);
        if (stat == null) {
            //send connection data to zookeeper for clientdriver lookup
            connectionData = InetAddress.getLocalHost().getHostName() + ":" + port;

            System.out.println("Creating " + jobTrackerPath);
            Code ret = zkc.create(
                        myPath,         // Path of znode
                        connectionData,           // Data not needed.
                        CreateMode.EPHEMERAL   // Znode type, set to EPHEMERAL.
                        );
            if (ret == Code.OK)System.out.println("Primary Job Tracker.");

            //Create server socket for communication with client driver 
            try{
               serverSocket = new ServerSocket(port);
            }catch (Exception e){
                System.err.println(e);
                System.exit(1);
            }
        } 
    }

    private void handleEvent(WatchedEvent event) {
        String path = event.getPath();
        EventType type = event.getType();
        if(path.equalsIgnoreCase(jobTrackerPath)) {
            if (type == EventType.NodeDeleted) {
                System.out.println(jobTrackerPath + " deleted! Let's go!");       
                checkpath(); // try to become the boss
            }
            if (type == EventType.NodeCreated) {
                System.out.println(jobTrackerPath + " created!");       
                try{ Thread.sleep(5000); } catch (Exception e) {}
                checkpath(); // re-enable the watch
            }
        }
    }

    public static void main(String[] args) {
      
        if (args.length != 1) {
            System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. JobTracker zkServer:clientPort");
            return;
        }
        else{
            hosts = args[0];
        }


        JobTracker jT = new jT();
        jT.initListeners(); 
 
        System.out.println("Sleeping...");
        try {
            Thread.sleep(5000);
        } catch (Exception e) {}
        
        jT.checkpath();

        while(true){
            Socket s = serverSocket.accept();
            new JobHandlerThread(s, hosts).run();
        }

    }

    

}