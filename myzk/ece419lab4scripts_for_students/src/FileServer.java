import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.Watcher.Event.EventType;

import java.net.*;
import java.io.*;
import java.util.*;

public class FileServer {
    
    String fileServerPath = "/FileServer";
    Watcher watcher;
    static ZooKeeper zk = null;
    static ZkConnector zkc = null;
    static String connection = null;
    static String hosts = null;
    static String connectionData = null;
    static int port = 9001;
    static ServerSocket serverSocket = null;
    static ArrayList<String> dictionary = null;
    static String file = null;


    public FileServer() {
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
    

    private void checkpath() {
        Stat stat = zkc.exists(fileServerPath, watcher);
        if (stat == null) {
            //send connection data to zookeeper for clientdriver lookup
            connectionData = InetAddress.getLocalHost().getHostName() + ":" + port;

            loadDictionary(path);

            System.out.println("Creating " + fileServerPath);
            Code ret = zkc.create(
                        myPath,         // Path of znode
                        connectionData,           // Data not needed.
                        CreateMode.EPHEMERAL   // Znode type, set to EPHEMERAL.
                        );
            if (ret == Code.OK)System.out.println("Primary File Server.");

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
        if(path.equalsIgnoreCase(fileServerPath)) {
            if (type == EventType.NodeDeleted) {
                System.out.println(fileServerPath + " deleted! Let's go!");       
                checkpath(); // try to become the boss
            }
            if (type == EventType.NodeCreated) {
                System.out.println(fileServerPath + " created!");       
                try{ Thread.sleep(5000); } catch (Exception e) {}
                checkpath(); // re-enable the watch
            }
        }
    }


    private void loadDictionary(){
        dictionary = new ArrayList();
        try(BufferedReader br = new BufferedReader(new FileReader(file))){
            String line;
            while((line=br.readLine()) != null){
                dictionary.add(line);
            }
        }

    }

    public static void main(String[] args) {
      
        if (args.length != 2) {
            System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. FileServer zkServer:clientPort <Path to Dictionary>");
            return;
        }
        else{
            hosts = args[0];
            file = args[1];
        }


        FileServer fS= new FileServer();

        System.out.println("Sleeping...");
        try {
            Thread.sleep(5000);
        } catch (Exception e) {}
        
        fS.checkpath();

        while(true){
            Socket s = serverSocket.accept();
            new FileServerThread(s, hosts, dictionary).run();
        }

    }

    

}