import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.WatchedEvent;


import java.io.IOException;
import java.net.Socket;
import java.net.ServerSocket;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.util.*;

public class FileServerThread implements Runnable{

	static Socket socket = null;
	static ZkConnector zkc = null;
	static Watcher watcher = null;
	static ObjectOutputStream workerOut = null;
	static ObjectInputStream workerIn = null;
	static ArrayList<String> dict = null;
	static int size = 0;
	static int numPartitions = 100;
	static int startIndex = 0;
	static int endIndex = 0;
	static int partitionSize= 0;
	static String connection =  null; 


	public FileServerThread(Socket s, String connection, ArrayList<String> dictionary){
		this.socket = s;
		this.connection = connection;
		zkc = new ZkConnector();
		try {
            zkc.connect(connection);
        } catch(Exception e) {
            System.out.println("Zookeeper connect "+ e.getMessage());
        }

        watcher = new Watcher() { // Anonymous Watcher
                            @Override
                            public void process(WatchedEvent event) {
                                handleEvent(event);
                        
                            } };


        try{
        	
        }catch (Exception e){

        }

        dict = dictionary;
        size = dict.size();
        partitionSize = size/numPartitions;

	}

	private void handleEvent(WatchedEvent event) {
  		String path = event.getPath();
        EventType type = event.getType();
        if (type == EventType.NodeCreated) {

        }
    }	

	


	


	public void run(){


		try{
			ObjectOutputStream workerOut = new ObjectOutputStream(this.socket.getOutputStream());
            ObjectInputStream workerIn = new ObjectInputStream(this.socket.getInputStream());
			while(true){
				//get request String


				String data = (String)workerIn.readObject();
				System.out.println(data);

				int partitionId = Integer.parseInt(data);

				if (partitionId+1 == numPartitions){
					startIndex = (partitionId-1)*partitionSize;
					endIndex = size-1;
				}
				else{
					startIndex = partitionId*partitionSize;
					endIndex = (partitionId+1)*partitionSize - 1;
				}
			
				System.out.println("Sending out partitionId " + partitionId + " with indices " + startIndex + "-" + endIndex + "to " + socket);

				ArrayList<String> partition = new ArrayList();

				int i = 0;
				for(i = startIndex; i<=endIndex; i++){
					partition.add(dict.get(i));
				}

				workerOut.writeObject(partition);

			}

    	}catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
}