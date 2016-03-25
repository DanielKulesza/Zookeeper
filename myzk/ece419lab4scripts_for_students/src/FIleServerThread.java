import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.Watcher.Event.EventType;

import java.io.IOException;
import java.net.Socket;
import java.net.ServerSocket;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.util.*;

public class FileServerThread extends Runnable{

	static Socket socket = null;
	static ZkConnector zkc = null;
	static Watcher watcher = null;
	static ObjectOutputStream workerOut = null;
	static ObjectInputStream workerIn = null;
	static ArrayList<String> dict = null;
	static int size = 0;
	static int num_partitions = 100;
	static int startIndex = 0;
	static int endIndex = 0;
	static int partition_size= 0;


	public FileServerThread(Socket s, String connection, ArrayList<String> dictionary){
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

        socket = s;
        workerOut = new ObjectOutputStream(socket.getOutputStream());
        workerIn = new ObjectInputStream(socket.getInputStream());
        dict = dictionary;
        size = dict.size();
        partition_size = size/num_partitions;

	}

	private void handleEvent(WatchedEvent event) {
  		String path = event.getPath();
        EventType type = event.getType();
        if(path.equalsIgnoreCase(myPath)) {

        }
        if (type == EventType.NodeCreated) {

        }
    }	

	


	


	public void run(){
		try{
			while(true){
				//get request String
				String data = (String)workerIn.readObject();
				Int partitionId = Integer.parseInt(data);

				if (partitionId+1 == num_partitions){
					startIndex = (partitionId-1)*partitionsize;
					endIndex = size-1;
				}
				else{
					startIndex = partitionId*partition_size;
					endIndex = (partitionId+1)*partition_size - 1;
				}
			
				System.out.println("Sending out partitionId " + partitionId + " with indices " + startIndex + "-" + endIndex + "to " + connection);

				ArrayList<String> partition = new ArrayList<String>;
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