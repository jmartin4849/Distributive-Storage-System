package app_kvECS;


import ecs.ECSNode;
import org.apache.log4j.Level;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.ExecutionException;


public class ECS {
    public NavigableMap<String, ECSNode> allNodes = new TreeMap<>();
    public NavigableMap<String, ECSNode> activeNodes = new TreeMap<>();
    public ZooKeeper zk;
    public boolean creating = false;

    public ECS(){
        try{
            new logger.LogSetup("logs/ecs.log", Level.OFF);
            Path fileName = Paths.get("ecs.config");
            String actual = Files.readString(fileName);
            String[] lines = actual.split("\\r?\\n");
            for (String line : lines) {
                String[] temp = line.split("\\s+");

                String toBeHashed = temp[1] + ":" + temp[2];

                MessageDigest md = MessageDigest.getInstance("MD5");
                md.update(toBeHashed.getBytes());
                byte[] digest = md.digest();
                StringBuilder sb = new StringBuilder();
                String bruv = "";
                for (int i = 0; i < digest.length; ++i) {
                    sb.append(Integer.toHexString((digest[i] & 0xFF) | 0x100).substring(1,3));
                }
                System.out.println(sb.toString());
                String[] bruh = {null, sb.toString()};
                ECSNode ecs = new ECSNode(temp[0], temp[1], Integer.parseInt(temp[2]), bruh);
                allNodes.put(sb.toString(), ecs);
            }
        } catch(Exception e){
            System.out.println(e.toString());
        }

        try {

            zk = new ZooKeeper("127.0.0.1:2181", 5000, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    try {
                        switch (event.getType()) {
                            case NodeDeleted:
				System.out.println("Entered Node Deleted");
				System.out.println("NODE HAS BEEN DELETED");
				String crashed = null;
				for(Map.Entry<String,ECSNode> node: activeNodes.entrySet()) {
					if(zk.exists("/servers/" + node.getValue().getNodeName(), true) == null){
						System.out.println("NODEE CRASHHHHED");
						String crashedKey = node.getKey();
						crashed = crashedKey;
						Map.Entry<String,ECSNode> successor = activeNodes.higherEntry(crashedKey);
						if(successor == null){
							successor = activeNodes.firstEntry();
						}
						Map.Entry<String,ECSNode> lowerNode1 = activeNodes.lowerEntry(crashedKey);
						if(lowerNode1 == null){
							lowerNode1 = activeNodes.lastEntry();
						}
						Map.Entry<String,ECSNode> lowerNode2 = activeNodes.lowerEntry(lowerNode1.getKey());
						if(lowerNode2 == null){
							lowerNode2 = activeNodes.lastEntry();
						}
						Map.Entry<String,ECSNode> Successor2 = activeNodes.higherEntry(successor.getKey());
						if(Successor2 == null){
							Successor2 = activeNodes.firstEntry();
						}
						Map.Entry<String,ECSNode> Successor3 = activeNodes.higherEntry(Successor2.getKey());
						if(Successor3 == null){
							Successor3 = activeNodes.firstEntry();
						}
						String successor_port = String.format("%d", successor.getValue().getNodePort());
						String successor2_port = String.format("%d", Successor2.getValue().getNodePort());
						String successor3_port = String.format("%d", Successor3.getValue().getNodePort());
						transfer_reps(lowerNode2.getValue().getNodeName(), lowerNode2.getValue().getNodeHashRange()[0], lowerNode2.getValue().getNodeHashRange()[1], successor.getValue().getNodeHost(), successor_port);
						transfer_reps(lowerNode1.getValue().getNodeName(), lowerNode1.getValue().getNodeHashRange()[0], lowerNode1.getValue().getNodeHashRange()[1], Successor2.getValue().getNodeHost(), successor2_port);
						transfer_reps(successor.getValue().getNodeName(), node.getValue().getNodeHashRange()[0], node.getValue().getNodeHashRange()[1], Successor3.getValue().getNodeHost(), successor3_port);
						
					}
				}
				activeNodes.remove(crashed);
				for( String  active_node : activeNodes.keySet()){
                                                    Map.Entry<String, ECSNode> pred = activeNodes.lowerEntry(active_node);
                                                    if (pred == null){
                                                        pred = activeNodes.lastEntry();
                                                    }
						    System.out.println("Active Node size:");
						    System.out.println(activeNodes.keySet().size());
                                                    ECSNode temp2 = activeNodes.get(active_node);
                                                    temp2.nodeHashRange[0] = pred.getKey();
                                                    activeNodes.put(active_node, temp2);
						    System.out.println("Active Node size Now:");
						    System.out.println(activeNodes.keySet().size());
                                                
                                                }
						BalanceServer();
                                                sendMetadata();
				
                            case NodeChildrenChanged:
				System.out.println("NODE CHILD CHANGED");
                                if(zk.exists("/servers", true) != null){
                                    Set<String> list = new HashSet<>(zk.getChildren("/servers", true));
                                    for (String s : list) {
                                        if(zk.exists("/servers/" + s, true)!=null){
                                            byte[] temp = zk.getData("/servers/" + s, true, zk.exists("/servers/" + s, true));
                                            String status = new String(temp);
                                            if (status.equals("waitingMeta")) {
                                                for(Map.Entry<String,ECSNode> node: allNodes.entrySet()){
                                                    if(node.getValue().getNodeName().equals(s)){
                                                        String new_node_key = node.getKey();
                                                        ECSNode new_node = node.getValue();
                                                        activeNodes.put(new_node_key, new_node);
							System.out.println("here!!!!!");
							System.out.println(s);
							//Here he is adding to active but not removing from all
                                                    }
                                                }
                                                for( String  active_node : activeNodes.keySet()){
                                                    Map.Entry<String, ECSNode> pred = activeNodes.lowerEntry(active_node);
                                                    if (pred == null){
                                                        pred = activeNodes.lastEntry();
                                                    }
						    System.out.println("Active Node size:");
						    System.out.println(activeNodes.keySet().size());
                                                    ECSNode temp2 = activeNodes.get(active_node);
                                                    temp2.nodeHashRange[0] = pred.getKey();
                                                    activeNodes.put(active_node, temp2);
						    System.out.println("Active Node size Now:");
						    System.out.println(activeNodes.keySet().size());
                                                
                                                }
						BalanceServer();
                                                sendMetadata(s);
                                            }
                                        }
                                    }
                                }
				System.out.println("Exited NodeChildrenChanged");
                                break;
                            case NodeDataChanged:
				System.out.println("Entered Node Data Changed");
                                if(zk.exists(event.getPath(), true) !=null) {
                                    byte[] temp = zk.getData(event.getPath(), true, zk.exists(event.getPath(), true));

                                    String data = new String(temp);
                                    String status = data.split("\\r?\\n")[0];
                                    System.out.println(status);
                                    if(status.equals("metadata")){
                                        break;
                                    }
                                    String node_name = event.getPath().split("/")[2];
                                    if (status.equals("started")) {
					System.out.println("Started " + node_name);
                                        Set<Map.Entry<String, ECSNode>> node_set = activeNodes.entrySet();
                                        String node_key = null;
					String node_port = null;
					String node_host = null;
					String start_key = null;
					
                                        for (Map.Entry<String, ECSNode> possible_node : node_set) {
                                            if (possible_node.getValue().getNodeName().equals(node_name)) {
                                                node_key = possible_node.getKey();
						node_port = String.format("%d", possible_node.getValue().getNodePort());
						node_host = possible_node.getValue().getNodeHost();
						start_key = activeNodes.lowerKey(possible_node.getKey());
						
                                            }		
                                        }
					if(start_key == null){
						start_key = activeNodes.lastKey();
					}
                                        Map.Entry<String, ECSNode> successor = activeNodes.higherEntry(node_key);
                                        if (successor == null) {
                                            successor = activeNodes.firstEntry();
                                        }
					//implement lock
                                        String successor_name = successor.getValue().getNodeName();
					
					System.out.println("Transfering from" + successor_name + "to" + node_port);
                                        transfer(successor_name, start_key, node_key, node_host, node_port);
					
					 Map.Entry<String, ECSNode> pred1 = activeNodes.lowerEntry(node_key);
					if(pred1 == null){
						pred1 = activeNodes.lastEntry();
					}
					Map.Entry<String, ECSNode> pred2 = activeNodes.lowerEntry(pred1.getKey());
					if(pred2 == null){
						pred2 = activeNodes.lastEntry();
					}
					String sKey = activeNodes.lowerKey(pred2.getKey());
					if(sKey == null){
						sKey = activeNodes.lastKey();
					}
					if(!pred1.getValue().getNodeName().equals(node_name)){
						System.out.print("Entered transfer reps1");
						transfer_reps(pred1.getValue().getNodeName(), pred2.getKey(), pred1.getKey(), node_host, node_port);
					}
					if(!pred2.getValue().getNodeName().equals(node_name)){
						System.out.print("Entered transfer reps2");
						transfer_reps(pred2.getValue().getNodeName(), sKey, pred2.getKey(), node_host, node_port);
					}
                                    } else if (status.equals("transferred")) {
                                        sendMetadata();
//                                        unlock(node_name);
                                    } else if (status.equals("deleted")) {
					System.out.println("Entering deleted!!!!!!");
					if(event.getPath().split("/").length < 3){
		                            break;
		                        }
		                        Set<Map.Entry<String, ECSNode>> set = activeNodes.entrySet();
		                        String to_remove = null;
					ECSNode inactive_node = null;
		                        for(Map.Entry<String, ECSNode> s : set){
		                            if(s.getValue().getNodeName().equals(node_name)){
		                                to_remove = s.getKey();
						inactive_node = s.getValue();
		                            }
		                        }
		                        if(inactive_node == null){
						System.out.println("NODE TO BE DELETED NOT FOUND! FATAL!");
					}
					System.out.println("1");
		                        activeNodes.remove(to_remove);
					Map.Entry<String, ECSNode> successor = activeNodes.higherEntry(to_remove);
					if(successor == null){
						successor = activeNodes.firstEntry();
					}
					System.out.println("2");
					Map.Entry<String, ECSNode> pred1 = activeNodes.lowerEntry(to_remove);
					if(pred1 == null){
						pred1 = activeNodes.lastEntry();
					}
					System.out.println("3");
					Map.Entry<String, ECSNode> pred2 = null;
					if(pred1 != null){
						pred2 = activeNodes.lowerEntry(pred1.getKey());
						if(pred2 == null){
							pred2 = activeNodes.lastEntry();
						}
					}
					System.out.println("4");
					String start = inactive_node.getNodeHashRange()[0];
					String end = inactive_node.getNodeHashRange()[1];
					String host = successor.getValue().getNodeHost();
					String port = String.format("%d", successor.getValue().getNodePort());
					String name = inactive_node.getNodeName();
					String succ_name = successor.getValue().getNodeName();
					System.out.println("name is: " + name);
					
		                        for( String  active_node : activeNodes.keySet()){
		                            Map.Entry<String, ECSNode> pred = activeNodes.lowerEntry(active_node);
		                            if (pred == null){
		                                pred = activeNodes.lastEntry();
		                            }
		                            ECSNode temp2 = activeNodes.get(active_node);
		                            temp2.nodeHashRange[0] = pred.getKey();
		                            activeNodes.put(active_node, temp2);
		                        }
					
					delete(name, succ_name, start, end, host, port);
					
					if(pred1 != null){
						System.out.println("Got passed delete");
						Map.Entry<String, ECSNode> first = activeNodes.higherEntry(pred1.getKey());
						if(first == null){
							first = activeNodes.firstEntry();	
						}
						Map.Entry<String, ECSNode> second = activeNodes.higherEntry(first.getKey());
						if(second == null){
							second = activeNodes.firstEntry();
						}
						String second_port = String.format("%d", second.getValue().getNodePort());
						String first_port = String.format("%d", first.getValue().getNodePort());
						String pred1_port = String.format("%d", pred1.getValue().getNodePort());
						transfer_reps(pred1.getValue().getNodeName(), pred1.getValue().getNodeHashRange()[0], pred1.getValue().getNodeHashRange()[1], second.getValue().getNodeHost(), second_port);
						if(pred2 != null){
							transfer_reps(pred2.getValue().getNodeName(), pred2.getValue().getNodeHashRange()[0], pred2.getValue().getNodeHashRange()[1], first.getValue().getNodeHost(), first_port);
							transfer_reps(pred1.getValue().getNodeName(), pred2.getValue().getNodeHashRange()[0], pred1.getValue().getNodeHashRange()[1], first.getValue().getNodeHost(), first_port);
							transfer_reps(pred2.getValue().getNodeName(), pred2.getValue().getNodeHashRange()[0], pred2.getValue().getNodeHashRange()[1], pred1.getValue().getNodeHost(), pred1_port);
						}
					}
					
					System.out.println("Exiting deleted");
				

		                        
                               
                                    }else if(status.equals("final delete")){
					
					sendMetadata();
					zk.delete("/servers/" + node_name, zk.exists("/servers/" + node_name, false).getVersion());
				    }
                                }
			    System.out.println("Exited Node Data Changed");
                            break;
                        default:
                            break;
                        }

                        List<String> node_list;
                        if(zk.exists("/servers", false) != null){
                            node_list = zk.getChildren("/servers", true);
                            for( String node : node_list ){
                                zk.exists("/servers/" + node, true);
                            }
                        }
                    } catch(Exception e){
                        e.printStackTrace();
                    }
                }
            });
            byte[] data = "".getBytes(StandardCharsets.UTF_8);
            if(zk.exists("/servers",true) == null){
                zk.create("/servers", data,  ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch(Exception e){
            e.printStackTrace();
        }
    }

    public int findLengthHashRange(String start, String end) {
        int MAX_STRING_COMP_LEN = 6;
        String digits = "0123456789abcdef"; 
        start = start.toLowerCase();
        end = end.toLowerCase();
        int val_start = 0;
        int val_end = 0;
        int buffer_legnth = 0;
        for (int i = 0; i < MAX_STRING_COMP_LEN; i++) {
            buffer_legnth = 16*buffer_legnth + 15;
        }
        for (int i = 0; i < MAX_STRING_COMP_LEN; i++) {  
            char c = start.charAt(i);
            int d = digits.indexOf(c);
            val_start = 16*val_start + d;
        }
        for (int i = 0; i < MAX_STRING_COMP_LEN; i++) {  
            char c = end.charAt(i);
            int d = digits.indexOf(c);
            val_end = 16*val_end + d;
        }
        int val_difference = val_end - val_start;
        if (val_difference >= 0) {
            return val_difference;
        } else {
            // val_start > val_end
            return buffer_legnth - (val_start - val_end);
        }
    }

    public String findMiddle(String start, String end) {
        int MAX_STRING_COMP_LEN = 6;
        String digits = "0123456789abcdef"; 
        start = start.toLowerCase();
        end = end.toLowerCase();
        int val_start = 0;
        int val_end = 0;
        int buffer_legnth = 0;
        for (int i = 0; i < MAX_STRING_COMP_LEN; i++) {
            buffer_legnth = 16*buffer_legnth + 15;
        }
        for (int i = 0; i < MAX_STRING_COMP_LEN; i++) {  
            char c = start.charAt(i);
            int d = digits.indexOf(c);
            val_start = 16*val_start + d;
        }
        for (int i = 0; i < MAX_STRING_COMP_LEN; i++) {  
            char c = end.charAt(i);
            int d = digits.indexOf(c);
            val_end = 16*val_end + d;
        }
        int val_difference = val_end - val_start;
        if (val_difference < 0) {
            val_difference = buffer_legnth - (val_start - val_end);  // val_start > val_end
        }
        int val_middle = val_difference/2 + val_start;
        if (val_middle > buffer_legnth) {
            val_middle = val_middle - buffer_legnth;
        }
        int rem = 0;
        String hex="";
        char hexchars[]={'0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'};
        while(val_middle>0) {  
            rem = val_middle % 16;   
            hex = hexchars[rem] + hex;   
            val_middle = val_middle / 16;  
        }
        for (int i = 0; i < start.length()-MAX_STRING_COMP_LEN; i++) {
            hex = hex + '0';
        }
        return hex;  
    }

    /*
        Convert to 10-based number; find the middle

        Iterate 5/10 times;
        if something range is too small, findMiddle;
        while doing this, need to move data

    */

    public void BalanceServer() {
        int MAX_STRING_COMP_LEN = 6;
        int MAX_ITER = 5;
        int threshold_factor = 2;

        int buffer_legnth = 0;
        for (int i = 0; i < MAX_STRING_COMP_LEN; i++) {
            buffer_legnth = 16*buffer_legnth + 15;
        }
        int num_servers = activeNodes.size();
        int MIN_RANGE_THRESHOLD = buffer_legnth / (threshold_factor * num_servers); 
        
        String start;
		String end;
        String successor_end;
        String middle;
	int i = 0;

        if (num_servers <= 1) { return; }
        for (int n_iter = 0; n_iter < MAX_ITER; n_iter ++ ) {
	    System.out.println(i);
            Map.Entry<String, ECSNode> current_node = activeNodes.firstEntry();
            if (current_node == null) { return; }
            Map.Entry<String, ECSNode> successor_node = activeNodes.higherEntry(current_node.getKey());
            while (current_node != null) {
		System.out.println(current_node.getValue().getNodeName());
		System.out.println(successor_node.getValue().getNodeName());
		i++;
		System.out.println(i);
                start = current_node.getValue().getNodeHashRange()[0];
                end = current_node.getValue().getNodeHashRange()[1];
		String current_key = current_node.getKey();
		if(start == null){
			System.out.println("START IS NULL");
		}
		else{
			System.out.println("START IS NOT NULL");
		}
                if (findLengthHashRange(start, end) < MIN_RANGE_THRESHOLD) {
                    // need to do load balancing!! 
                    successor_end = successor_node.getValue().getNodeHashRange()[1];
		    System.out.println("SUCCESSOR IS: " + successor_end);
		    System.out.println("Start is: " + start);
                    middle = findMiddle(start, successor_end);
		    System.out.println("MIDDLE IS: " + middle);
		    
		    
                    // change the hash range
		    current_key = middle;
		    ECSNode current = current_node.getValue();
		    current.setNodeHashRange(current.getNodeHashRange()[0], middle);
		    activeNodes.put(middle, current);
		    activeNodes.remove(current_node.getKey());
		    
		    successor_node.getValue().setNodeHashRange(middle, successor_node.getKey());
                    // then move data
		    String current_port = String.format("%d", current.getNodePort());
		    transfer(successor_node.getValue().getNodeName(), current.getNodeHashRange()[0], current.getNodeHashRange()[1], current.getNodeHost(), current_port);
                }
		current_node = activeNodes.higherEntry(current_key);
		successor_node = activeNodes.higherEntry(successor_node.getKey());
		if (successor_node == null) { successor_node = activeNodes.firstEntry(); }
            }
            
        }

        System.out.println("BalanceServer FINISHED !!!");
        Map.Entry<String, ECSNode> current_node = activeNodes.firstEntry();
        if (current_node == null) { 
            System.out.println("BalanceServer FINISHED BUT current_node == null");
            return; 
        }
        int node_cnt = 0;
        while (current_node != null) {
            System.out.println("BalanceServer FINISHED; node_cnt = " + node_cnt);
            System.out.println("Node: host " + current_node.getValue().getNodeHost() + " port " + current_node.getValue().getNodePort());
            System.out.println("BalanceServer FINISHED; start " + current_node.getValue().getNodeHashRange()[0]);
            System.out.println("BalanceServer FINISHED; end   " + current_node.getValue().getNodeHashRange()[1]);
            current_node = activeNodes.higherEntry(current_node.getKey());
            node_cnt = node_cnt + 1;
        }
    }

    public boolean removeNode(String addr, String port){
        String toBeHashed = addr+ ":" + port;
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(toBeHashed.getBytes());
            byte[] digest = md.digest();
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < digest.length; ++i) {
                sb.append(Integer.toHexString((digest[i] & 0xFF) | 0x100).substring(1,3));
            }
            String key = sb.toString();
            System.out.println(key);
            ECSNode node = activeNodes.get(key);
            if (node == null) {
		System.out.println("GOT HERE!");
                return false;
            }
            Map.Entry<String, ECSNode> successor = activeNodes.higherEntry(key);
            if(successor == null){
                successor = activeNodes.firstEntry();
            }
	    String successor_key = successor.getKey();
	    String successor_host = successor.getValue().getNodeHost();
	    String successor_port = String.format("%d", successor.getValue().getNodePort());
            //delete(node.getNodeName(), node.getNodeHashRange()[0], node.getNodeHashRange()[1], successor_host, successor_key);
	    byte[] start = ("delete1\n" + successor_key + "\n" + successor_host + "\n" + successor_port).getBytes(StandardCharsets.UTF_8);
            zk.setData("/servers/"+ node.getNodeName(), start, zk.exists("/servers/"+ node.getNodeName(), true).getVersion());

            return true;
        } catch(Exception e){
            e.printStackTrace();
            return false;
        }
    }

    public ECSNode activateNode(){
	System.out.println("HERE IT IS:");
	System.out.println(allNodes.keySet().size());
        Set<String> set1 = allNodes.keySet();
        Set<String> set2 = activeNodes.keySet();
        //set1.removeAll(set2);
	System.out.println("NOW IT IS:");
	System.out.println(allNodes.keySet().size());
        if(set2.containsAll(set1)){
            System.out.println("No more nodes left to activate");
            return null;
        }
	String new_node_key;
	do{
        	new_node_key = (String) set1.toArray()[(int)((System.currentTimeMillis()* (Math.random()*200))% set1.toArray().length)];
	}
	while(set2.contains(new_node_key));

        ECSNode new_node = allNodes.get(new_node_key);
	System.out.println(new_node.getNodeName());


        Process proc;
        String script = "./script.sh";
        Runtime run = Runtime.getRuntime();
        try{
            String[] env = {"HOST=" + new_node.getNodeHost(), "PORT=" + new_node.getNodePort(), "NAME=" + new_node.getNodeName(),"ZK_ADDR=127.0.0.1", "ZK_PORT=2181"};
            proc = run.exec(script, env);
            ECSNode updated = allNodes.get(new_node_key);
            updated.proc = proc;
            allNodes.put(new_node_key, updated); //Something werid is happening here when allNodes is being put

        } catch (Exception e){
		System.out.println("Got HERE");
            e.printStackTrace();
        }


        return new_node;
    }

    public void shutdown(){
//        Set<String> set = activeNodes.keySet();
//        for(String s : set){
//            ECSNode bruh = activeNodes.get(s);
//            bruh.proc.destroy();
//            System.out.println(bruh.proc.isAlive());
//        }
        try{
            if(zk.exists("/servers", true) != null) {
                List<String> server_list = zk.getChildren("/servers", true);
                for (String s : server_list) {
                    zk.delete("/servers/" + s, zk.exists("/servers/" + s, true).getVersion());
                }
                zk.sync("/servers", new AsyncCallback.VoidCallback() {
                    @Override
                    public void processResult(int rc, String path, Object ctx) {

                    }
                }, null);
                zk.delete("/servers", zk.exists("/servers", true).getVersion());
            }
        } catch( Exception e){
            e.printStackTrace();
            System.out.println("If you got here the program is really broken.");
        }
    }
    public void transfer_reps(String s, String successor_start_key, String to, String to_host, String to_port){
        try{
	    /*
            StringBuilder to_send = new StringBuilder();
            for(Map.Entry<String, ECSNode> temp : activeNodes.entrySet()){
                to_send.append(temp.getValue().print());
            };
	    */
		
            byte[] start = ("tRep\n" + to + "\n" + successor_start_key + "\n" + to_host + "\n" + to_port).getBytes(StandardCharsets.UTF_8);
	    System.out.println("tRep\n" + to + "\n" + successor_start_key + "\n" + to_host + "\n" + to_port);
	    System.out.println("End of transfer messege");
            zk.setData("/servers/"+ s, start, zk.exists("/servers/"+ s, true).getVersion());
        } catch( Exception e){
            e.printStackTrace();
        }
	
    }

    public void transfer(String s, String successor_start_key, String to, String to_host, String to_port){
        try{
	    /*
            StringBuilder to_send = new StringBuilder();
            for(Map.Entry<String, ECSNode> temp : activeNodes.entrySet()){
                to_send.append(temp.getValue().print());
            };
	    */
            byte[] start = ("transfer\n" + to + "\n" + successor_start_key + "\n" + to_host + "\n" + to_port).getBytes(StandardCharsets.UTF_8);
	    System.out.println("transfer\n" + to + "\n" + successor_start_key + "\n" + to_host + "\n" + to_port);
	    System.out.println("End of transfer messege");
            zk.setData("/servers/"+ s, start, zk.exists("/servers/"+ s, true).getVersion());
        } catch( Exception e){
            e.printStackTrace();
        }
    }

    public void delete(String s, String succ_name, String range_start, String deleted_node_key, String to_host, String to_port){
        try{
	    sendMetadata(succ_name);
	    sendMetadata(s);
	    if(s == null){
		System.out.println("S IS NULLLLLLLLLLLLLLL");
	    }
	    System.out.println("GOT INSIDE DELETE");
	    System.out.println("delete2\n" + range_start + "\n" + deleted_node_key + "\n" + to_host + "\n" + to_port);
	    System.out.println("range start: " + range_start);
            byte[] start = ("delete2\n" + range_start + "\n" + deleted_node_key + "\n" + to_host + "\n" + to_port).getBytes(StandardCharsets.UTF_8);
            zk.setData("/servers/"+ s, start, zk.exists("/servers/"+ s, true).getVersion());
        } catch( Exception e){
            e.printStackTrace();
        }
    }

    public void start(){
        try{
            List<String> znode_list = zk.getChildren("/servers", true);
            for(String znode_name : znode_list){
                byte[] start = "start".getBytes(StandardCharsets.UTF_8);
                zk.setData("/servers/"+ znode_name, start, zk.exists("/servers/"+ znode_name, true).getVersion());
            }

        } catch( Exception e){
            e.printStackTrace();
        }
    }

    public void start(String s){
        try{
            byte[] start = "start".getBytes(StandardCharsets.UTF_8);
            zk.setData("/servers/"+ s, start, zk.exists("/servers/"+ s, true).getVersion());

        } catch( Exception e){
            e.printStackTrace();
        }
    }

    public void lock(String s){
        try{
            byte[] start = "lock".getBytes(StandardCharsets.UTF_8);
            zk.setData("/servers/"+ s, start, zk.exists("/servers/"+ s, true).getVersion());

        } catch( Exception e){
            e.printStackTrace();
        }
    }

    public void unlock(String s){
        try{
            StringBuilder to_send = new StringBuilder();
            for(Map.Entry<String, ECSNode> temp : activeNodes.entrySet()){
                to_send.append(temp.getValue().print());
            };
            byte[] start = ("unlock\n" + to_send).getBytes(StandardCharsets.UTF_8);
            zk.setData("/servers/"+ s, start, zk.exists("/servers/"+ s, true).getVersion());

        } catch( Exception e){
            e.printStackTrace();
        }
    }


    public void stop(){
        try{
            List<String> znode_list = zk.getChildren("/servers", true);
            for(String znode_name : znode_list){
                byte[] start = "stop".getBytes(StandardCharsets.UTF_8);
                zk.setData("/servers/"+ znode_name, start, zk.exists("/servers/"+ znode_name, true).getVersion());
            }

        } catch( Exception e){
            e.printStackTrace();
        }
    }

    void sendMetadata(){
        StringBuilder to_send = new StringBuilder();
        for(Map.Entry<String, ECSNode> temp : activeNodes.entrySet()){
            to_send.append(temp.getValue().print());
        }
        byte[] data = ( "metadata\n" + to_send.toString()).getBytes(); // Declare data
        try{
            zk.setData("/servers", data, zk.exists("/servers", false).getVersion());
        } catch(Exception e){
	    System.out.println("ZKSERVER ERROR DOESNT EXIST ERROR");
            System.out.println(e.getMessage()); //Catch error message
        }
	for(Map.Entry<String, ECSNode> temp : activeNodes.entrySet()){
            try{
		 zk.setData("/servers/" + temp.getValue().getNodeName(), data, zk.exists("/servers/" + temp.getValue().getNodeName(), false).getVersion());
	    } catch(Exception e){
		 System.out.println(e.getMessage()); //Catch error message
            }
        }
    }
    void sendMetadata(String s){
        StringBuilder to_send = new StringBuilder();
        for(Map.Entry<String, ECSNode> temp : activeNodes.entrySet()){
            to_send.append(temp.getValue().print());
        };
        byte[] data = ( "metadata\n" + to_send.toString()).getBytes(); // Declare data
        try{
            zk.setData("/servers/" + s, data, zk.exists("/servers/" + s, false).getVersion());
        } catch(Exception e){
            System.out.println(e.getMessage()); //Catch error message
        }
    }

}

