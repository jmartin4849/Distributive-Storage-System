package app_kvServer;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.UnknownHostException;
import client.KVStore;
import org.apache.zookeeper.data.Stat;
import shared.messages.KVMessage;

import app_kvECS.ECS;
import ecs.ECSNode;
import org.apache.zookeeper.*;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.json.JSONException;
import java.util.Timer;

import java.util.*;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class KVServer extends Thread implements IKVServer {
	/**
	 * Start KV Server at given port
	 * @param port given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed
	 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the cache
	 *           is full and there is a GET- or PUT-request on a key that is
	 *           currently not contained in the cache. Options are "FIFO", "LRU",
	 *           and "LFU".
	 */

	private static final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
	private int port;
    private static ServerSocket serverSocket;
    private boolean running;
	private static Logger log = Logger.getRootLogger();
	public NavigableMap<String, ECSNode> metadata = new TreeMap<>();
	private String serverName;
	private boolean stopped;
	private int cacheSize;
	private String strategy;
	private String filename;
	private String zk_addr;
	private String zk_port;
	private static ZooKeeper zk;
	private boolean connected = false;
	private boolean deleting = false;
	private JSONObject cleanedStorage;
	boolean writelocked;

	static Thread shutdownThread = new Thread(){
		public void run(){
			try {
				serverSocket.close();
				zk.close();
			} catch (IOException e) {
				log.error("Error! Cannot close socket on", e);
			} catch (InterruptedException e){
				log.error("Error closing zookeeper");
			}
		}
	};

	public KVServer(int port, int cacheSize, String strategy, String name, String zk_addr, String zk_port) {
		// TODO Auto-generated method stub
		System.out.println("TEsting Testing 123");
		this.writelocked = false;
		this.port = port;
		this.cacheSize = cacheSize;
		this.strategy = strategy;
		this.serverName = name;
		this.stopped = true;
		this.filename = name + ".json";
		this.zk_addr = zk_addr;
		this.zk_port = zk_port;
		this.cleanedStorage = null;
		this.start();
	}
	public void start_req(){
		this.stopped = false;
	}
	public void stop_req(){
		this.stopped = true;
	}
	public void shutDown_req(){
		kill();
	}
	public void lockWrite_req(){
		this.writelocked = true;

	}
	public void unLockWrite_req(){
		this.writelocked = false;

	}
	public void moveData_req(){
		//Don't know how to do atm

	}

	public boolean isWriteLocked(){
		return this.writelocked;
	}
	public boolean isServerStopped(){
		return this.stopped;
	}
	public boolean isResponsibleFor(String k){
		System.out.println(k);
		Map.Entry<String, ECSNode> s;
		String storedKey = null;
		try{
			MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(k.getBytes());
			byte[] digest = md.digest();
			StringBuilder sb = new StringBuilder();
			String bruv = "";
			for (int i = 0; i < digest.length; ++i) {
				sb.append(Integer.toHexString((digest[i] & 0xFF) | 0x100).substring(1,3));
			}
			log.fatal(sb.toString());
			storedKey = sb.toString();
		} catch( Exception e){
			e.printStackTrace();
		}
		System.out.println(storedKey);
		/*
		s = metadata.higherEntry(storedKey);
		if(s == null){
			s = metadata.firstEntry();
		}
		ECSNode targetServer = s.getValue();
		log.fatal(targetServer.getNodeName());
		if(targetServer.getNodeName().equals(this.serverName)){
			return true;
		}
		return false;
		*/
		log.fatal("KVServer isResponsibleFor starting to get messy");
		String string_key = null;
		for(Map.Entry<String,ECSNode> node: metadata.entrySet()){
			if(node.getValue().getNodeName().equals(this.serverName)){
				string_key = node.getKey();
			}
		}
		log.fatal("KVServer isResponsibleFor " + string_key);
		ECSNode this_server = metadata.get(string_key);
		log.fatal("KVServer isResponsibleFor this_server determined");
		if (this_server == null) {
			return false;
		}
		log.fatal("KVServer isResponsibleFor this_server is not null");
		String start = this_server.getNodeHashRange()[0];
		String end = this_server.getNodeHashRange()[1];
		log.fatal("starting " + start + " ending " + end);
		log.fatal("KVServer isResponsibleFor got both start and end");
		boolean ascent = start.compareTo(end) < 0;  // start is smaller than end
		boolean within_range = (
			storedKey.compareTo(start) == 0 ||
			storedKey.compareTo(end) == 0 ||
			(ascent && storedKey.compareTo(start) >0 && storedKey.compareTo(end) <0 ) ||
			(!ascent && storedKey.compareTo(start) <0 && storedKey.compareTo(end) <0 ) ||
			(!ascent && storedKey.compareTo(start) >0 && storedKey.compareTo(end) >0 )
		);
		log.fatal("KVServer isResponsibleFor got through " + within_range);
		if (within_range) {
			return true;
		} 
		return false;
	}
	
	public boolean isResponsibleForGet(String k){
		System.out.println(k);
		Map.Entry<String, ECSNode> successor;
		String storedKey = null;
		try{
			MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(k.getBytes());
			byte[] digest = md.digest();
			StringBuilder sb = new StringBuilder();
			String bruv = "";
			for (int i = 0; i < digest.length; ++i) {
				sb.append(Integer.toHexString((digest[i] & 0xFF) | 0x100).substring(1,3));
			}
			System.out.println(sb.toString());
			storedKey = sb.toString();
		} catch( Exception e){
			e.printStackTrace();
		}
		System.out.println(storedKey);

		String upper = metadata.get(storedKey).getNodeHashRange()[0];
		String lower = metadata.get(storedKey).getNodeHashRange()[1];

		successor = metadata.higherEntry(storedKey);
		if(successor == null){
			successor = metadata.firstEntry();
		}
		Map.Entry<String, ECSNode> successor2 = null;
		if (successor != null) {
			successor2 = metadata.higherEntry(successor.getKey());
			if (successor2 == null) {
				successor2 = metadata.firstEntry();
			}
		}
		
		/*
		// if the default version doesn't work, try this version
		if (successor2 != null) {
			upper = successor2.getValue().getNodeHashRange()[0];
		} else if (successor != null) {
			upper = successor.getValue().getNodeHashRange()[0];
		}

		boolean descend = (upper.compareTo(lower) == 1);

		return storedKey.compareTo(upper) == 0 ||
                storedKey.compareTo(lower) == 0 ||
                (storedKey.compareTo(upper) == 1 && storedKey.compareTo(lower) == -1 && !descend) ||
                (storedKey.compareTo(upper) == -1 && storedKey.compareTo(lower) == -1 && descend) ||
                (storedKey.compareTo(upper) == 1 && storedKey.compareTo(lower) == 1 && descend);
		*/

		if (successor2 != null) {
			if (successor2.getValue().getNodeName().equals(this.serverName)) {
				return true;
			}
		}
		if (successor != null) {
			if (successor.getValue().getNodeName().equals(this.serverName)){
				return true;
			}
		} 
		return false;
	}
	
	public void deleteMovedData(){
		/*
		lock.writeLock().lock();
		try (FileWriter writer = new FileWriter(this.filename, false)){
			writer.write(this.cleanedStorage.toString());
			writer.flush();
		} catch (Exception e){
			e.printStackTrace();
		}
		lock.writeLock().unlock();
		*/
	}
	public void moveData(String start, String end, String host, String port) throws UnknownHostException{
		log.fatal("Moving data");
		log.fatal("Port " + port);
		log.fatal("start: " + start + "end: " + end);
		//lock.readLock().lock();
		try (FileReader reader = new FileReader(this.filename)){
			JSONTokener jsonToken = new JSONTokener(reader);
			JSONObject storage = new JSONObject(jsonToken);
			Iterator<String> keys = storage.keys();
			List<String> toremove = new ArrayList<String>();
			while(keys.hasNext()) {
				String key = keys.next();
				log.fatal("Transfer key: " + key); 
				MessageDigest md = MessageDigest.getInstance("MD5");
				md.update(key.getBytes());
				byte[] digest = md.digest();
				StringBuilder sb = new StringBuilder();
				for (byte b : digest) {
					sb.append(String.format("%02X", b));
				}
				String storedKey = sb.toString().toLowerCase();
				log.fatal("Key hash: " + storedKey);
				boolean ascent = start.compareTo(end) < 0;  // start is smaller than end
				boolean within_range = (
					storedKey.compareTo(start) == 0 ||
					storedKey.compareTo(end) == 0 ||
					(ascent && storedKey.compareTo(start) >0 && storedKey.compareTo(end) <0 ) ||
					(!ascent && storedKey.compareTo(start) <0 && storedKey.compareTo(end) <0 ) ||
					(!ascent && storedKey.compareTo(start) >0 && storedKey.compareTo(end) >0 )
				);
				log.fatal("check start:" + start + " end:" + end);
				log.fatal("check key:" + key + " bool within range:" + within_range);
				log.fatal("check ascent:" + ascent + " key>start:" + storedKey.compareTo(start) + " key>end:" + storedKey.compareTo(end));
				if (!within_range) {
					continue;
				}
				//if(start.compareTo(end) == -1){
				//	if(!((storedKey.compareTo(start) == 1 || storedKey.compareTo(start) == 0) && storedKey.compareTo(end) == -1)){
				//		continue;
				//	}
				//}
				//else{
				//	if(!((storedKey.compareTo(start) == 1 || storedKey.compareTo(start) == 0) || storedKey.compareTo(end) == -1)){
				//		continue;
				//	}
				//}
				log.fatal("Transfering key: " + key);
				toremove.add(key);
				
				//Now we must test to see if its within the servers range
				/*
				Map.Entry<String, ECSNode> s;
				s = metadata.higherEntry(storedKey);
				if(s == null){
					s = metadata.firstEntry();
				}
				ECSNode targetServer = s.getValue();
				if(targetServer.getNodeName().equals(this.name)){
					continue;
				}
				*/
				String value = (String)storage.get(key);
				log.fatal("value" + value);
				KVStore store = new KVStore(host, Integer.parseInt(port));
				store.connect();
				try {
				KVMessage res = store.putMove(key, value);
				log.fatal(res.getKey());
				} catch (Exception e){
					log.fatal("Store failed" + " " + host + " " + port);
					log.fatal("connection exception",e);
					e.printStackTrace();
				}
				store.disconnect();
				
				//Error check res
				
			}
			for(int i=0; i<toremove.size(); i++){
				storage.remove(toremove.get(i));
			}
			try (FileWriter writer = new FileWriter(this.filename, false)){//false overwrites file
				writer.write(storage.toString());
				writer.flush();
			}
			//lock.readLock().unlock();
			//this.cleanedStorage = storage;
			//Possible race condition
		}
		catch (FileNotFoundException e) {
            		e.printStackTrace();
			lock.readLock().unlock();
			lock.writeLock().unlock();
			e.printStackTrace();
		} catch (IOException e) {
		    lock.readLock().unlock();
				e.printStackTrace();
		} catch (JSONException e) {
		    lock.readLock().unlock();
				e.printStackTrace();
		}
		catch( NoSuchAlgorithmException e){
			lock.readLock().unlock();
			e.printStackTrace();
		}
		log.fatal("Exiting move");
		return;
	}


	public void moveRep(String start, String end, String host, String port) throws UnknownHostException{
		log.fatal("Moving REP data");
		log.fatal("Port " + port);
		log.fatal("start: " + start + "end: " + end);
		//lock.readLock().lock();
		try (FileReader reader = new FileReader(this.filename)){
			JSONTokener jsonToken = new JSONTokener(reader);
			JSONObject storage = new JSONObject(jsonToken);
			Iterator<String> keys = storage.keys();
			List<String> toremove = new ArrayList<String>();
			while(keys.hasNext()) {
				String key = keys.next();
				log.fatal("Transfer REP key: " + key); 
				MessageDigest md = MessageDigest.getInstance("MD5");
				md.update(key.getBytes());
				byte[] digest = md.digest();
				StringBuilder sb = new StringBuilder();
				for (byte b : digest) {
					sb.append(String.format("%02X", b));
				}
				String storedKey = sb.toString().toLowerCase();
				log.fatal("Key hash: " + storedKey);
				boolean ascent = start.compareTo(end) < 0;  // start is smaller than end
				boolean within_range = (
					storedKey.compareTo(start) == 0 ||
					storedKey.compareTo(end) == 0 ||
					(ascent && storedKey.compareTo(start) >0 && storedKey.compareTo(end) <0 ) ||
					(!ascent && storedKey.compareTo(start) <0 && storedKey.compareTo(end) <0 ) ||
					(!ascent && storedKey.compareTo(start) >0 && storedKey.compareTo(end) >0 )
				);
				log.fatal("check start:" + start + " end:" + end);
				log.fatal("check key:" + key + " bool within range:" + within_range);
				log.fatal("check ascent:" + ascent + " key>start:" + storedKey.compareTo(start) + " key>end:" + storedKey.compareTo(end));
				if (!within_range) {
					continue;
				}
				log.fatal("Transfering REP key: " + key);
				
				//Now we must test to see if its within the servers range
				/*
				Map.Entry<String, ECSNode> s;
				s = metadata.higherEntry(storedKey);
				if(s == null){
					s = metadata.firstEntry();
				}
				ECSNode targetServer = s.getValue();
				if(targetServer.getNodeName().equals(this.name)){
					continue;
				}
				*/
				String value = (String)storage.get(key);
				log.fatal("value" + value);
				KVStore store = new KVStore(host, Integer.parseInt(port));
				store.connect();
				try {
				KVMessage res = store.putRep(key, value);
				log.fatal(res.getKey());
				} catch (Exception e){
					log.fatal("Store failed" + " " + host + " " + port);
					log.fatal("connection exception",e);
					e.printStackTrace();
				}
				store.disconnect();
				
				//Error check res
				
			}
			//lock.readLock().unlock();
			//this.cleanedStorage = storage;
			//Possible race condition
		}
		catch (FileNotFoundException e) {
            		e.printStackTrace();
			lock.readLock().unlock();
			lock.writeLock().unlock();
			e.printStackTrace();
		} catch (IOException e) {
		    lock.readLock().unlock();
				e.printStackTrace();
		} catch (JSONException e) {
		    lock.readLock().unlock();
				e.printStackTrace();
		}
		catch( NoSuchAlgorithmException e){
			lock.readLock().unlock();
			e.printStackTrace();
		}
		log.fatal("Exiting move");
		return;
	}
//	public void update_req(String meta){
//		this.metadata = meta;
//	}
	public void putRep1(String key, String val){
		log.fatal("Inside put rep1!!!!!!!");
		Map.Entry<String, ECSNode> node = null;
		for(Map.Entry<String,ECSNode> temp: metadata.entrySet()){
			log.fatal("GOT HERE inside rep1");
			log.fatal(temp.getValue().getNodeName());
	                if(temp.getValue().getNodeName().equals(serverName)){
	                   node = temp;
			//Here he is adding to active but not removing from all
	                }
                }
		log.fatal("Inside put rep1!!!!!!! STEP 2");
		Map.Entry<String, ECSNode> node1 = metadata.higherEntry(node.getKey());
		if(node1 == null){
			node1 = metadata.firstEntry();
		}
		if(node1.getValue().getNodeName().equals(serverName)){
			log.fatal("Same name");
			return;
		}
		try{
			KVStore tempClient = new KVStore(node1.getValue().getNodeHost(), node1.getValue().getNodePort());
			tempClient.connect();
			KVMessage res = tempClient.putRep(key, val);
			log.fatal("Put rep1 response: " + res);
		}
		catch(Exception e){
			log.fatal("PUTREP EXCEPTION", e);
		}
		log.fatal("Exiting");
	}

	public void putRep2(String key, String val){
		log.fatal("Inside put rep1!!!!!!!");
		Map.Entry<String, ECSNode> node = null;
		for(Map.Entry<String,ECSNode> temp: metadata.entrySet()){
			log.fatal("GOT HERE inside rep1");
			log.fatal(temp.getValue().getNodeName());
	                if(temp.getValue().getNodeName().equals(serverName)){
	                   node = temp;
			//Here he is adding to active but not removing from all
	                }
                }
		log.fatal("Inside put rep1!!!!!!! STEP 2");

		Map.Entry<String, ECSNode> node1 = metadata.higherEntry(node.getKey());
		if(node1 == null){
			node1 = metadata.firstEntry();
		}
		if(node1.getValue().getNodeName().equals(serverName)){
			log.fatal("Same name");
			return;
		}
		Map.Entry<String, ECSNode> node2 = metadata.higherEntry(node1.getKey());
		if(node2 == null){
			node2 = metadata.firstEntry();
		}
		if(node2.getValue().getNodeName().equals(serverName)){
			log.fatal("Same name");
			return;
		}
		try{
			KVStore tempClient = new KVStore(node2.getValue().getNodeHost(), node2.getValue().getNodePort());
			tempClient.connect();
			KVMessage res = tempClient.putRep(key, val);
			log.fatal("Put rep1 response: " + res);
		}
		catch(Exception e){
			log.fatal("PUTREP EXCEPTION", e);
		}
		log.fatal("Exiting");
	}
		
		
		
		

	
	@Override
	public int getPort(){
		// TODO Auto-generated method stub
		// return serverSocket.getLocalPort();
		return this.port;
	}

	@Override
    public String getHostname(){
		// TODO Auto-generated method stub
		if (serverSocket != null)
			return serverSocket.getInetAddress().getHostName();
		else
			return null;
	}

	@Override
    public CacheStrategy getCacheStrategy(){
		// TODO Auto-generated method stub
		return IKVServer.CacheStrategy.None;
	}

	@Override
    public int getCacheSize(){
		// TODO Auto-generated method stub
		return this.cacheSize;
	}

	@Override
    public boolean inStorage(String key){
		// TODO Auto-generated method stub
		lock.readLock().lock();
		try (FileReader reader = new FileReader(this.filename)){
			JSONTokener jsonToken = new JSONTokener(reader);
			JSONObject storage = new JSONObject(jsonToken);
			lock.readLock().unlock();
			return storage.has(key);
			

		}
		catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (JSONException e) {
            e.printStackTrace();
		}
		lock.readLock().unlock();
		return false;
	}

	@Override
    public boolean inCache(String key){
		// TODO Auto-generated method stub
		return false;
	}

	@Override
    public String getKV(String key) throws Exception{
		// TODO Auto-generated method stub
		// TODO Auto-generated method stub

		lock.readLock().lock();
		try (FileReader reader = new FileReader(this.filename)){
			JSONTokener jsonToken = new JSONTokener(reader);
			JSONObject storage = new JSONObject(jsonToken);
			lock.readLock().unlock();
			if(!storage.has(key)){
				throw new Exception("Key doesn't Exist");
			}
			return (String)storage.get(key);
		}
		catch (FileNotFoundException e) {
			e.printStackTrace();
        } catch (IOException e) {
			e.printStackTrace();
        } catch (JSONException e) {
			e.printStackTrace();
		}
		lock.readLock().unlock();
		return null;
	}

	@Override
    public void putKV(String key, String value) throws Exception{
		if(!value.equals("")){
			//lock.writeLock().lock();
			try (FileReader reader = new FileReader(this.filename)){
				JSONTokener jsonToken = new JSONTokener(reader);
				JSONObject storage = new JSONObject(jsonToken);//JSON object might need to be adjusted slightly to work with tokener
				if(storage.has(key)){
					storage.remove(key);
				}
				storage.put(key, value);
				try (FileWriter writer = new FileWriter(this.filename, false)){
					writer.write(storage.toString());
					writer.flush();
				}//Theres a more effecient way of doing this that involves not rewriting the whole
				//file however its a little annoying to do so we can change this if we have time
				//We can argue that it increases robustness.
	
			}
			catch (FileNotFoundException e) {
				e.printStackTrace();
				//lock.writeLock().unlock();
				throw new Exception("File error");
			} catch (IOException e) {
				e.printStackTrace();
				//lock.writeLock().unlock();
				throw new Exception("File error");
			} catch (JSONException e) {
				e.printStackTrace();
				//lock.writeLock().unlock();
				throw new Exception("File error");
			}
			//lock.writeLock().unlock();
		}
		else{
			//lock.writeLock().lock();
			try (FileReader reader = new FileReader(this.filename)){
				JSONTokener jsonToken = new JSONTokener(reader);
				JSONObject storage = new JSONObject(jsonToken);
				if(!storage.has(key)){
					throw new Exception("Key doesn't Exist");
				}
				storage.remove(key);
				try (FileWriter writer = new FileWriter(this.filename, false)){//false overwrites file
					writer.write(storage.toString());
					writer.flush();
				}//Theres a more effecient way of doing this that involves not rewriting the whole
				//file however its a little annoying to do so we can change this if we have time
				//We can argue that it increases robustness.
				

			}
			catch (FileNotFoundException e) {
				e.printStackTrace();
				lock.writeLock().unlock();
				throw new Exception("File error");
			} catch (IOException e) {
				e.printStackTrace();
				lock.writeLock().unlock();
				throw new Exception("File error");
			} catch (JSONException e) {
				e.printStackTrace();
				lock.writeLock().unlock();
				throw new Exception("File error");
			}
			lock.writeLock().unlock();
		}
	}

	@Override
    public void clearCache(){
		// TODO Auto-generated method stub
	}
	public static boolean createStorage(String n){
		// TODO Auto-generated method stub
		int count = 0;

		JSONObject clear = new JSONObject();
		lock.writeLock().lock();
		try (FileWriter writer = new FileWriter(n, true)){//False overwrites file
			writer.write(clear.toString());
			writer.flush();
		}
		catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		}
		lock.writeLock().unlock();
		return true;

	}

	@Override
    public void clearStorage(){
		// TODO Auto-generated method stub
		JSONObject clear = new JSONObject();
		lock.writeLock().lock();
		try (FileWriter writer = new FileWriter(this.filename, false)){//False overwrites file
			writer.write(clear.toString());
			writer.flush();
		}
		catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		}
		lock.writeLock().unlock();
		return;
	}

    public static boolean clearStorageFile(String n){
		// TODO Auto-generated method stub
		JSONObject clear = new JSONObject();
		lock.writeLock().lock();
		try (FileWriter writer = new FileWriter(n, false)){//False overwrites file
			writer.write(clear.toString());
			writer.flush();
		}
		catch (FileNotFoundException e) {
			lock.writeLock().unlock();
			e.printStackTrace();
			return false;
		} catch (IOException e) {
			lock.writeLock().unlock();
			e.printStackTrace();
			return false;
		} catch (JSONException e) {
			lock.writeLock().unlock();
			e.printStackTrace();
			return false;
		}
		lock.writeLock().unlock();
		return true;
	}

	@Override
    public void run(){ //start
		// TODO Auto-generated method stub
		running = initializeServer();



		//Create node in init and wait for initKVServer request
		if (serverSocket != null) {
			try{
				zk = new ZooKeeper(zk_addr + ":" + zk_port, 5000, new Watcher() {
					@Override
					public void process(WatchedEvent event) {
						try{
							if(event.getType().equals(Event.EventType.NodeDataChanged)){
								byte[] buf = zk.getData(event.getPath(), false, zk.exists(event.getPath(), false));
								String msg = new String(buf);
								System.out.println(msg);
								String[] lines = msg.split("\\r?\\n");
								String type = lines[0];
								if(type.equals("metadata")){
									NavigableMap<String, ECSNode> temp = new TreeMap<>();
									for(int i = 1; i < lines.length; i++){
										
										String[] data_entry = lines[i].split("\\s+");
										String[] range = {data_entry[3],data_entry[4]};
										ECSNode new_node = new ECSNode(data_entry[0], data_entry[1], Integer.parseInt(data_entry[2]), range);
										temp.put(range[1], new_node);
									}
									metadata = temp;
									if(!connected){
										start_req();
										connected = true;
										byte[] data = "started".getBytes(StandardCharsets.UTF_8);
										zk.setData(event.getPath(), data, zk.exists(event.getPath(),false).getVersion(), new AsyncCallback.StatCallback() {
											@Override
											public void processResult(int rc, String path, Object ctx, Stat stat) {
												System.out.println("started");
											}
										}, null);
									}
								}else if(type.equals("start")){
									start_req();
								}else if(type.equals("tRep")){
									String end_range = lines[1];
									//temp is key of transfer to
									String start_range = lines[2];
									String to_host = lines[3];
									String to_port = lines[4];
									log.fatal("ENTERING MOVE REP");

									moveRep(start_range, end_range, to_host, to_port);

								} else if(type.equals("transfer")){
									String end_range = lines[1];
									//temp is key of transfer to
									String start_range = lines[2];
									String to_host = lines[3];
									String to_port = lines[4];
									/*
									for(int i = 2; i < lines.length; i++){
										String[] data_entry = lines[i].split("\\s+");
										String[] range = {data_entry[3],data_entry[4]};
										ECSNode new_node = new ECSNode(data_entry[0], data_entry[1], Integer.parseInt(data_entry[2]), range);
										metadata.put(range[1], new_node);
									}
									*/
									moveData(start_range, end_range, to_host, to_port);
									unLockWrite_req();
									byte[] data = "transferred".getBytes(StandardCharsets.UTF_8);
									zk.setData(event.getPath(), data, zk.exists(event.getPath(),false).getVersion());
								} else if(type.equals("stop")){
									stop_req();
								}

								else if(type.equals("delete1")){
									log.fatal("Starting delete1");
									byte[] data = "deleted".getBytes(StandardCharsets.UTF_8);
									zk.setData(event.getPath(), data, zk.exists(event.getPath(), false).getVersion());
								} else if(type.equals("delete2")){
									log.fatal("Starting Move Delete");
									String range_start = lines[1];
									String range_end = lines[2];
									String to_host = lines[3];
									String to_port = lines[4];
									/*
									for(int i = 2; i < lines.length; i++){
										String[] data_entry = lines[i].split("\\s+");
										String[] range = {data_entry[3],data_entry[4]};
										ECSNode new_node = new ECSNode(data_entry[0], data_entry[1], Integer.parseInt(data_entry[2]), range);
										metadata.put(range[1], new_node);
									}
									*/
									//lockWrite_req();
									moveData(range_start, range_end, to_host, to_port);
									byte[] data = "final delete".getBytes(StandardCharsets.UTF_8);
									zk.setData(event.getPath(), data, zk.exists(event.getPath(),false).getVersion());
								}
								
							} else if(event.getType().equals(Event.EventType.NodeDeleted)){
								kill();
								System.exit(0);
							}
							zk.exists("/servers", true);
							zk.exists("/servers/"+serverName, true);
						} catch (Exception e){
							e.printStackTrace();
							kill();
							System.exit(0);
						}
					}
				});
				byte[] data = "waitingMeta".getBytes(StandardCharsets.UTF_8);
				zk.create("/servers/" + serverName, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

			} catch (Exception e){
				kill();
				System.exit(0);
			}
			
			while (isRunning()) {
				//Get and proccess ECS requests
					try {
						Socket client = serverSocket.accept();                
						ClientConnection connection = 
								new ClientConnection(client, this);
						new Thread(connection).start();

						log.info("KVServer/Connected to "
								+ client.getInetAddress().getHostName() 
								+  " on port " + client.getPort());
					} catch (IOException e) {
						log.error("Error! " +
								"Unable to establish connection. \n", e);
					}
			}
		}
		log.info("Server stopped.");
	}

	private boolean isRunning() {
        return this.running;
    }
	private boolean isStopped() {
		return this.stopped;
	}

	@Override
    public void kill(){
		// TODO Auto-generated method stub
		running = false;
		try {
			serverSocket.close();
			zk.close();
		} catch (IOException e) {
			log.error("Error! Cannot close socket on port: " + port, e);
		} catch (InterruptedException e){
			log.error("Error closing zookeeper");
		}
	}

	@Override
    public void close(){
		// TODO Auto-generated method stub
		running = false;
		try {
			serverSocket.close();
		} catch (IOException e) {
			log.error("Error! Cannot close socket on port: " + port, e);
		}
	}

	private boolean initializeServer() {
		log.info("Initialize server ...");
		try (FileReader reader = new FileReader(this.filename)){
			
		} catch (FileNotFoundException e){
			createStorage(this.filename);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		}
    	try {
			//Wait for initKVServer
	    clearStorage();
            serverSocket = new ServerSocket(port);
			log.info("Server listening on port: " + serverSocket.getLocalPort());
            return true;
        } catch (IOException e) {
			log.error("Error! Cannot open server socket:");
            if(e instanceof BindException){
				log.error("Port " + port + " is already bound!");
            }
            return false;
        }
    }

	public static void main(String[] args){
		//Assuming input format is port, name, meta
		Runtime.getRuntime().addShutdownHook(shutdownThread);
		try {
			new LogSetup("logs/server.log", Level.FATAL);
			if(args.length != 4) {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: Server <port> <name>!");
			} else {
				int port = Integer.parseInt(args[0]);
				String name = args[1];
				String zk_addr = args[2];
				String zk_port = args[3];

//				int cacheSize = Integer.parseInt(args[1]);
//				String strategy = args[2];

//				new KVServer(port, cacheSize, strategy).start();
//				new KVServer(port, 0, null, name);
				new KVServer(port, 0, null, name, zk_addr, zk_port);
			}
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		} catch (NumberFormatException nfe) {
			System.out.println("Error! Invalid argument <port>! Not a number!");
			System.out.println("Usage: Server <port>!");
			System.exit(1);
		}
	}
}
