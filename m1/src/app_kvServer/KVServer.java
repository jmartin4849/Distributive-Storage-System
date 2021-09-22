package app_kvServer;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
 
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.json.JSONException;
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
    private ServerSocket serverSocket;
    private boolean running;
	private static Logger log = Logger.getRootLogger();

	private int cacheSize;
	private String strategy;

	public KVServer(int port, int cacheSize, String strategy) {
		// TODO Auto-generated method stub
		this.port = port;
		this.cacheSize = cacheSize;
		this.strategy = strategy;
		this.start();
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
		try (FileReader reader = new FileReader("storage.json")){
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
		try (FileReader reader = new FileReader("storage.json")){
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
			lock.writeLock().lock();
			try (FileReader reader = new FileReader("storage.json")){
				JSONTokener jsonToken = new JSONTokener(reader);
				JSONObject storage = new JSONObject(jsonToken);//JSON object might need to be adjusted slightly to work with tokener
				if(storage.has(key)){
					storage.remove(key);
				}
				storage.put(key, value);
				try (FileWriter writer = new FileWriter("storage.json", false)){
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
		else{
			lock.writeLock().lock();
			try (FileReader reader = new FileReader("storage.json")){
				JSONTokener jsonToken = new JSONTokener(reader);
				JSONObject storage = new JSONObject(jsonToken);
				if(!storage.has(key)){
					throw new Exception("Key doesn't Exist");
				}
				storage.remove(key);
				try (FileWriter writer = new FileWriter("storage.json", false)){//false overwrites file
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

	@Override
    public void clearStorage(){
		// TODO Auto-generated method stub
		JSONObject clear = new JSONObject();
		lock.writeLock().lock();
		try (FileWriter writer = new FileWriter("storage.json", false)){//False overwrites file
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

	}

	@Override
    public void run(){
		// TODO Auto-generated method stub
		running = initializeServer();
		if (serverSocket != null) {
			while (isRunning()) {
				try {
					Socket client = serverSocket.accept();                
	                ClientConnection connection = 
	                		new ClientConnection(client, this);
	                new Thread(connection).start();

					log.info("Connected to "
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

	@Override
    public void kill(){
		// TODO Auto-generated method stub
		running = false;
		try {
			serverSocket.close();
		} catch (IOException e) {
			log.error("Error! Cannot close socket on port: " + port, e);
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
    	try {
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
		try {
			new LogSetup("logs/server.log", Level.ALL);
			if(args.length != 1) {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: Server <port>!");
			} else {
				int port = Integer.parseInt(args[0]);
//				int cacheSize = Integer.parseInt(args[1]);
//				String strategy = args[2];

//				new KVServer(port, cacheSize, strategy).start();
				new KVServer(port, 0, null);
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
