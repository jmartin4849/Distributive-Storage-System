package client;

import app_kvClient.KVClient;
import ecs.ECSNode;
import org.apache.log4j.Logger;
import org.w3c.dom.Text;
import shared.messages.KVMessage;
import shared.messages.TextMessage;

import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import java.lang.NullPointerException;
import java.net.ConnectException;


public class KVStore implements KVCommInterface {

	public NavigableMap<String, ECSNode> allNodes = new TreeMap<>();

	private Logger log = Logger.getRootLogger();
	public String address;
	public int port;
	public boolean connected;
	private Socket clientSocket;
	private InputStream input;
	private OutputStream output;
	private KVClient client;

	private KVMessage response = new TextMessage(KVMessage.StatusType.GET, "", null);

	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 1024 * BUFFER_SIZE;

	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		// TODO Auto-generated method stub
		this.port = port;
		this.address = address;
	}

	@Override
	public void connect() throws UnknownHostException, IOException, IllegalArgumentException {
		clientSocket = new Socket(address, port);
		connected = true;
		output = clientSocket.getOutputStream();
		input = clientSocket.getInputStream();
		log.info("Connection established");
		// TODO Auto-generated method stub
	}

	private TextMessage receiveMessage() throws IOException {
		System.out.println("KVStore/receiveMessage receiving");
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];

		/* read first char from stream */
		byte read = (byte) input.read();
		boolean reading = true;

		if(read == -1){
			System.out.println("KVStore/receiveMessage read==-1");
			disconnect();
		}

		while(read != 13  && read != 10 && read !=-1 && reading) {/* CR, LF, error */
			/* if buffer filled, copy to msg array */
			if(index == BUFFER_SIZE) {
				if(msgBytes == null){
					tmp = new byte[BUFFER_SIZE];
					System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
				} else {
					tmp = new byte[msgBytes.length + BUFFER_SIZE];
					System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
					System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
							BUFFER_SIZE);
				}

				msgBytes = tmp;
				bufferBytes = new byte[BUFFER_SIZE];
				index = 0;
			}

			/* only read valid characters, i.e. letters and constants */
			bufferBytes[index] = read;
			index++;

			/* stop reading is DROP_SIZE is reached */
			if(msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
				reading = false;
			}

			/* read next char from stream */
			read = (byte) input.read();
		}

		if(msgBytes == null){
			tmp = new byte[index];
			System.arraycopy(bufferBytes, 0, tmp, 0, index);
		} else {
			tmp = new byte[msgBytes.length + index];
			System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
			System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
		}

		msgBytes = tmp;

		if(msgBytes.length != 0){
			System.out.println(print(msgBytes));
			TextMessage msg = new TextMessage(msgBytes);
			System.out.println(msg.getStatus() + " " + msg.getKey() + " " + msg.getValue());
			log.info("RECEIVE \t<"
					+ clientSocket.getInetAddress().getHostAddress() + ":"
					+ clientSocket.getPort() + ">: <"
					+ msg.getKey() + "," + msg.getValue() + ">");
			return msg;
		}
		System.out.println("KVStore receive null");
		
		return null;
	}

	public void tearDownConnection() throws IOException {
		connected = false;
		log.info("tearing down the connection ...");
		if (clientSocket != null) {
			//input.close();
			//output.close();
			clientSocket.close();
			clientSocket = null;
			log.info("connection closed!");
		}
		System.out.println("KVStore/tearDownConnection: successful");
	}

	@Override
	public void disconnect() {
		// TODO Auto-generated method stub
		log.info("try to close connection ...");

		try {
			tearDownConnection();
			if(client != null){
				client.handleStatus(KVClient.SocketStatus.DISCONNECTED);
			}
			System.out.println("KVStore/disconnect function successful");

		} catch (IOException ioe) {
			log.error("Unable to close connection!");
		}
	}

	public void attach(KVClient client){
		this.client = client;
	}

	public boolean isConnected(){
		if(connected){
			return true;
		}
		return false;
	}

	@Override
	public TextMessage put(String key, String value) throws Exception {
		// TODO Auto-generated method stub
		TextMessage text = new TextMessage(KVMessage.StatusType.PUT, key, value);
		byte[] msgBytes = text.getMsgBytes();
		System.out.println(print(msgBytes));
		//What are these for
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		log.info("Sent message:\t '" + text.getStatus() + " " + text.getKey() + " " + text.getValue() + "'");
		System.out.println(address + " " + port);
		TextMessage response = receiveMessage();
		assert response != null;
		System.out.println(response.getStatus() + " " + response.getKey());
		 receiveMessage();
		if(response.getStatus() == KVMessage.StatusType.SERVER_NOTRESPONSIBLE){
			response = reconnectServer(response, KVMessage.StatusType.PUT, key, value);
		}
		return response;
	}

	public TextMessage putRep(String key, String value) throws Exception {
		// TODO Auto-generated method stub
		TextMessage text = new TextMessage(KVMessage.StatusType.REPLICA, key, value);
		byte[] msgBytes = text.getMsgBytes();
		System.out.println(print(msgBytes));
		//What are these for
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		log.info("Sent message:\t '" + text.getStatus() + " " + text.getKey() + " " + text.getValue() + "'");
		System.out.println(address + " " + port);
		TextMessage response = receiveMessage();
		assert response != null;
		System.out.println(response.getStatus() + " " + response.getKey());
		 receiveMessage();
		if(response.getStatus() == KVMessage.StatusType.SERVER_NOTRESPONSIBLE){
			response = reconnectServer(response, KVMessage.StatusType.REPLICA, key, value);
		}
		return response;
	}

	public TextMessage putMove(String key, String value) throws Exception {
		// TODO Auto-generated method stub
		TextMessage text = new TextMessage(KVMessage.StatusType.MOVE, key, value);
		byte[] msgBytes = text.getMsgBytes();
		System.out.println(print(msgBytes));
		//What are these for
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		log.info("Sent message:\t '" + text.getStatus() + " " + text.getKey() + " " + text.getValue() + "'");
		System.out.println(address + " " + port);
		TextMessage response = receiveMessage();
		assert response != null;
		System.out.println(response.getStatus() + " " + response.getKey());
		 receiveMessage();
		if(response.getStatus() == KVMessage.StatusType.SERVER_NOTRESPONSIBLE){
			response = reconnectServer(response, KVMessage.StatusType.REPLICA, key, value);
		}
		return response;
	}


	@Override
	public TextMessage get(String key) throws Exception {
		// TODO Auto-generated method stub
		TextMessage text = new TextMessage(KVMessage.StatusType.GET, key, null);
		byte[] msgBytes = text.getMsgBytes();
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		TextMessage response = receiveMessage();
		TextMessage response2 = receiveMessage();
		assert response != null;
		System.out.println("KVStore/get whether response is null " + (response == null));
		System.out.println("KVStore/get whether response2 is null " + (response2 == null));
		try {
			System.out.println(response.getStatus() + " " +response.getKey() + " " + response.getValue());
			if(response.getStatus() == KVMessage.StatusType.SERVER_NOTRESPONSIBLE){
				System.out.println("KVStore/get: SERVER_NOTRESPONSIBLE; reconnecting Server ...");
				response = reconnectServer(response, KVMessage.StatusType.GET, key, null);
			}
			log.info("Sent message:\t '" + text.getStatus() + " " + text.getKey() + "'");
		} catch (NullPointerException nexp) {
			System.out.println("KVStore/get response points to null " + (response == null));
			disconnect();
			for(Map.Entry<String, ECSNode> s: allNodes.entrySet()) {
				if (s == null) {
					continue;
				}
				try {
					ECSNode targetServer = s.getValue();
					this.port = targetServer.getNodePort();
					this.address = targetServer.getNodeHost();
					connect();
					break;
				} catch (ConnectException cexp) {
					continue;
				}
			}
			response = receiveMessage();
			System.out.println("KVStore/get NullPointerException response is null " + (response == null));
			
			response = reconnectServer(response, KVMessage.StatusType.GET, key, null);
			log.info("Sent message:\t '" + text.getStatus() + " " + text.getKey() + "'");
		}
		return response;
	}

	private TextMessage reconnectServer(TextMessage oldMessage, KVMessage.StatusType status, String key, String value) throws Exception {
		TextMessage message = oldMessage;

		NavigableMap<String, ECSNode> allNodesNew = new TreeMap<>();

		// get metadata
		String[] lines = message.getValue().split("/");
		for (String line : lines) {
			System.out.println(line);
			String[] temp = line.split("\\s+");

			String toBeHashed = temp[1] + ":" + temp[2];

			MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(toBeHashed.getBytes());
			byte[] digest = md.digest();
			StringBuilder sb = new StringBuilder();
			for (byte b : digest) {
				sb.append(String.format("%02X", b));
			}
			String[] bruh = {null, sb.toString().toLowerCase()};
			ECSNode ecs = new ECSNode(temp[0], temp[1], Integer.parseInt(temp[2]), bruh);
			System.out.println("Key:");
			System.out.println(sb.toString().toLowerCase());
			allNodesNew.put(temp[0], ecs);
		}

		allNodes = allNodesNew;
		System.out.println("KVStore/reconnectServer: about to disconnect ...");
		disconnect();
		System.out.println("KEY " + key);

		System.out.println("KVStore/reconnectServer: about to findResponsibleServer ...");
		findResponsibleServer(key);
		connect();
		TextMessage response;
		if(status == KVMessage.StatusType.GET){
			response = get(key);
		}else {
			response = put(key, value);
		}

		assert response != null;
		return response;
	}

	private void findResponsibleServer(String key) {
		System.out.println("findResponsibleServer first line ..");
		try {
			if (allNodes != null) {
				MessageDigest md = MessageDigest.getInstance("MD5");
				md.update(key.getBytes());
				byte[] digest = md.digest();
				StringBuilder sb = new StringBuilder();
				for (byte b : digest) {
					sb.append(String.format("%02X", b));
				}
				String storedKey = sb.toString().toLowerCase();
				//Now we must test to see if its within the servers range
				Map.Entry<String, ECSNode> s;
				s = allNodes.higherEntry(storedKey);
				System.out.println("    HELLO findResponsibleServer higherEntry");
				if(s == null){
					s = allNodes.firstEntry();
					System.out.println("    HELLO findResponsibleServer s==null firstEntry");
				}

				ECSNode targetServer = s.getValue();
				this.port = targetServer.getNodePort();
				this.address = targetServer.getNodeHost();
				System.out.println(port);
			} else {
				log.error("Cannot retrieve metadata from server");
			}
		} catch (NoSuchAlgorithmException nexp) {
			log.error("Find responsible server " + nexp);
		}
	}
	public String print(byte[] bytes) {
		StringBuilder sb = new StringBuilder();
		sb.append("[ ");
		for (byte b : bytes) {
			sb.append(String.format("0x%02X ", b));
		}
		sb.append("]");
		return sb.toString();
	}

	public String get_stored_key(String key) {
		try{
			MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(key.getBytes());
			byte[] digest = md.digest();
			StringBuilder sb = new StringBuilder();
			for (byte b : digest) {
				sb.append(String.format("%02X", b));
			}
			String storedKey = sb.toString().toLowerCase();
			return storedKey;
			}
		catch(NoSuchAlgorithmException e){
			return null;
		}
	}
}
