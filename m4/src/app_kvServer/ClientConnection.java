package app_kvServer;

import ecs.ECSNode;
import jdk.jshell.spi.ExecutionControlProvider;
import org.apache.log4j.Logger;
import shared.messages.KVMessage;
import shared.messages.TextMessage;

import java.io.*;
import java.net.Socket;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;


/**
 * Represents a connection end point for a particular client that is 
 * connected to the server. This class is responsible for message reception 
 * and sending. 
 * The class also implements the echo functionality. Thus whenever a message 
 * is received it is going to be echoed back to the client.
 */
public class ClientConnection implements Runnable {

	private static Logger logger = Logger.getRootLogger();
	
	private boolean isOpen;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;
	
	private Socket clientSocket;
	private InputStream input;
	private OutputStream output;

	private KVServer server;

	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 * @param clientSocket the Socket object for the client connection.
	 */
	public ClientConnection(Socket clientSocket, KVServer server) {
		this.clientSocket = clientSocket;
		this.isOpen = true;
		this.server = server;
	}
	
	/**
	 * Initializes and starts the client connection. 
	 * Loops until the connection is closed or aborted by the client.
	 */
	public void run() {
		try {
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();

			
			while(isOpen) {
				try {
					TextMessage latestMsg = receiveMessage();
					if(latestMsg != null){
						logger.fatal("ClientConnection/run: latestMsg != null");
						KVMessage.StatusType rcv_status = latestMsg.getStatus();
						logger.fatal(latestMsg.getStatus() + " " +latestMsg.getKey() + " " + latestMsg.getValue());
						if (rcv_status.equals(KVMessage.StatusType.GET)){
							logger.fatal("ClientConnection/run: KVMessage.StatusType.GET");
							if(this.server.isServerStopped()){
								logger.fatal("ClientConnection/run: KVMessage.StatusType.GET: server.isServerStopped");
								TextMessage send = new TextMessage(KVMessage.StatusType.SERVER_STOPPED, latestMsg.getKey(), null);
								sendMessage(send);
								logger.fatal("Cannot get key, server is stopped");
							}
							else if(!this.server.isResponsibleFor(latestMsg.getKey())){
								logger.fatal("ClientConnection/run: KVMessage.StatusType.GET: NOT server.isResponsibleFor");
								Set<Map.Entry<String, ECSNode>> meta = server.metadata.entrySet();
								String msg = "";
								for(Map.Entry<String, ECSNode> node : meta){
									msg = msg + node.getKey() + " " + node.getValue().getNodeHost() + " " + node.getValue().getNodePort() + "/";
								}
								TextMessage send = new TextMessage(KVMessage.StatusType.SERVER_NOTRESPONSIBLE, latestMsg.getKey(), msg);
								sendMessage(send);
								logger.fatal("Server is not responsible for key:" + send.getKey());
							}
							else{
								logger.fatal("ClientConnection/run: KVMessage.StatusType.GET: else condition");
								try{
									String val = server.getKV(latestMsg.getKey());
									TextMessage send = new TextMessage(KVMessage.StatusType.GET_SUCCESS, latestMsg.getKey(), val);
									sendMessage(send);
									logger.fatal("Sent response for get request: <" + send.getKey() + "," + send.getValue() + ">");
								}catch (Exception e){
									logger.fatal("ClientConnection/run: KVMessage.StatusType.GET: else condition: GET_ERROR");
									logger.fatal(e.getMessage());
									TextMessage send = new TextMessage(KVMessage.StatusType.GET_ERROR, latestMsg.getKey(), null);
									sendMessage(send);
									logger.error("Error getting value for key: " + latestMsg.getKey());
								}
							}
						} else if((rcv_status.equals(KVMessage.StatusType.PUT))){
							logger.fatal("ClientConnection/run: KVMessage.StatusType.PUT");
							KVMessage.StatusType stat;
							String key = latestMsg.getKey();
							String val = latestMsg.getValue();
							if(this.server.isServerStopped()){
								TextMessage send = new TextMessage(KVMessage.StatusType.SERVER_STOPPED, latestMsg.getKey(), null);
								sendMessage(send);
								logger.fatal("Server is stopped, cannot put");
							}
							else if(!this.server.isResponsibleFor(latestMsg.getKey())){
								logger.fatal(latestMsg.getKey());
								Set<Map.Entry<String, ECSNode>> meta = server.metadata.entrySet();
								String msg = "";
								for(Map.Entry<String, ECSNode> node : meta){
									msg = msg + node.getKey() + " " + node.getValue().getNodeHost() + " " + node.getValue().getNodePort() + "/";
								}
								TextMessage send = new TextMessage(KVMessage.StatusType.SERVER_NOTRESPONSIBLE, latestMsg.getKey(), msg);
								sendMessage(send);
								logger.fatal("Server is not responsible for key: " + send.getKey());
							}
							else if(server.isWriteLocked()){
								TextMessage send = new TextMessage(KVMessage.StatusType.SERVER_WRITELOCK, latestMsg.getKey(), null);
								sendMessage(send);
								logger.fatal("Cannot put, server is write locked");
							}
							else{
								if(val.equals("null") || val.equals("")){
									logger.fatal("Got in");
									stat = KVMessage.StatusType.DELETE_SUCCESS;
									val = "";
								} else if(server.inStorage(key)){
									stat = KVMessage.StatusType.PUT_UPDATE;
								} else {
									stat = KVMessage.StatusType.PUT_SUCCESS;
								}
								try{
									server.putKV(key, val);
									server.putRep1(key, val);
									server.putRep2(key, val);
									TextMessage send = new TextMessage(stat, key, val);
									System.out.println(send.getStatus() + " " + send.getKey() + " " + send.getValue());
									sendMessage(send);
									logger.fatal("done");
								} catch( Exception e){
									if(stat == KVMessage.StatusType.DELETE_SUCCESS){
										logger.error("Error deleting KV-pair for key: " + key );
										TextMessage send = new TextMessage(KVMessage.StatusType.DELETE_ERROR, key, null);
										sendMessage(send);
									} else {
										System.out.println(e.getMessage());
										logger.error("Error putting value: <" + key + "," + val + ">");
										TextMessage send = new TextMessage(KVMessage.StatusType.PUT_ERROR, key, val);
										sendMessage(send);
									}
								}
							}
						}
						else if((rcv_status.equals(KVMessage.StatusType.REPLICA))){
							logger.fatal("ClientConnection/run: KVMessage.StatusType.REPLICA");
							KVMessage.StatusType stat;
							String key = latestMsg.getKey();
							String val = latestMsg.getValue();
							if(this.server.isServerStopped()){
								TextMessage send = new TextMessage(KVMessage.StatusType.SERVER_STOPPED, latestMsg.getKey(), null);
								sendMessage(send);
								logger.fatal("Server is stopped, cannot put");
							}
							if(server.isWriteLocked()){
								TextMessage send = new TextMessage(KVMessage.StatusType.SERVER_WRITELOCK, latestMsg.getKey(), null);
								sendMessage(send);
								logger.fatal("Cannot put, server is write locked");
							}
							else{
								if(val.equals("null") || val.equals("")){
									stat = KVMessage.StatusType.DELETE_SUCCESS;
									val = "";
								} else if(server.inStorage(key)){
									stat = KVMessage.StatusType.PUT_UPDATE;
								} else {
									stat = KVMessage.StatusType.PUT_SUCCESS;
								}
								try{
									logger.fatal("REPLICATING the data");
									server.putKV(key, val);
									TextMessage send = new TextMessage(stat, key, val);
									System.out.println(send.getStatus() + " " + send.getKey() + " " + send.getValue());
									sendMessage(send);
								} catch( Exception e){
									if(stat == KVMessage.StatusType.DELETE_SUCCESS){
										logger.fatal("GOT TO DELETE ERROR");
										logger.fatal("Error deleting KV-pair for key: " + key );
										TextMessage send = new TextMessage(KVMessage.StatusType.DELETE_ERROR, key, null);
										sendMessage(send);
									} else {
										logger.fatal("Got to put error");
										System.out.println(e.getMessage());
										logger.fatal("Error putting value: <" + key + "," + val + ">");
										TextMessage send = new TextMessage(KVMessage.StatusType.PUT_ERROR, key, val);
										sendMessage(send);
									}
								}
							}

						}
						else if((rcv_status.equals(KVMessage.StatusType.MOVE))){
							logger.fatal("ClientConnection/run: KVMessage.StatusType.MOVE");
							KVMessage.StatusType stat;
							String key = latestMsg.getKey();
							String val = latestMsg.getValue();
							if(this.server.isServerStopped()){
								TextMessage send = new TextMessage(KVMessage.StatusType.SERVER_STOPPED, latestMsg.getKey(), null);
								sendMessage(send);
								logger.fatal("Server is stopped, cannot put");
							}
							if(server.isWriteLocked()){
								TextMessage send = new TextMessage(KVMessage.StatusType.SERVER_WRITELOCK, latestMsg.getKey(), null);
								sendMessage(send);
								logger.fatal("Cannot put, server is write locked");
							}
							else{
								if(val.equals("null") || val.equals("")){
									stat = KVMessage.StatusType.DELETE_SUCCESS;
									val = "";
								} else if(server.inStorage(key)){
									stat = KVMessage.StatusType.PUT_UPDATE;
								} else {
									stat = KVMessage.StatusType.PUT_SUCCESS;
								}
								try{
									logger.fatal("MOVING THE DATA into: " + server.getPort());
									
									server.putKV(key, val);
									server.putRep1(key, val);
									server.putRep2(key, val);
									TextMessage send = new TextMessage(stat, key, val);
									System.out.println(send.getStatus() + " " + send.getKey() + " " + send.getValue());
									sendMessage(send);
								} catch( Exception e){
									if(stat == KVMessage.StatusType.DELETE_SUCCESS){
										logger.fatal("GOT TO DELETE ERROR");
										logger.fatal("Error deleting KV-pair for key: " + key );
										TextMessage send = new TextMessage(KVMessage.StatusType.DELETE_ERROR, key, null);
										sendMessage(send);
									} else {
										logger.fatal("Got to put error");
										System.out.println(e.getMessage());
										logger.fatal("Error putting value: <" + key + "," + val + ">");
										TextMessage send = new TextMessage(KVMessage.StatusType.PUT_ERROR, key, val);
										sendMessage(send);
									}
								}
							}

						}
						else {
							logger.fatal("Invalid message status");
						}
						/* connection either terminated by the client or lost due to
						 * network problems*/
					}
				} catch (IOException ioe) {
					logger.fatal("Error! Connection lost!");
					isOpen = false;
				}				
			}
			
		} catch (IOException ioe) {
			logger.error("Error! Connection could not be established!", ioe);
			
		} finally {
			
			try {
				if (clientSocket != null) {
					input.close();
					output.close();
					clientSocket.close();
				}
			} catch (IOException ioe) {
				logger.error("Error! Unable to tear down connection!", ioe);
			}
		}
	}
	
	/**
	 * Method sends a TextMessage using this socket.
	 * @param msg the message that is to be sent.
	 * @throws IOException some I/O error regarding the output stream 
	 */
	public void sendMessage(TextMessage msg) throws IOException {
		byte[] msgBytes = msg.getMsgBytes();

		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		logger.fatal("SEND \t<"
				+ clientSocket.getInetAddress().getHostAddress() + ":" 
				+ clientSocket.getPort() + ">: <"
				+ msg.getKey() + "," + msg.getValue() +">");
    }
	
	
	private TextMessage receiveMessage() throws IOException {
		logger.debug("receiving message");
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];

		/* read first char from stream */
		byte read = (byte) input.read();
		boolean reading = true;
		
//		logger.info("First Char: " + read);
//		Check if stream is closed (read returns -1)
//		if (read == -1){
//			TextMessage msg = new TextMessage("");
//			return msg;
//		}

		if(read == -1){
			isOpen = false;
		}

		while(read != 13  &&  read != 10 && read !=-1 && reading) {/* CR, LF, error */
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
		System.out.println(print(msgBytes));
		if(msgBytes.length != 0){
			TextMessage msg = new TextMessage(msgBytes);
			logger.info("RECEIVE \t<"
					+ clientSocket.getInetAddress().getHostAddress() + ":"
					+ clientSocket.getPort() + ">: <"
					+ msg.getKey() + "," + msg.getValue() + ">");
			return msg;
		}
		return null;
    }

	public static String print(byte[] bytes) {
		StringBuilder sb = new StringBuilder();
		sb.append("[ ");
		for (byte b : bytes) {
			sb.append(String.format("0x%02X ", b));
		}
		sb.append("]");
		return sb.toString();
	}

}
