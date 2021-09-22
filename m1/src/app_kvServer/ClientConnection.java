package app_kvServer;

import jdk.jshell.spi.ExecutionControlProvider;
import org.apache.log4j.Logger;
import shared.messages.KVMessage;
import shared.messages.TextMessage;

import java.io.*;
import java.net.Socket;


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
						KVMessage.StatusType rcv_status = latestMsg.getStatus();
						if (rcv_status.equals(KVMessage.StatusType.GET)){
							try{

								String val = server.getKV(latestMsg.getKey());
								TextMessage send = new TextMessage(KVMessage.StatusType.GET_SUCCESS, latestMsg.getKey(), val);
								sendMessage(send);
								logger.info("Sent response for get request: <" + send.getKey() + "," + send.getValue() + ">");
							}catch (Exception e){
								System.out.println(e.getMessage());
								TextMessage send = new TextMessage(KVMessage.StatusType.GET_ERROR, latestMsg.getKey(), null);
								sendMessage(send);
								logger.error("Error getting value for key: " + latestMsg.getKey());
							}
						} else if((rcv_status.equals(KVMessage.StatusType.PUT))){
							KVMessage.StatusType stat;
							String key = latestMsg.getKey();
							String val = latestMsg.getValue();
							if(val.equals("null")){
								stat = KVMessage.StatusType.DELETE_SUCCESS;
								val = "";
							} else if(server.inStorage(key)){
								stat = KVMessage.StatusType.PUT_UPDATE;
							} else {
								stat = KVMessage.StatusType.PUT_SUCCESS;
							}
							try{
								server.putKV(key, val);
								TextMessage send = new TextMessage(stat, key, val);
								sendMessage(send);
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
						} else {
							logger.error("Invalid message status");
						}
						/* connection either terminated by the client or lost due to
						 * network problems*/
					}
				} catch (IOException ioe) {
					logger.error("Error! Connection lost!");
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
		StringBuffer result = new StringBuffer();
		for (byte b : msgBytes) {
			result.append(String.format("%02X ", b));
			result.append(" "); // delimiter
		}
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		logger.info("SEND \t<" 
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

}
