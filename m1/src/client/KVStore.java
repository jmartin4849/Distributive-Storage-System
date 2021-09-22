package client;

import app_kvClient.KVClient;
import org.apache.log4j.Logger;
import org.w3c.dom.Text;
import shared.messages.KVMessage;
import shared.messages.TextMessage;

import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;

public class KVStore implements KVCommInterface {
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
			TextMessage msg = new TextMessage(msgBytes);
			log.info("RECEIVE \t<"
					+ clientSocket.getInetAddress().getHostAddress() + ":"
					+ clientSocket.getPort() + ">: <"
					+ msg.getKey() + "," + msg.getValue() + ">");
			return msg;
		}
		return null;
	}

	private void tearDownConnection() throws IOException {
		connected = false;
		log.info("tearing down the connection ...");
		if (clientSocket != null) {
			//input.close();
			//output.close();
			clientSocket.close();
			clientSocket = null;
			log.info("connection closed!");
		}
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
	public KVMessage put(String key, String value) throws Exception {
		// TODO Auto-generated method stub
		TextMessage text = new TextMessage(KVMessage.StatusType.PUT, key, value);
		byte[] msgBytes = text.getMsgBytes();
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		log.info("Sent message:\t '" + text.getStatus() + " " + text.getKey() + " " + text.getValue() + "'");
		TextMessage response = receiveMessage();
		receiveMessage();
		return response;
	}


	@Override
	public KVMessage get(String key) throws Exception {
		// TODO Auto-generated method stub
		TextMessage text = new TextMessage(KVMessage.StatusType.GET, key, null);
		byte[] msgBytes = text.getMsgBytes();
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		TextMessage response = receiveMessage();
		receiveMessage();
		log.info("Sent message:\t '" + text.getStatus() + " " + text.getKey() + "'");
		return response;
	}


}
