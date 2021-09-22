package app_kvClient;


import client.KVCommInterface;
import client.KVStore;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import shared.messages.KVMessage;
import shared.messages.TextMessage;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;

public class KVClient implements IKVClient {
    private static Logger log = Logger.getRootLogger();
    private static final String PROMPT = "KVClient> ";
    private BufferedReader stdin;
    private KVStore store;
    private boolean stop = false;

    public String serverAddress;
    public int serverPort;

    public enum SocketStatus{CONNECTED, DISCONNECTED, CONNECTION_LOST};


    /**
     * Entry point for KVClient program
     *
     * @param args command line arguments for starting program, should be null here
     */
    public static void main(String[] args){
        try {
            new logger.LogSetup("logs/client.log", Level.ALL);
        } catch (IOException e){
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
        KVClient client = new KVClient();
        log.info("client running");
        client.run();
    }


    /**
     * Client creates KVStore and attaches to KVStore. KVStore connects to KVServer using provided hostname
     * and port number
     *
     * @param hostname hostname of socket we want to connect to
     * @param port port number of socket we want to connect to
     * @throws UnknownHostException hostname is incorrect
     * @throws IOException unable to create I/O socket
     * @throws IllegalArgumentException port number is incorrect
     */
    @Override
    public void newConnection(String hostname, int port) throws UnknownHostException, IOException, IllegalArgumentException{
        // TODO Auto-generated method stub
        store = new KVStore(hostname, port);
        store.connect();
    }

    /**
     * Disconnect KVStore from KVServer, detach KVClient from KVStore
     */
    private void disconnect() {
        if(store != null){
            store.disconnect();
            store = null;
        }
    }

    /**
     * Fetches KVStore object Client is attached to
     * @return KVStore object
     */
    @Override
    public KVStore getStore(){
        return store;
    }

    /**
     * Main run loop for KVClient, listens for input from stdin and handles commands
     */
    public void run() {
        while(!stop) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);

            try {
                String cmdLine = stdin.readLine();
                this.handleCommand(cmdLine);
            } catch (IOException e) {
                stop = true;
                printError("CLI does not respond - Application terminated ");
                log.fatal("CLI does not respond - Application terminated", e);
            }
        }
    }

    public boolean isRunning() {
        if(!stop){
            return true;
        }
        return false;
    }


    /**
     * When KVStore receives message from KVServer, this function handles message passed from KVStore
     * @param msg TextMessage passed from KVStore
     */
    public void handleNewMessage(TextMessage msg) {
        if(!stop) {
            if (msg.getStatus() == KVMessage.StatusType.GET_SUCCESS){
                System.out.println("Got: <" + msg.getKey() + "," + msg.getValue() + ">");
                log.info("Got: <" + msg.getKey() + "," + msg.getValue() + ">");
            } else if(msg.getStatus() == KVMessage.StatusType.GET_ERROR){
                printError("Error with Get request for key: " + msg.getKey());
                log.error("Error with Get request for key: " + msg.getKey());
            } else if(msg.getStatus() == KVMessage.StatusType.PUT_SUCCESS){
                System.out.println("Put: <" + msg.getKey() + "," + msg.getValue() + ">");
                log.info("Put: <" + msg.getKey() + "," + msg.getValue() + ">");
            } else if(msg.getStatus() == KVMessage.StatusType.PUT_ERROR) {
                printError("Error with put request for: <" + msg.getKey() + "," + msg.getValue() + ">");
                log.error("Error with put request for: <" + msg.getKey() + "," + msg.getValue() + ">");
            } else if(msg.getStatus() == KVMessage.StatusType.DELETE_SUCCESS) {
                System.out.println("Deleted entry: <" + msg.getKey() + "," + msg.getValue() + ">");
                log.info("Deleted entry: <" + msg.getKey() + "," + msg.getValue() + ">");
            } else if(msg.getStatus() == KVMessage.StatusType.DELETE_ERROR){
                printError("Error with delete request for: <" + msg.getKey() + "," + msg.getValue() + ">");
                log.error("Error with delete request for: <" + msg.getKey() + "," + msg.getValue() + ">");
            }
            System.out.print(PROMPT);
        }
    }


    /**
     *
     * @param status
     */
    public void handleStatus(SocketStatus status) {
        if(status == SocketStatus.CONNECTED) {

        } else if (status == SocketStatus.DISCONNECTED) {
            System.out.print(PROMPT);
            System.out.println("Connection terminated: "
                    + serverAddress + " / " + serverPort);
            log.info("Connection terminated: " + serverAddress + " / " + serverPort);
            store.connected = false;
            store = null;

        } else if (status == SocketStatus.CONNECTION_LOST) {
            System.out.println("Connection lost: "
                    + serverAddress + " / " + serverPort);
            System.out.print(PROMPT);
            log.error("Connection terminated: " + serverAddress + " / " + serverPort);
            store.connected = false;
            store = null;
        }
        System.out.print(PROMPT);
    }

    public void handleCommand(String cmdLine) {
        String[] tokens = cmdLine.split("\\s+");

        if(tokens[0].equals("quit")) {
            stop = true;
            disconnect();
            System.out.println(PROMPT + "Application exit!");

        } else if (tokens[0].equals("connect")){
            if(tokens.length == 3) {
                try{
                    serverAddress = tokens[1];
                    serverPort = Integer.parseInt(tokens[2]);
                    newConnection(serverAddress, serverPort);
                    log.info("Connected to KVServer");
                } catch(NumberFormatException nfe) {
                    printError("No valid address. Port must be a number!");
                    log.error("Unable to parse argument <port>", nfe);
                } catch (UnknownHostException e) {
                    printError("Unable to connect to host!");
                    log.error("Unable to connect to host: " + serverAddress, e);
                } catch (IOException e) {
                    printError("I/O error when creating socket");
                    log.error("I/O error when creating socket on port: " + serverPort, e);
                } catch (IllegalArgumentException e){
                    printError("Port number provided invalid");
                    log.error("Port number provided invalid: " + serverPort, e);
                }

            } else {
                printError("Invalid number of parameters!");
            }
        } else  if (tokens[0].equals("put")) {

            if(tokens.length >= 2) {
                if(store != null && store.isConnected()){
                    StringBuilder msg = new StringBuilder();
                    for(int i = 2; i < tokens.length; i++) {
                        msg.append(tokens[i]);
                        if (i != tokens.length -1 ) {
                            msg.append(" ");
                        }
                    }
                    String msgString = msg.toString();
                    try{
                        TextMessage response = store.put(tokens[1],msgString);
                        handleNewMessage(response);
                        log.info("Sent request for PUT: <" + tokens[1] + "," + msgString + ">");
                        log.info("Got response: " + response.getStatus() + " " + response.getKey() + " " + response.getValue());
                    } catch(Exception e){
                        printError("Unable to put key: " + tokens[1] + " for value: " +msgString);
                        log.error("Unable to put key: " + tokens[1] + " for value: " + msgString,e);
                    }

                } else {
                    printError("Not connected!");
                }
            } else {
                printError("Incorrect number of parameters, pass in a key followed by a value");
            }

        } else if(tokens[0].equals("get")) {
            if(tokens.length == 2) {
                if (store != null && store.isConnected()){
                    try{
                        TextMessage response = store.get(tokens[1]);
                        handleNewMessage(response);
                        log.info("Sent request for GET: " + tokens[1]);
                        log.info("Got response: " + response.getStatus() + " " + response.getKey() + " " + response.getValue());
                    } catch(Exception e){
                        printError("Error getting value for key: " + tokens[1]);
                        log.error("Error getting value for key: " + tokens[1], e);
                    }
                }
                else {
                    printError("Not connected!");
                }
            }
            else{
                printError("Incorrect number of parameters, pass in just one key");
            }
        } else if(tokens[0].equals("disconnect")) {

            disconnect();
            log.info("Disconnected from KVServer");

        } else if(tokens[0].equals("logLevel")) {
            if(tokens.length == 2) {
                String level = setLevel(tokens[1]);
                if(level.equals(LogSetup.UNKNOWN_LEVEL)) {
                    printError("No valid log level!");
                    printPossibleLogLevels();
                } else {
                    System.out.println(PROMPT +
                            "Log level changed to level " + level);
                }
            } else {
                printError("Invalid number of parameters!");
            }
        } else if(tokens[0].equals("help")) {
            printHelp();
        } else {
            printError("Unknown command");
            printHelp();
        }
    }

    private void printError(String error){
        System.out.println(PROMPT + "Error! " +  error);
    }

    private void printHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append(PROMPT).append("ECHO CLIENT HELP (Usage):\n");
        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");
        sb.append(PROMPT).append("connect <host> <port>");
        sb.append("\t establishes a connection to a server\n");
        sb.append(PROMPT).append("send <text message>");
        sb.append("\t\t sends a text message to the server \n");
        sb.append(PROMPT).append("disconnect");
        sb.append("\t\t\t disconnects from the server \n");

        sb.append(PROMPT).append("logLevel");
        sb.append("\t\t\t changes the logLevel \n");
        sb.append(PROMPT).append("\t\t\t\t ");
        sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");

        sb.append(PROMPT).append("quit ");
        sb.append("\t\t\t exits the program");
        System.out.println(sb.toString());
    }

    private void printPossibleLogLevels() {
        System.out.println(PROMPT
                + "Possible log levels are:");
        System.out.println(PROMPT
                + LogSetup.getPossibleLogLevels());
    }

    private String setLevel(String levelString) {

        if(levelString.equals(Level.ALL.toString())) {
            log.setLevel(Level.ALL);
            return Level.ALL.toString();
        } else if(levelString.equals(Level.DEBUG.toString())) {
            log.setLevel(Level.DEBUG);
            return Level.DEBUG.toString();
        } else if(levelString.equals(Level.INFO.toString())) {
            log.setLevel(Level.INFO);
            return Level.INFO.toString();
        } else if(levelString.equals(Level.WARN.toString())) {
            log.setLevel(Level.WARN);
            return Level.WARN.toString();
        } else if(levelString.equals(Level.ERROR.toString())) {
            log.setLevel(Level.ERROR);
            return Level.ERROR.toString();
        } else if(levelString.equals(Level.FATAL.toString())) {
            log.setLevel(Level.FATAL);
            return Level.FATAL.toString();
        } else if(levelString.equals(Level.OFF.toString())) {
            log.setLevel(Level.OFF);
            return Level.OFF.toString();
        } else {
            return logger.LogSetup.UNKNOWN_LEVEL;
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

}
