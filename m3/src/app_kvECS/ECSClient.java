package app_kvECS;

import java.util.*;

import ecs.ECSNode;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import ecs.IECSNode;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class ECSClient implements IECSClient {

    private static final Logger log = Logger.getRootLogger();
    private static final String PROMPT = "ECSClient> ";
    private boolean running = true;
    static ECS ecs = new ECS();
    static Thread shutdownThread = new Thread(){
        public void run(){
            ecs.shutdown();
        }
    };
    public static void main(String[] args) {
        // TODO
        Runtime.getRuntime().addShutdownHook(shutdownThread);
        try {
            new logger.LogSetup("logs/client.log", Level.OFF);
        } catch (IOException e){
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
        ECSClient client = new ECSClient();


        log.info("client running");
        client.run();
    }

    private void printError(String error){
        System.out.println(PROMPT + "Error! " +  error);
    }

    public void run() {
        while(running) {
            BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);

            try {
                String cmdLine = stdin.readLine();
                this.handleCommand(cmdLine);
            } catch (IOException e) {
                running = false;
                printError("CLI does not respond - Application terminated ");
                log.fatal("CLI does not respond - Application terminated", e);
            }
        }
    }

    public void handleCommand(String cmdLine) {
        String[] tokens = cmdLine.split("\\s+");
        switch (tokens[0]) {
            case "shutdown":
                running = false;
                shutdown();
                System.out.println(PROMPT + "Application exit!");
                break;
            case "stop":
                stop();
                System.out.println(PROMPT + "Stopped the service");
                break;
            case "start":
                start();
                System.out.println(PROMPT + "Started the service");
                break;
            case "addNodes":
                if (tokens.length == 2) {
                    addNodes(Integer.parseInt(tokens[1]));
                    System.out.println(PROMPT + "Adding " + tokens[1] + " nodes");
                } else {
                    printError("Invalid number of parameters!");
                }
                break;
            case "addNode":
                addNode();
                break;
            case "removeNode":
                if (tokens.length == 3) {
                    boolean removed = ecs.removeNode(tokens[1], tokens[2]);
                    if(removed){
                        System.out.println(PROMPT + "Removing node at " + tokens[1] + ":" + tokens[2]);
                    }else {
                        printError("Unable to remove specified node");
                    }
                } else {
                    printError("Invalid number of parameters!");
                }
                break;
            case "help":
                printHelp();
                break;
            default:
                printError("Unknown command");
                printHelp();
                break;
        }
    }
    private void printHelp() {
        String sb = PROMPT + "ECS CLIENT HELP (Usage):\n" +
                PROMPT +
                "::::::::::::::::::::::::::::::::" +
                "::::::::::::::::::::::::::::::::\n" +
                PROMPT + "addNodes <numOfNodes>" +
                "\t\t Adds <numOfNodes> nodes to service\n" +
                PROMPT + "start" +
                "\t\t\t\t Starts service\n" +
                PROMPT + "stop" +
                "\t\t\t\t\t Stops Service\n" +
                PROMPT + "shutdown" +
                "\t\t\t\t Stops service and exits remote processes\n" +
                PROMPT + "addNode" +
                "\t\t\t\t Adds one node to the service\n" +
                PROMPT + "removeNode <list of server names>" +
                "\t Removes specified nodes from the service";
        System.out.println(sb);
    }

    @Override
    public boolean start() {
        // TODO
        ecs.start();
        return false;
    }

    @Override
    public boolean stop() {
        // TODO
        ecs.stop();
        return false;
    }

    @Override
    public boolean shutdown() {
        // TODO
        ecs.shutdown();
        return false;
    }

    @Override
    public IECSNode addNode() {


        return ecs.activateNode();
    }

    @Override
    public Collection<ECSNode> addNodes(int count) {
        // TODO
        Collection<ECSNode> bruh = new ArrayList<ECSNode>();

        for(int i = 0; i < count ; i++){
            ecs.activateNode();
            try{
                synchronized (this){
                    wait(500);
                }
            } catch(Exception e){
                e.printStackTrace();
            }

        }
        return bruh;
    }

    @Override
    public Collection<IECSNode> setupNodes(int count) {
        // TODO
        return null;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        // TODO
        return false;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        // TODO
        return false;
    }

    @Override
    public Map<String, IECSNode> getNodes() {
        // TODO
        return null;
    }

    @Override
    public IECSNode getNodeByKey(String Key) {
        // TODO
        return null;
    }
}
