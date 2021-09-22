package performance;

import app_kvServer.KVServer;
import client.KVStore;
import logger.LogSetup;
import org.apache.log4j.Level;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class performanceEval {
    public static void main(String args[]){
        try {
            new LogSetup("logs/performance.log", Level.OFF);
        } catch (Exception e){

        }
        KVServer server = new KVServer(7000, 0, null);
        int numberOfTasks = Integer.parseInt(args[0]);
        ExecutorService executor= Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        System.out.println("Running...");
        long startTime = System.nanoTime();
        try{
            for ( int i=0; i < numberOfTasks; i++){
                executor.execute(new MyRunnable8020(i));
            }
        }catch(Exception err){
            err.printStackTrace();
        }


        executor.shutdown(); // once you are done with ExecutorService
        try{
            executor.awaitTermination(10, TimeUnit.MINUTES);
        } catch(Exception e) {

        }
        long endTime = System.nanoTime();
        long timeElapsed = endTime - startTime;
         System.out.println("Execution time in milliseconds : " +
        (timeElapsed / 1000000) + " msec");
         System.exit(0);
    }
}


class MyRunnable5050 implements Runnable{
    int id;
    public MyRunnable5050(int i){
        this.id = i;
    }
    public void run(){
        try{
            KVStore client = new KVStore("localhost", 7000);
            client.connect();
            for(Integer i = 0; i < 100; i++){
                client.put(Integer.toString(id), Integer.toString(id));
                client.get(Integer.toString(id));
                client.put(Integer.toString(id), Integer.toString(id+1));
                client.get(Integer.toString(id));
                client.get(Integer.toString(id));
                client.put(Integer.toString(id), Integer.toString(id));
                client.get(Integer.toString(id));
                client.put(Integer.toString(id), Integer.toString(id+1));
                client.get(Integer.toString(id));
                client.put(Integer.toString(id), null);
            }

        }catch(Exception err){
            err.printStackTrace();
        }
    }
}

class MyRunnable8020 implements Runnable{
    int id;
    public MyRunnable8020(int i){
        this.id = i;
    }
    public void run(){
        try{
            KVStore client = new KVStore("localhost", 7000);
            client.connect();
            for(Integer i = 0; i < 100; i++){
                client.put(Integer.toString(id), Integer.toString(id));
                client.get(Integer.toString(id));
                client.get(Integer.toString(id));
                client.get(Integer.toString(id));
                client.get(Integer.toString(id));
                client.get(Integer.toString(id));
                client.get(Integer.toString(id));
                client.get(Integer.toString(id));
                client.get(Integer.toString(id));
                client.put(Integer.toString(id), null);
            }

        }catch(Exception err){
            err.printStackTrace();
        }
    }
}

class MyRunnable2080 implements Runnable{
    int id;
    public MyRunnable2080(int i){
        this.id = i;
    }
    public void run(){
        try{
            KVStore client = new KVStore("localhost", 7000);
            client.connect();
            for(Integer i = 0; i < 100; i++){
                client.put(Integer.toString(id), Integer.toString(id));
                client.get(Integer.toString(id));
                client.put(Integer.toString(id), Integer.toString(id+1));
                client.put(Integer.toString(id), Integer.toString(id));
                client.put(Integer.toString(id), Integer.toString(id+1));
                client.put(Integer.toString(id), Integer.toString(id));
                client.put(Integer.toString(id), Integer.toString(id+1));
                client.put(Integer.toString(id), Integer.toString(id));
                client.get(Integer.toString(id));
                client.put(Integer.toString(id), null);
            }

        }catch(Exception err){
            err.printStackTrace();
        }
    }
}