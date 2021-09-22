package performance;

import app_kvECS.ECSClient;
import app_kvServer.KVServer;
import client.KVStore;
import logger.LogSetup;
import org.apache.log4j.Level;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class performanceEval {


    public static void main(String[] args){

        try {
            new LogSetup("logs/performance.log", Level.OFF);
            File fout = new File("ecs.config");
            FileOutputStream fos = new FileOutputStream(fout);

            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

            for (int i = 0; i < Integer.parseInt(args[1]); i++) {
                int number = 50000+i;
                bw.write("server" + i + " 127.0.0.1" + " " +number);
                bw.newLine();
            }

            bw.close();
        } catch (Exception e){

        }



        int numberOfTasks = Integer.parseInt(args[0]);
        ExecutorService executor= Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        System.out.println("Running...");
        long startTime = System.nanoTime();
        try{
            for ( int i=0; i < numberOfTasks; i++){
                executor.execute(new MyRunnable5050(i));
            }
        }catch(Exception err){
            err.printStackTrace();
        }
//
//
        executor.shutdown(); // once you are done with ExecutorService
        try{
            executor.awaitTermination(10, TimeUnit.MINUTES);
        } catch(Exception e) {

        }
        long endTime = System.nanoTime();
        long timeElapsed = endTime - startTime;
         System.out.println("Execution time in milliseconds : " +
        (timeElapsed / 1000000) + " msec");
//         System.exit(0);
    }
}




class MyRunnable5050 implements Runnable{
    int i = 0;
    public void listFilesForFolder(final File folder, KVStore client) {
        for (final File fileEntry : folder.listFiles()) {
            if(i>5000){
                break;
            }
            if (fileEntry.isDirectory()) {
                listFilesForFolder(fileEntry, client);
            } else {
                System.out.println(fileEntry.getPath());
                try{
                    Path file_path = Paths.get(fileEntry.getPath());
                    String actual = Files.readString(file_path);
                    String[] bruh = actual.split("\\r?\\n");
                    String key = fileEntry.getPath().substring(0, 19);
                    client.put(key, bruh[0]);
                    i = i + 1;
                } catch (Exception e){
                    e.printStackTrace();
                }
            }
        }
    }
    int id;
    public MyRunnable5050(int i){
        this.id = i;
    }
    public void run(){
        try{
            KVStore client = new KVStore("127.0.0.1", 50000);
            client.connect();
            final File folder = new File("maildir");
            listFilesForFolder(folder, client);

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