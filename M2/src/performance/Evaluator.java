package performance;

import app_kvServer.KVServer;
import client.KVStore;
import logger.LogSetup;
import org.apache.log4j.Level;

import java.io.IOException;
import java.net.InetAddress;

public class Evaluator {
    public static void startServer() {
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    new LogSetup("logs/testing/test.log", Level.ERROR);
                    new KVServer(50000, 10, "FIFO", "sample_keys",
                            InetAddress.getByName("127.0.0.1"), InetAddress.getByName("127.0.0.1"), 25).run();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        thread.start();
    }

    public static void getTest(int num, KVStore client) {
        for (int i = 0; i < num; i++) {
            try {
                client.get("key");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static void putTest(int num, KVStore client) {
        for (int i = 0; i < num; i++) {
            try {
                client.put("key", "value");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }


    public static void evaluate(float num_messages) {

        KVStore client = new KVStore("localhost", 50000);
        try {
            client.connect();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        long startTime = System.nanoTime();
        //Execution for 80% GET 20% PUT
        getTest((int)(0.8 * num_messages), client);
        putTest((int)(0.2 * num_messages), client);
        long endTime = System.nanoTime();
        long time = (endTime - startTime) / 1000000;
        System.out.println(num_messages/time);

        startTime = System.nanoTime();
        //Execution for 50% GET 50% PUT
        getTest((int)(0.5 * num_messages), client);
        putTest((int)(0.5 * num_messages), client);
        endTime = System.nanoTime();
        time = (endTime - startTime) / 1000000;
        System.out.println(num_messages/ time);

        startTime = System.nanoTime();
        //Execution for 80% GET 20% PUT
        getTest((int)(0.8 * num_messages), client);
        putTest((int)(0.2 * num_messages), client);
        endTime = System.nanoTime();
        time = (endTime - startTime) / 1000000;
        System.out.println(num_messages/time);
    }

    public static void main(String[] args) {
        float[] num_messages = {1000, 2000, 3000, 4000, 5000};
        for (float n: num_messages
             ) {
            evaluate(n);
        };
    }
}
