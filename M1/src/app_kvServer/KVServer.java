package app_kvServer;

import app_kvServer.persistence.Storage;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;

public class KVServer implements IKVServer {
    private static Logger logger = Logger.getRootLogger();

    private int port;

    private ServerSocket serverSocket;

    private int cacheSize;

    private CacheStrategy cacheStrategy = CacheStrategy.None;

    private boolean running;

    private Storage store;

    /**
     * Start KV Server at given port
     *
     * @param port      given port for storage server to operate
     * @param cacheSize specifies how many key-value pairs the server is allowed
     *                  to keep in-memory
     * @param strategy  specifies the cache replacement strategy in case the cache
     *                  is full and there is a GET- or PUT-request on a key that is
     *                  currently not contained in the cache. Options are "FIFO", "LRU",
     *                  and "LFU".
     */
    public KVServer(int port, int cacheSize, String strategy, String path) {
        this.port = port;
        this.cacheSize = cacheSize;
        this.cacheStrategy = CacheStrategy.valueOf(strategy);
        this.store = new Storage(path);
    }

    @Override
    public int getPort() {
        return this.port;
    }

    @Override
    public String getHostname() {
        if (this.serverSocket != null) {
            return this.serverSocket.getInetAddress().getHostName();
        }
        return null;
    }

    @Override
    public CacheStrategy getCacheStrategy() {
        return this.cacheStrategy;
    }

    @Override
    public int getCacheSize() {
        return this.cacheSize;
    }

    @Override
    public boolean inStorage(String key) {
        return store.inStorage(key);
    }

    @Override
    public boolean inCache(String key) {
        // TODO Create cache
        return false;
    }

    @Override
    public synchronized String getKV(String key) throws Exception {
        String value = "";
        if (inCache(key)) {
            return "TODO";
        }
        try {
             value = store.get(key);
        } catch (Exception e) {
            throw new Exception("GET_ERROR");
        }
        return value;
    }

    @Override
    public synchronized void putKV(String key, String value) throws Exception {
        if (!store.put(key, value)) {
            throw new Exception("PUT_ERROR");
        }
    }

    public synchronized void deleteKV(String key) throws Exception {
        //hashmap remove method
    }

    @Override
    public void clearCache() {
        // TODO Create cache
    }

    @Override
    public void clearStorage() {
        store.clearStorage();
    }

    @Override
    public void run() {
        running = initializeServer();

        if (serverSocket != null) {
            while (this.running) {
                try {
                    Socket client = serverSocket.accept();
                    KVServerComm connection = new KVServerComm(client, this);
                    new Thread(connection).start();

                    logger.info("Connected to "
                            + client.getInetAddress().getHostName()
                            + " on port " + client.getPort());
                } catch (IOException e) {
                    logger.error("Error! " +
                            "Unable to establish connection. \n", e);
                }
            }
        }
        logger.info("Server stopped");
    }

    private boolean initializeServer() {
        logger.info("Initialize server...");
        try {
            this.serverSocket = new ServerSocket(port);
            logger.info("Server listening on port: "
                    + serverSocket.getLocalPort());
            return true;
        } catch (IOException e) {
            logger.error("Error! Cannot open server socket:");
            if (e instanceof BindException) {
                logger.error("Port " + port + " is already bound!");
            }
            return false;
        }
    }

    @Override
    public void kill() {
        // TODO Check if this is enough
        this.running = false;
    }

    @Override
    public void close() {
        // TODO Check if this works
        running = false;
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (IOException ioe) {
            logger.error("Error closing server", ioe);
        }
    }

    public static void main(String[] args) {
        try {
            new LogSetup("logs/server.log", Level.ALL);
            if (args.length != 4) {
                System.out.println("Error! Invalid number of arguments");
                System.out.println("Usage: KVServer <port> <cache size> <cache strategy> <storage_path>");
            } else {
                int port = Integer.parseInt(args[0]);
                int cacheSize = Integer.parseInt(args[1]);
                new KVServer(port, cacheSize, args[2], args[3]).run();
            }
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        } catch (NumberFormatException nfe) {
            System.out.println("Error! Invalid argument <port>! Not a number!");
            System.out.println("Usage: KVServer <port> <cache size> <cache strategy>");
            System.exit(1);
        }
    }
}
