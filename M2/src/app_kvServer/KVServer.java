package app_kvServer;

import app_kvServer.persistence.Storage;
import client.KVCommInterface;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.InetAddress;

import java.net.*;
import java.io.*;
import java.util.*;

public class KVServer implements IKVServer {
    private static Logger logger = Logger.getRootLogger();

    private int port;

    private ServerSocket serverSocket;

    private int cacheSize;

    private CacheStrategy cacheStrategy = CacheStrategy.None;

    private boolean running;

    private Storage store;

    private InetAddress address;

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
     * @param address  specifies which address the server should listen to.
     *                 set to localhost by default
     *
     */
    public KVServer(int port, int cacheSize, String strategy, String path, InetAddress address) {
        this.port = port;
        this.cacheSize = cacheSize;
        this.cacheStrategy = CacheStrategy.valueOf(strategy);
        this.store = new Storage(path);

        this.address = address;
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
        store.delete(key);
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
            this.serverSocket = new ServerSocket(port, 20000, address); // backlog -> value between 10k - 40k
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
        running = false;
        try {
            logger.info("Closing sockets!");
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (IOException ioe) {
            logger.error("Error closing server", ioe);
        }
    }

    // HELPERS
    private static Level getLevel(String levelString) {

        if(levelString.equals(Level.ALL.toString())) {
            return Level.ALL;
        } else if(levelString.equals(Level.DEBUG.toString())) {
            return Level.DEBUG;
        } else if(levelString.equals(Level.INFO.toString())) {
            return Level.INFO;
        } else if(levelString.equals(Level.WARN.toString())) {
            return Level.WARN;
        } else if(levelString.equals(Level.ERROR.toString())) {
            return Level.ERROR;
        } else if(levelString.equals(Level.FATAL.toString())) {
            return Level.FATAL;
        } else if(levelString.equals(Level.OFF.toString())) {
            return Level.OFF;
        } else {
//            return LogSetup.UNKNOWN_LEVEL;
            return null;
        }
    }

    private static void printHelp() {
        String s = "KVSERVER HELP (Usage):\n" +
                "::::::::::::::::::::::::::::::::" +
                "::::::::::::::::::::::::::::::::\n" +
                "%-30.30s  %-30s%n" +
                "%-30.30s  %-30s%n" +
                "%-30.30s  %-30s%n" +
                "%-30.30s  %-30s%n" +
                "%-30.30s  %-30s%n" +
                "%-30.30s  %-30s%n" +
                "%-30.30s  %-30s%n" +
                "%-30.30s  %-30s%n" +
                "%-30.30s  %-30s%n" +
                "%-30.30s  %-30s%n";
        System.out.printf(s,
                "-p", "Sets the port of the server",
                "-a", "Which address the server should listen to, set the default to localhost. Default: 127.0.0.1",
                "-d", "Directory for files (Put here the files you need to persist the data)",
                "-l", "Relative path of the logfile. Default: file 'echo.log' in current directory",
                "-ll", "Loglevel. Default: ALL.",
                "", "| Possible log levels are:",
                "", "| ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF",
                "-csz", "Cache size. Specifies how many key-value pairs the server is allowed to keep in-memory",
                "-cst", "Specifies the cache replacement strategy in case the cache is full. " +
                        "Options are FIFO, LRU, and LFU.",
                "-h", "Display the help.");
    }

    public static void main(String[] args) {
        try {
            // set variables to default
            boolean help = false;

            int port = -1;
            InetAddress addr = InetAddress.getByName("127.0.0.1");
            String stPath = null;
            String logDir = "server.log"; // default is curr directory
            String logLevelStr = "ALL";
            int cSize = 21;
            String cStrat = "LRU";

            // convert args to an ArrayList
            List<String> tokens = new ArrayList<String>(Arrays.asList(args));

            // parse commandline arguments
            while (!tokens.isEmpty()){
                String curr = tokens.get(0);

                if (curr.equals("-h")) {
                    help = true;
                    printHelp();

                    break;
                } else {
                    switch (curr) {
                        case "-p":
                            // sets the port of the server
                            port = Integer.parseInt(tokens.get(1));
                            break;
                        case "-a":
                            // which address the server should listen to
                            addr = InetAddress.getByName(tokens.get(1));
                            break;
                        case "-d":
                            // storage path / directory for files
                            stPath = tokens.get(1);
                            break;
                        case "-l":
                            // relative path of the logfile
                            logDir = tokens.get(1);
                            break;
                        case "-ll":
                            // LogLevel, e.g., INFO, ALL, ...,
                            logLevelStr = tokens.get(1);
                            break;
                        case "-csz":
                            // cache size
                            cSize = Integer.parseInt(tokens.get(1));
                            break;
                        case "-cst":
                            cStrat = tokens.get(1);
                            break;
                    }
                    tokens.remove(1);
                    tokens.remove(0);
                }
            }

            // all are parsed
            if (!help) {
                Level logLevel = getLevel(logLevelStr);

                new LogSetup(logDir, logLevel);
                final KVServer kvServer = new KVServer(port, cSize, cStrat, stPath, addr);
                Runtime.getRuntime().addShutdownHook(new Thread() {
                    public void run() {
                        kvServer.close();
                    }
                });
                kvServer.run();
            }
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        } catch (NumberFormatException nfe) {
//            System.out.println("Error! Invalid argument <port>! Not a number!");
//            System.out.println("Usage: KVServer <port> <cache size> <cache strategy>");
            System.out.println("Error! Invalid arguments provided!");
            printHelp();
            System.exit(1);
        } catch (NullPointerException e) {
            System.out.println("Error! No argument given for mandatory variables.");
            printHelp();
            System.exit(1);
        } catch (IndexOutOfBoundsException iobe) {
            System.out.println("Error! Invalid arguments provided!");
            printHelp();
            System.exit(1);
        } catch (IllegalArgumentException iae) {
            System.out.println("Error! Port must be provided!");
            printHelp();
            System.exit(1);
        }
    }
}
