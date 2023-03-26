package app_kvServer;

import app_kvServer.caching.Cache;
import app_kvServer.caching.LFUCache;
import app_kvServer.persistence.Storage;
import logger.LogSetup;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import shared.messages.IKVMessage;
import shared.messages.KVMessage;

import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

public class KVServer implements IKVServer {
    private static Logger logger = Logger.getRootLogger();

    private int port;

    private ServerSocket serverSocket;

    private int cacheSize;

    private CacheStrategy cacheStrategy = CacheStrategy.None;

    private boolean running;

    private String status; //STOPPED. WRITE_LOCKED or ACTIVE.

    private Storage store;

    private InetAddress address;

    private String lowerRange; //inclusive

    private String upperRange; //exclusive

    private HashMap<String, String> metadata;

    private InetAddress ecsIp;

    private int ecsPort;

    private ECSListener ecsListener;

    private Cache cache;

    private InternalStore internalStore;

    private String[] replicas;

    private String[] coordinators;

    private String extendedLowerRange;
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
    public KVServer(int port, int cacheSize, String strategy, String path, InetAddress address, InetAddress ecsIp, int ecsPort) {
        this.status = "STOPPED";
        this.port = port;
        this.cacheSize = cacheSize;
        this.cacheStrategy = CacheStrategy.valueOf(strategy);
        this.store = new Storage(path);
        this.address = address;
        this.ecsIp = ecsIp;
        this.ecsPort = ecsPort;
        this.internalStore = new InternalStore(getHostname(), port);
        switch (this.cacheStrategy) {
            case LFU:
                this.cache = new LFUCache(this.cacheSize);
        }
    }

    @Override
    public int getPort() {
        return this.port;
    }

    @Override
    public String getHostname() {
        if (this.serverSocket != null) {
            return this.serverSocket.getInetAddress().getHostAddress();
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
    public synchronized void putKV(String key, String value, boolean replicate) throws Exception {
        if (replicate && this.replicas == null) {
            replicate = false;
        }
        // TODO: Method blocks if we wait for receive message, still have to decide if its worth
        if (replicate) {
            for (int i = 0; i < this.replicas.length; i++) {
                String[] ipAndPort = this.replicas[i].split(":");
                internalStore.connect(ipAndPort[0], Integer.parseInt(ipAndPort[1]));
                internalStore.put(key, value);
//                IKVMessage msg = internalStore.receiveMessage();
//
//                if (msg.getStatus() != IKVMessage.StatusType.PUT_SUCCESS || msg.getStatus() != IKVMessage.StatusType.PUT_UPDATE) {
//                    logger.error("Could not replicate data, received:" + msg.getMessage());
//                    throw new Exception("PUT_ERROR");
//                }
                internalStore.disconnect();
            }

        }
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
    public boolean clearStorage() {
        return store.clearStorage();
    }

    public boolean isStopped() {
        return this.status.equals("STOPPED");
    }

    public boolean isWriteLocked() {
        return this.status.equals("WRITE_LOCKED");
    }

    public boolean isActive() {
        return this.status.equals("ACTIVE");
    }

    public boolean keyInRange(String key) {
        String hashedKey = DigestUtils.md5Hex(key);
        if (hashedKey.compareTo(this.upperRange) == 0) {
            return true;
        }
        // if upperRange is larger than lowerRange
        if (this.upperRange.compareTo(this.lowerRange) > 0) {
            // hashedkey <= upperRange and hashedkey > lowerRange
            return ((hashedKey.compareTo(this.upperRange) <= 0) && hashedKey.compareTo(this.lowerRange) > 0);
        } else {
            // upperRange is smaller than lowerRange (wrap around)
            return ((hashedKey.compareTo(this.upperRange) <= 0 && (this.lowerRange.compareTo(hashedKey) > 0))) ||
                    ((hashedKey.compareTo(this.upperRange) > 0) && (this.lowerRange.compareTo(hashedKey) < 0));
        }
    }

    public void setStatus(String status) {
        if (!(status.equals("STOPPED") || status.equals("ACTIVE") || status.equals("WRITE_LOCKED"))) {
            logger.error("Invalid status type");
            return;
        }
        this.status = status;
    }

    public void setRange(String lowerRange, String upperRange) {
        this.lowerRange = lowerRange;
        this.upperRange = upperRange;
    }

    public synchronized Map<String, String> exportData(String lowerRange, String upperRange) throws IOException {
        if (extendedLowerRange == null) {
            return store.createMap(lowerRange, upperRange);
        }
        System.out.println(extendedLowerRange);
        return store.createMapNonReplicas(lowerRange, upperRange, extendedLowerRange, this.lowerRange, this.upperRange);
    }

    public synchronized Map<String, String> exportData() throws IOException {
        return store.createMap();
    }

    public synchronized Map<String, String> exportDataInRange() throws IOException {
        return store.createMapInRange(this.lowerRange, this.upperRange);
    }

    public synchronized boolean importData(String[] keyAndVals) {
        return store.processMap(keyAndVals);
    }

    public synchronized boolean removeRedundantData() {
        if (extendedLowerRange == null) {
            return store.removeExtraData(this.lowerRange, this.upperRange);
        }
        return store.removeExtraData(this.extendedLowerRange, this.upperRange);
    }
    public void setMetadata(HashMap<String, String> map) {
        this.metadata = map;
        replicateData();
    }

    public void replicateData() {
        boolean changed = setReplicas();
        if (changed) {
            logger.info("Replicating data to " + Arrays.toString(this.replicas));
            try {
                String data = ecsListener.dataToString(exportDataInRange());
                for (int i = 0; i < this.replicas.length; i++) {
                    String[] ipAndPort = this.replicas[i].split(":");
                    internalStore.connect(ipAndPort[0], Integer.parseInt(ipAndPort[1]));
                    internalStore.sendMessage(new KVMessage(IKVMessage.StatusType.REPLICATE, data));
//                IKVMessage msg = internalStore.receiveMessage();
//
//                if (msg.getStatus() != IKVMessage.StatusType.PUT_SUCCESS || msg.getStatus() != IKVMessage.StatusType.PUT_UPDATE) {
//                    logger.error("Could not replicate data, received:" + msg.getMessage());
//                    throw new Exception("PUT_ERROR");
//                }
                    internalStore.disconnect();
                }
            } catch (Exception e) {
                logger.error("Could not replicate data");
            }
        }
    }

    public boolean inMetadata(String ipAndPort) {
        return this.metadata.containsKey(ipAndPort);
    }


    /**
     * Stringify the metadata HashMap
     *
     * @return a String in the format of "<kr-from>,<kr-to>,<ip:port>; <kr-from>,<kr-to>,<ip:port>;" for example,
     * assuming there are only two nodes recorded the metadata
     */
    public String metadataToString() {
        if (this.metadata.isEmpty()) {
            return "EMPTY";
        }
        StringBuilder res = new StringBuilder();
        for (Map.Entry<String, String> entry : this.metadata.entrySet()) {
            res.append(entry.getValue());
            res.append(",");
            res.append(entry.getKey());
            res.append(";");
        }
        res.deleteCharAt(res.length() - 1);
        return res.toString();
    }


    public boolean setReplicas() {
        //TODO: Still needs to be tested properly for edge cases
        logger.info("Trying to assign replicas");
        if (this.metadata.size() == 2) {
            String[] singleReplica = new String[1];
            String[] singleCoors = new String[1];
            String extendedLower = null;
            for (Map.Entry<String, String> entry: this.metadata.entrySet()) {
                if (!Objects.equals(entry.getKey(), this.getHostname() + ":" + this.getPort())) {
                    singleReplica[0] = entry.getKey();
                    singleCoors[0] = entry.getKey();
                    extendedLower = entry.getValue().split(",")[0];
                }
            }
            if (this.replicas != null && Objects.equals(this.replicas[0], singleReplica[0])) {
                logger.info("Replica unchanged");
                return false;
            } else {
                this.replicas = singleReplica;
                this.coordinators = singleCoors;
                this.extendedLowerRange = extendedLower;
                return true;
            }
        }
        if (this.metadata.size() < 3) {
            logger.info("Not enough servers to replicate");
            return false;
        }
        String[] replicaIps = new String[2];
        String[] coordIps = new String[2];
        String secondUpper = null;
        String secondLower = null;
        String finalLower = null;
        for (Map.Entry<String, String> entry : this.metadata.entrySet()) {
            //System.out.println(entry.getKey() + "vs" + this.getHostname());
            if (Objects.equals(entry.getKey(), this.getHostname() + ":" + this.getPort())) {
                continue;
            }
            String[] range = entry.getValue().split(",");
            if (Objects.equals(this.upperRange, range[0])) {
                replicaIps[0] = entry.getKey();
                secondUpper = range[1];
            }
            if (Objects.equals(this.lowerRange, range[1])) {
                coordIps[0] = entry.getKey();
                secondLower = range[0];
                System.out.println("Secondlower1: " + entry.getKey());
            }
        }
        if (secondUpper == null) {
            logger.info("Could not find a replica");
            return false;
        }
        for (Map.Entry<String, String> entry : this.metadata.entrySet()) {
            //System.out.println(entry.getKey() + "vs" + this.getHostname());
            if (Objects.equals(entry.getKey(), this.getHostname() + ":" + this.getPort())) {
                continue;
            }
            String[] range = entry.getValue().split(",");
            if (Objects.equals(secondUpper, range[0])) {
                replicaIps[1] = entry.getKey();
            }

            if (Objects.equals(secondLower, range[1])) {
                coordIps[1] = entry.getKey();
                // Reusing variable to keep extended range
                finalLower = range[0];
                System.out.println("Secondlower2 " + entry.getKey());
            }
        }
        //this.replicas = replicaIps;
        if (this.replicas != null && this.replicas.length == replicaIps.length && Objects.equals(this.replicas[0], replicaIps[0]) && Objects.equals(this.replicas[1], replicaIps[0])) {
            logger.info("Replicas unchanged");
            return false;
        } else {
            this.replicas = replicaIps;
            this.coordinators = coordIps;
            this.extendedLowerRange = finalLower;
            return true;
        }
    }

    @Override
    public void run() {
        running = initializeServer();
        this.ecsListener = new ECSListener(this, ecsIp, ecsPort);
        if (running) {
            new Thread(ecsListener).start();
        }
        if (serverSocket != null) {
            while (this.running) {
                try {
                    Socket client = serverSocket.accept();
                    KVServerComm connection = new KVServerComm(client, this);
                    new Thread(connection).start();

                    logger.info("Connected to "
                            + client.getInetAddress().getHostAddress()
                            + " on port " + client.getPort());
                } catch (IOException e) {
                    logger.error("Error! " +
                            "Unable to establish connection. \n");
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
        System.exit(0);
    }

    public void initiateShutdown() {
        this.ecsListener.shutdown();
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
                "%-30.30s  %-30s%n" +
                "%-30.30s  %-30s%n";
        System.out.printf(s,
                "-b", "Sets the address and port of ECS. Format: <ip>:<port>",
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
            InetAddress ecsAddr = InetAddress.getByName("127.0.0.1");
            int ecsPort = -1;

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
                        case "-b":
                            ecsAddr = InetAddress.getByName(tokens.get(1).split(":")[0]);
                            ecsPort = Integer.parseInt(tokens.get(1).split(":")[1]);
                            break;
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
                final Thread mainThread = Thread.currentThread();
                final KVServer kvServer = new KVServer(port, cSize, cStrat, stPath, addr, ecsAddr, ecsPort);
                Runtime.getRuntime().addShutdownHook(new Thread() {
                    public void run() {
                        try{
                            System.out.println("Waiting for data transfer");
                            kvServer.initiateShutdown();
                            mainThread.join();
                        } catch(InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                });
                kvServer.run();
            }
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        } catch (NumberFormatException nfe) {
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
