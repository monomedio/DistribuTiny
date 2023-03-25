package app_kvECS;

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

public class ECS implements IECS {

    private boolean running;
    private static Logger logger = Logger.getRootLogger();

    private int port;
    private InetAddress address;
    private ServerSocket serverSocket;

    private HashMap<String, String> metadata;
    private HashMap<String, ECSComm> connections;


    public ECS(int port, InetAddress address) {
        this.port = port;
        this.address = address;
        this.metadata = new HashMap<>();
        this.connections = new HashMap<>();
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
        for (Map.Entry entry : this.metadata.entrySet()) {
            res.append(entry.getValue());
            res.append(",");
            res.append(entry.getKey());
            res.append(";");
        }
        res.deleteCharAt(res.length() - 1);
        return res.toString();
    }

    /**
     * Sends a META_UPDATE message containing updated metadata to all servers registered with ECS
     * @throws IOException
     */
    public void broadcastMetadata() throws IOException {
        // construct message that will be sent to each server
        String res = metadataToString();
        System.out.println(res);
        ArrayList<String> connectionsToDelete = new ArrayList<>();
        // send to metadata to each server
        for (Map.Entry<String, ECSComm> entry : this.connections.entrySet()) {
            entry.getValue().sendMessage(new KVMessage(IKVMessage.StatusType.META_UPDATE, res));
            if (!this.metadata.containsKey(entry.getKey())) {
                connectionsToDelete.add(entry.getKey());
            }
        }
        for (String conn : connectionsToDelete) {
            this.connections.remove(conn);
        }
    }

    public ECSComm getECSComm(String key) {
        return this.connections.get(key);
    }

    public ECSComm updateMetadataRemove(String ipAndPort) {
        String[] myRange = this.metadata.get(ipAndPort).split(",");
        String myToHash = myRange[0];
        String myFromHash = myRange[1];
        this.metadata.remove(ipAndPort);
        String fromHash = "";
        String toHash = "";
        String entryIp = "";
        for (Map.Entry<String, String> entry: this.metadata.entrySet()) {
            String[] range = entry.getValue().split(",");
            toHash = range[0];
            fromHash = range[1];
            entryIp = entry.getKey();
            if (toHash.equals(myFromHash)) {
                toHash = myToHash;
                break;
            }
        }

        this.metadata.replace(entryIp, toHash + "," + fromHash);
        return this.connections.get(entryIp);
    }

    /**
     * Hashes ipAndPort <ip:port> and updates metadata.
     * metadata entry:
         * Key: "<ip:port>"
         * Value: "<lowerRange>,<upperRange>"
     * Returns successor ECSComm if exists, null if it doesn't
    */
    public ECSComm updateMetadataAdd(String ipAndPort) {
        String hash = DigestUtils.md5Hex(ipAndPort);
        if (metadata.isEmpty()) {
            metadata.put(ipAndPort, hash + "," + hash);
            return null;
        } else {
            // Key whose lowerRange is the smallest
            String minKey = null;
            // Key whose lowerRange is the smallest that is larger than hash
            String targetKey = null;

            for (Map.Entry<String, String> entry : metadata.entrySet()) {
                String entryKey = entry.getKey();
                String entryUpper = entry.getValue().split(",")[1];

                // minKey computation
                if (minKey == null) {
                    minKey = entryKey;
                } else if (metadata.get(minKey).split(",")[1].compareTo(entryUpper) > 0) { // if minKey's lowerRange is larger than entryUpper
                    minKey = entryKey;
                }

                // targetKey computation
                if ((targetKey == null)) {
                    if (hash.compareTo(entryUpper) < 0) {
                        targetKey = entryKey;
                    }
                } else {
                    if ((metadata.get(targetKey).split(",")[1].compareTo(entryUpper) > 0) && (hash.compareTo(entryUpper) < 0)) {
                        targetKey = entryKey;
                    }
                }
            }

            // minKey will be its successor
            if (targetKey == null) {
                String[] minLowerUpper = metadata.get(minKey).split(",");
                String minLower = minLowerUpper[0];
                String minUpper = minLowerUpper[1];
                metadata.put(minKey, hash + "," + minUpper);
                metadata.put(ipAndPort, minLower + "," + hash);
                return getECSComm(minKey);
            } else { // targetKey is successor
                String[] targetLowerUpper = metadata.get(targetKey).split(",");
                String targetLower = targetLowerUpper[0];
                String targetUpper = targetLowerUpper[1];
                metadata.put(targetKey, hash + "," + targetUpper);
                metadata.put(ipAndPort, targetLower + "," + hash);
                return getECSComm(targetKey);
            }
        }
    }

    public void putConnection(String clientListenerIpAndport, ECSComm comm) {
        this.connections.put(clientListenerIpAndport,comm);
    }

    private boolean initializeECS() {
        logger.info("Initialize ECS...");
        try {
            this.serverSocket = new ServerSocket(this.port, 20000, this.address); // backlog -> value between 10k - 40k
            logger.info("ECS listening on port: "
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
    public void run() {
        running = initializeECS();

        if (serverSocket != null) {
            while (this.running) {
                try {
                    Socket client = serverSocket.accept();
                    ECSComm connection = new ECSComm(client, this);
                    logger.info("New connection registered with ECS");
                    new Thread(connection).start();

                    logger.info("Connected to "
                            + client.getInetAddress().getHostAddress()
                            + " on port " + client.getPort());
                } catch (IOException e) {
                    logger.error("Error! " +
                            "Unable to establish connection. \n", e);
                }
            }
        }
        logger.info("Server stopped");
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
            logger.error("Error closing ECS", ioe);
        }
    }

    public synchronized void addServer(String ipAndPort) throws IOException {
        ECSComm successor = updateMetadataAdd(ipAndPort);
        if (successor == null) {
            broadcastMetadata();
            return;
        }

        successor.retrieveData(metadata.get(successor.getClientListenerIpPort()));
    }

    public synchronized void removeServer(String ipAndPort, String data) throws IOException {
        ECSComm successor = updateMetadataRemove(ipAndPort);
        if (successor == null) {
            // Last server trying to shut down
            this.connections.get(ipAndPort).sendMessage(new KVMessage(IKVMessage.StatusType.LAST_ONE, "goodnight"));
            this.connections.remove(ipAndPort);
            return;
        }
        successor.sendData(data);
    }

    //HELPERS

    /**
     * Find the server that is responsible for the given key
     *
     * @param sampleKey Key for which we're trying to find the responsible server
     * @return "<ip>:<port>" string of the responsible server
     */
    public String findResponsibleServer(String sampleKey) {
        System.out.println("Sample key: " + sampleKey + ". Metadata: " + metadataToString());
        for (Map.Entry<String, String> entry: this.metadata.entrySet()) {
            String[] lowerUpper = entry.getValue().split(",");
            String lower = lowerUpper[0];
            String upper = lowerUpper[1];
            if (keyInRange(sampleKey, lower, upper)) {
                return entry.getKey();
            }
        }
        return "No responsible server found";
    }

    /**
     * Determine whether a key (unhashed) is within the given lower and upper ranges
     *
     * @param key candidate key in determining whether it's in the range
     * @param lower lower range
     * @param upper upper range (inclusive)
     * @return boolean value
     */
    private boolean keyInRange(String key, String lower, String upper) {
        String hashedKey = DigestUtils.md5Hex(key);
        if (hashedKey.compareTo(upper) == 0) {
            return true;
        }
        // if upperRange is larger than lowerRange
        if (upper.compareTo(lower) > 0) {
            // hashedkey <= upperRange and hashedkey > lowerRange
            return ((hashedKey.compareTo(upper) <= 0) && hashedKey.compareTo(lower) > 0);
        } else {
            // upperRange is smaller than lowerRange (wrap around)
            return ((hashedKey.compareTo(upper) <= 0 && (lower.compareTo(hashedKey) > 0))) ||
                    ((hashedKey.compareTo(upper) > 0) && (lower.compareTo(hashedKey) < 0));
        }
    }

    private static void printHelp() {
        String s = "ECS HELP (Usage):\n" +
                "::::::::::::::::::::::::::::::::" +
                "::::::::::::::::::::::::::::::::\n" +
                "%-30.30s  %-30s%n" +
                "%-30.30s  %-30s%n" +
                "%-30.30s  %-30s%n" +
                "%-30.30s  %-30s%n" +
                "%-30.30s  %-30s%n" +
                "%-30.30s  %-30s%n" +
                "%-30.30s  %-30s%n" +
                "%-30.30s  %-30s%n";
        System.out.printf(s,
                "-p", "Sets the port of ECS",
                "-a", "Which address ECS should listen to, set the default to localhost. Default: 127.0.0.1",
                "-d", "Directory for files (Put here the files you need to persist the data)",
                "-l", "Relative path of the logfile. Default: file 'echo.log' in current directory",
                "-ll", "Loglevel. Default: ALL.",
                "", "| Possible log levels are:",
                "", "| ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF",
                "-h", "Display the help.");
    }

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

    public static void main(String[] args) {
        try {
            // set variables to default
            boolean help = false;

            int port = -1;
            InetAddress addr = InetAddress.getByName("127.0.0.1");
            String logDir = "ECS.log"; // default is curr directory
            String logLevelStr = "ALL";


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
                            // sets the port of ECS
                            port = Integer.parseInt(tokens.get(1));
                            break;
                        case "-a":
                            // which address ECS should listen to
                            addr = InetAddress.getByName(tokens.get(1));
                            break;
                        case "-l":
                            // relative path of the logfile
                            logDir = tokens.get(1);
                            break;
                        case "-ll":
                            // LogLevel, e.g., INFO, ALL, ...,
                            logLevelStr = tokens.get(1);
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
                final ECS ecs = new ECS(port, addr);
                Runtime.getRuntime().addShutdownHook(new Thread() {
                    public void run() {
                        ecs.close();
                    }
                });
                ecs.run();
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