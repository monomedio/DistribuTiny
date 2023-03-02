package app_kvECS;

import logger.LogSetup;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import shared.messages.KVMessage;

import java.io.IOException;
import java.net.*;
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
     * @return a String in the format of "<kr-from>, <kr-to>, <ip:port>; <kr-from>, <kr-to>, <ip:port>" for example,
     * assuming there are only two nodes recorded the metadata
     */
    private String metadataToString() {
        // TODO

        return null;
    }

    public void broadcastMetadata() {
        // TODO: construct message that will be sent to each server
        // TODO: send to metadata to each server

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
        if (this.metadata.isEmpty()) {
            this.metadata.put(ipAndPort, hash + "," + hash);
            logger.info("Successfully added server: " + ipAndPort + " to empty ring");
            return null;
        } else {
            // Key whose lowerRange is the smallest
            String minKey = null;
            // Key whose lowerRange is the smallest that is larger than hash
            String targetKey = null;

            for (Map.Entry<String, String> entry : this.metadata.entrySet()) {
                String entryKey = entry.getKey();
                String entryLower = entry.getValue().split(",")[0];

                // minKey computation
                if (minKey == null) {
                    minKey = entryKey;
                } else if (this.metadata.get(minKey).split(",")[0].compareTo(entryLower) > 0) { // if minKey's lowerRange is larger than entryLower
                    minKey = entryKey;
                }

                // targetKey computation
                if ((targetKey == null)) {
                    if (hash.compareTo(entryLower) < 0) {
                        targetKey = entryKey;
                    }
                } else {
                    if ((this.metadata.get(targetKey).split(",")[0].compareTo(entryLower) > 0) && (hash.compareTo(entryLower) < 0)) {
                        targetKey = entryKey;
                    }
                }
            }

            // minKey will be its successor
            if (targetKey == null) {
                String[] minLowerUpper = this.metadata.get(minKey).split(",");
                String minLower = minLowerUpper[0];
                String minUpper = minLowerUpper[1];
                this.metadata.put(minKey, minLower + "," + hash);
                this.metadata.put(ipAndPort, hash + "," + minUpper);
                return this.connections.get(minKey);
            } else { // targetKey is successor
                String[] targetLowerUpper = this.metadata.get(targetKey).split(",");
                String targetLower = targetLowerUpper[0];
                String targetUpper = targetLowerUpper[1];
                this.metadata.put(targetKey, targetLower + "," + hash);
                this.metadata.put(ipAndPort, hash + "," + targetUpper);
                return this.connections.get(targetKey);
            }

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
                    // Add new connection to the connections HashMap
                    this.connections.put(client.getInetAddress().getHostAddress() + ":" + client.getPort(), connection);
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

    //HELPERS

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

    public static void main(String[] args) {
//        try {
//            ECS ecs = new ECS(8008, InetAddress.getByName("127.0.0.1"));
//            ecs.updateMetadataAdd("127.0.0.1:8008");
//        } catch (UnknownHostException e) {
//            throw new RuntimeException(e);
//        }

//        String ipAndPort = "127.0.0.1:3";
//        String hash = "80e7bcff701a97e48834556f72689200";
//
//        HashMap<String, String> metadata = new HashMap();
//        HashMap connections = new HashMap();
//
//        String hash1 = "80e7bcff701a97e48834556f72689308";
//        String hash2 = "0".repeat(32);
//
//        metadata.put("127.0.0.1:1", hash1 + "," + hash2);
//        connections.put("127.0.0.1:1", "ECSComm 127.0.0.1:1");
//
//
//        metadata.put("127.0.0.1:2", hash2 + "," + hash1);
//        connections.put("127.0.0.1:2", "ECSComm 127.0.0.1:2");
//
//
//        if (metadata.isEmpty()) {
//            metadata.put(ipAndPort, hash + "," + hash);
//            System.out.println("bruh");
//        } else {
//            // Key whose lowerRange is the smallest
//            String minKey = null;
//            // Key whose lowerRange is the smallest that is larger than hash
//            String targetKey = null;
//
//            for (Map.Entry<String, String> entry : metadata.entrySet()) {
//                String entryKey = entry.getKey();
//                String entryLower = entry.getValue().split(",")[0];
//
//                // minKey computation
//                if (minKey == null) {
//                    minKey = entryKey;
//                } else if (metadata.get(minKey).split(",")[0].compareTo(entryLower) > 0) { // if minKey's lowerRange is larger than entryLower
//                    minKey = entryKey;
//                }
//
//                // targetKey computation
//                if ((targetKey == null)) {
//                    if (hash.compareTo(entryLower) < 0) {
//                        targetKey = entryKey;
//                    }
//                } else {
//                    if ((metadata.get(targetKey).split(",")[0].compareTo(entryLower) > 0) && (hash.compareTo(entryLower) < 0)) {
//                        targetKey = entryKey;
//                    }
//                }
//            }
//
//            // minKey will be its successor
//            if (targetKey == null) {
//                String[] minLowerUpper = metadata.get(minKey).split(",");
//                String minLower = minLowerUpper[0];
//                String minUpper = minLowerUpper[1];
//                metadata.put(minKey, minLower + "," + hash);
//                metadata.put(ipAndPort, hash + "," + minUpper);
//                System.out.println("Succeeded by minKey");
//                System.out.println(connections.get(minKey));
//            } else { // targetKey is successor
//                String[] targetLowerUpper = metadata.get(targetKey).split(",");
//                String targetLower = targetLowerUpper[0];
//                String targetUpper = targetLowerUpper[1];
//                metadata.put(targetKey, targetLower + "," + hash);
//                metadata.put(ipAndPort, hash + "," + targetUpper);
//                System.out.println("Succeeded by targetKey");
//                System.out.println(connections.get(targetKey));
//            }



//        try {
//            // set variables to default
//            boolean help = false;
//
//            int port = -1;
//            InetAddress addr = InetAddress.getByName("127.0.0.1");
//            String logDir = "ECS.log"; // default is curr directory
//            String logLevelStr = "ALL";
//
//
//            // convert args to an ArrayList
//            List<String> tokens = new ArrayList<String>(Arrays.asList(args));
//
//            // parse commandline arguments
//            while (!tokens.isEmpty()){
//                String curr = tokens.get(0);
//
//                if (curr.equals("-h")) {
//                    help = true;
//                    printHelp();
//
//                    break;
//                } else {
//                    switch (curr) {
//                        case "-p":
//                            // sets the port of ECS
//                            port = Integer.parseInt(tokens.get(1));
//                            break;
//                        case "-a":
//                            // which address ECS should listen to
//                            addr = InetAddress.getByName(tokens.get(1));
//                            break;
//                        case "-l":
//                            // relative path of the logfile
//                            logDir = tokens.get(1);
//                            break;
//                        case "-ll":
//                            // LogLevel, e.g., INFO, ALL, ...,
//                            logLevelStr = tokens.get(1);
//                            break;
//                    }
//                    tokens.remove(1);
//                    tokens.remove(0);
//                }
//            }
//
//            // all are parsed
//            if (!help) {
//                Level logLevel = getLevel(logLevelStr);
//
//                new LogSetup(logDir, logLevel);
//                final ECS ecs = new ECS(port, addr);
//                Runtime.getRuntime().addShutdownHook(new Thread() {
//                    public void run() {
//                        ecs.close();
//                    }
//                });
//                ecs.run();
//            }
//        } catch (IOException e) {
//            System.out.println("Error! Unable to initialize logger!");
//            e.printStackTrace();
//            System.exit(1);
//        } catch (NumberFormatException nfe) {
////            System.out.println("Error! Invalid argument <port>! Not a number!");
////            System.out.println("Usage: KVServer <port> <cache size> <cache strategy>");
//            System.out.println("Error! Invalid arguments provided!");
//            printHelp();
//            System.exit(1);
//        } catch (NullPointerException e) {
//            System.out.println("Error! No argument given for mandatory variables.");
//            printHelp();
//            System.exit(1);
//        } catch (IndexOutOfBoundsException iobe) {
//            System.out.println("Error! Invalid arguments provided!");
//            printHelp();
//            System.exit(1);
//        } catch (IllegalArgumentException iae) {
//            System.out.println("Error! Port must be provided!");
//            printHelp();
//            System.exit(1);
//        }
    }
}

// Paste into public method and uncomment to test updateMetadataAdd
//        String ipAndPort = "127.0.0.1:3";
//        String hash = "80e7bcff701a97e48834556f72689200";
//
//        HashMap<String, String> metadata = new HashMap();
//        HashMap connections = new HashMap();
//
//        String hash1 = "80e7bcff701a97e48834556f72689308";
//        String hash2 = "0".repeat(32);
//
//        metadata.put("127.0.0.1:1", hash1 + "," + hash2);
//        connections.put("127.0.0.1:1", "ECSComm 127.0.0.1:1");
//
//
//        metadata.put("127.0.0.1:2", hash2 + "," + hash1);
//        connections.put("127.0.0.1:2", "ECSComm 127.0.0.1:2");
//
//
//        if (metadata.isEmpty()) {
//            metadata.put(ipAndPort, hash + "," + hash);
//            System.out.println("bruh");
//        } else {
//            // Key whose lowerRange is the smallest
//            String minKey = null;
//            // Key whose lowerRange is the smallest that is larger than hash
//            String targetKey = null;
//
//            for (Map.Entry<String, String> entry : metadata.entrySet()) {
//                String entryKey = entry.getKey();
//                String entryLower = entry.getValue().split(",")[0];
//
//                // minKey computation
//                if (minKey == null) {
//                    minKey = entryKey;
//                } else if (metadata.get(minKey).split(",")[0].compareTo(entryLower) > 0) { // if minKey's lowerRange is larger than entryLower
//                    minKey = entryKey;
//                }
//
//                // targetKey computation
//                if ((targetKey == null)) {
//                    if (hash.compareTo(entryLower) < 0) {
//                        targetKey = entryKey;
//                    }
//                } else {
//                    if ((metadata.get(targetKey).split(",")[0].compareTo(entryLower) > 0) && (hash.compareTo(entryLower) < 0)) {
//                        targetKey = entryKey;
//                    }
//                }
//            }
//
//            // minKey will be its successor
//            if (targetKey == null) {
//                String[] minLowerUpper = metadata.get(minKey).split(",");
//                String minLower = minLowerUpper[0];
//                String minUpper = minLowerUpper[1];
//                metadata.put(minKey, minLower + "," + hash);
//                metadata.put(ipAndPort, hash + "," + minUpper);
//                System.out.println("Succeeded by minKey");
//                System.out.println(connections.get(minKey));
//            } else { // targetKey is successor
//                String[] targetLowerUpper = metadata.get(targetKey).split(",");
//                String targetLower = targetLowerUpper[0];
//                String targetUpper = targetLowerUpper[1];
//                metadata.put(targetKey, targetLower + "," + hash);
//                metadata.put(ipAndPort, hash + "," + targetUpper);
//                System.out.println("Succeeded by targetKey");
//                System.out.println(connections.get(targetKey));
//            }
//
//        }