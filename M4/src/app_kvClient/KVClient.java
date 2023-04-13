package app_kvClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.util.HashMap;

import client.KVStore;
import jdk.jshell.Snippet;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import client.KVCommInterface;
import shared.messages.IKVMessage;


public class KVClient implements IKVClient {

    private static final String PROMPT = "KVClient> ";
    private static final Logger logger = Logger.getRootLogger();
    private BufferedReader stdin;
    private KVStore kvStore = null;
    private boolean stop = false;

    public void run() {
        while(!this.stop) {
            this.stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);

            try {
                String command = this.stdin.readLine();
                this.handleCommand(command);
//                System.out.println("after handle");
//                System.out.println(this.stop);
            } catch (Exception e) {
                e.printStackTrace();
                this.stop = true;
                printError("stdin I/O");
                System.out.println("Terminating application...");
            }
        }
//        System.out.println("done run");
    }

    @Override
    public void newConnection(String hostname, int port) throws Exception {
        try {
            if (this.kvStore == null) {
                this.kvStore = new KVStore(hostname, port);
                this.kvStore.connect();
                Thread kvStoreThread = new Thread(this.kvStore);
                kvStoreThread.setDaemon(true);
                kvStoreThread.start();
                logger.info("Connection established.");
            } else if (this.kvStore.getIsConnected()) {
                System.out.println("Connection already established with " + this.kvStore.getAddress() + ":" + this.kvStore.getPort());
            } else {
                this.kvStore.setAddress(hostname);
                this.kvStore.setPort(port);
                this.kvStore.connect();
                logger.info("Connection established.");
            }
        } catch (Exception e) {
            this.kvStore = null;
            printError("Connection failed.");
        }
    }

    @Override
    public KVCommInterface getStore(){
        return this.kvStore;
    }

//////////////////// METHOD HELPERS ////////////////////
    // TODO make sure this is being used
    public void disconnect() {
        if (this.kvStore != null) {
            this.kvStore.disconnect();
        }
    }

    private void handleCommand(String command) throws Exception {
        String[] tokens = command.split("\\s+");
        String firstToken = tokens[0];
        switch (firstToken) {
            case "subscriptions":
                if (this.kvStore == null || !this.kvStore.getIsConnected()) {
                    printError("Not connected to a server.");
                } else {
                    System.out.println(this.kvStore.getSubscriptions());
                }
                break;
            case "subscribe":
                if (tokens.length != 2) {
                    printError("Invalid number of arguments");
                    printHelp();
                }
                else if (this.kvStore == null || !this.kvStore.getIsConnected()) {
                    printError("Not connected to a server.");
                } else {
                    if (this.kvStore.getSubscriptions().contains(tokens[1])) {
                        printError("Already subscribed to key.");
                    } else {
                       this.kvStore.addSubscription(tokens[1]);
                        System.out.println("Subscribed to key: " + tokens[1]);
                    }
                }
                break;
            case "unsubscribe":
                if (tokens.length != 2) {
                    printError("Invalid number of arguments");
                    printHelp();
                } else if (this.kvStore == null || !this.kvStore.getIsConnected()) {
                    printError("Not connected to a server.");
                } else {
                    if (this.kvStore.getSubscriptions().contains(tokens[1])) {
                        this.kvStore.removeSubscription(tokens[1]);
                        System.out.println("Unsubscribed from key: " + tokens[1]);
                    } else {
                        printError("Already unsubscribed to key.");
                    }
                }
                break;
            case "quit":
                this.stop = true;
                disconnect();
                System.out.println("Terminating application...");
                break;
            case "help":
                printHelp();
                break;
            case "disconnect":
                disconnect();
                System.out.println(PROMPT + "Connection terminated.");
                break;
            case "connect":
                if(tokens.length == 3 && (this.kvStore == null || !this.kvStore.getIsConnected())) {
                    try{
                        newConnection(tokens[1], Integer.parseInt(tokens[2]));
                    } catch(NumberFormatException nfe) {
                        printError("No valid address. Port must be a number.");
                        logger.info("Unable to parse argument <port>", nfe);
                    } catch (UnknownHostException e) {
                        printError("Unknown Host.");
                        logger.info("Unknown Host.", e);
                    } catch (Exception e) {
                        printError("Could not establish connection.");
                        logger.warn("Could not establish connection.", e);
                    }
                } else if (tokens.length != 3) {
                    printError("Invalid number of arguments.");
                } else if (this.kvStore.getIsConnected()) {
                    printError("Connection already established.");
                }
                break;
            case "put":
                if (tokens.length == 3 && this.kvStore != null) {
                    try {
                        kvStore.put(tokens[1], tokens[2]);
                    } catch (Exception e) {
                        printError("KVClient PUT");
                        throw new RuntimeException(e);
                    }
                } else if (tokens.length != 3) {
                    printError("Invalid number of arguments");
                } else {
                    printError("Not connected to a server.");
                }
                break;
            case "get":
                if (tokens.length == 2 && this.kvStore != null && this.kvStore.getIsConnected()) {
                    try {
                        kvStore.get(tokens[1]);
                    } catch (Exception e) {
//                        e.printStackTrace();
                        printError("KVClient GET");
                        throw new RuntimeException(e);
                    }
                } else if (tokens.length != 2) {
                        printError("Invalid number of arguments.");
                    } else {
                        printError("Not connected to a server.");
                    }
                break;
            case "keyrange":
                if (tokens.length == 1 && this.kvStore != null && this.kvStore.getIsConnected()) {
                    try {
                        kvStore.keyRange();

                    } catch (Exception e) {
                        printError("KVClient KEYRANGE");
                        throw new RuntimeException(e);
                    }
                } else if (tokens.length != 1) {
                    printError("Too many arguments given. 0 expected.");
                } else {
                    printError("Not connected to a server.");
                }
                break;
            case "logLevel":
                if(tokens.length == 2) {
                    String level = setLevel(tokens[1]);
                    if(level.equals(LogSetup.UNKNOWN_LEVEL)) {
                        printError(String.format("'%s' is not a valid log level.", tokens[1]));
                        printPossibleLogLevels();
                    } else {
                        System.out.println(PROMPT +
                                "Log level changed to level " + level);
                    }
                } else {
                    printError("Invalid number of arguments.");
                }
                break;
            // Add new lines to terminal
            case "":
                break;
            default:
                printError(String.format("'%s' is not a valid KVClient command.", firstToken));
                printHelp();
                break;
        }
    }

    private String setLevel(String levelString) {

        if(levelString.equals(Level.ALL.toString())) {
            logger.setLevel(Level.ALL);
            return Level.ALL.toString();
        } else if(levelString.equals(Level.DEBUG.toString())) {
            logger.setLevel(Level.DEBUG);
            return Level.DEBUG.toString();
        } else if(levelString.equals(Level.INFO.toString())) {
            logger.setLevel(Level.INFO);
            return Level.INFO.toString();
        } else if(levelString.equals(Level.WARN.toString())) {
            logger.setLevel(Level.WARN);
            return Level.WARN.toString();
        } else if(levelString.equals(Level.ERROR.toString())) {
            logger.setLevel(Level.ERROR);
            return Level.ERROR.toString();
        } else if(levelString.equals(Level.FATAL.toString())) {
            logger.setLevel(Level.FATAL);
            return Level.FATAL.toString();
        } else if(levelString.equals(Level.OFF.toString())) {
            logger.setLevel(Level.OFF);
            return Level.OFF.toString();
        } else {
            return LogSetup.UNKNOWN_LEVEL;
        }
    }

//////////////////// PRINT HELPERS ////////////////////

    private void printError(String error){
        System.out.println(PROMPT + "[ERROR] " +  error);
    }

    private void printPossibleLogLevels() {
        System.out.println(PROMPT
                + "Possible log levels are:");
        System.out.println(PROMPT
                + "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
    }

    private void printHelp() {
        String s = PROMPT + "KVCLIENT HELP (Usage):\n" +
                PROMPT +
                "::::::::::::::::::::::::::::::::" +
                "::::::::::::::::::::::::::::::::\n" +
                PROMPT + "%-30.30s  %-30s%n" +
                PROMPT + "%-30.30s  %-30s%n" +
                PROMPT + "%-30.30s  %-30s%n" +
                PROMPT + "%-30.30s  %-30s%n" +
                PROMPT + "%-30.30s  %-30s%n" +
                PROMPT + "%-30.30s  %-30s%n" +
                PROMPT + "%-30.30s  %-30s%n" +
                PROMPT + "%-30.30s  %-30s%n" +
                PROMPT + "%-30.30s  %-30s%n" +
                PROMPT + "%-30.30s  %-30s%n";
        System.out.printf(s,
                "connect <address> <port>", "Establishes a server connection",
                "disconnect", "Disconnects from the server",
                "put <key> <value>", "| INSERTS a key value pair into the server",
                "", "| UPDATES the current value IF the key already exists in the server",
                "", "| DELETES the entry for the given key IF <value> equals null",
                "get <key>", "Retrieves the value for the given key from the server",
                "quit", "Exits the program",
                "logLevel <level>", "Sets the logger to the specified level",
                "subscribe <key>", "Subscribes to changes and deletion of a key",
                "unsubscribe <key>", "Unsubscribes from changes and deletion of a key",
                "", "| Possible log levels are:",
                "", "| ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
    }

//////////////////// PROGRAM ENTRYPOINT ////////////////////

    public static void main(String[] args) {
        try {
            new LogSetup("logs/client/client.log", Level.ALL);
            final KVClient kvClient = new KVClient();
            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    KVCommInterface store = kvClient.getStore();
                    if (store != null) {
                        store.disconnect();
                        System.out.println("Closing sockets...");
                    }
                }
            });
            kvClient.run();
//            System.out.println("after run");
        } catch (IOException e) {
            System.out.println("[ERROR] Unable to initialize logger.");
            e.printStackTrace();
            System.exit(1);
        }
    }
}
