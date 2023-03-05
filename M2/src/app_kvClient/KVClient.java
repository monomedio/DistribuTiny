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
    private KVCommInterface kvStore = null;
    private boolean stop = false;

    public void run() {
        while(!this.stop) {
            this.stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);

            try {
                String command = this.stdin.readLine();
                this.handleCommand(command);
            } catch (IOException e) {
                this.stop = true;
                printError("stdin I/O");
                System.out.println("Terminating application...");
            }
        }
    }

    @Override
    public void newConnection(String hostname, int port) throws Exception {
        if (this.kvStore != null) {
            throw new IOException("Connection already established.");
        }
        this.kvStore = new KVStore(hostname, port);
        this.kvStore.connect();
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
            this.kvStore = null;
        }
    }

    private void handleCommand(String command) {
        String[] tokens = command.split("\\s+");
        String firstToken = tokens[0];
        switch (firstToken) {
            case "quit":
                this.stop = true;
                if (this.kvStore != null) {
                    kvStore.disconnect();
                }
                // TODO Tears down the active connection to the server and exits the program.
                System.out.println("Terminating application...");
                break;
            case "help":
                printHelp();
                break;
            case "disconnect":
                // TODO disconnect implemented?
                disconnect();
                System.out.println(PROMPT + "Connection terminated.");
                break;
            case "connect":
                // TODO establish connection
                if(tokens.length == 3 && this.kvStore == null) {
                    try{
                        newConnection(tokens[1], Integer.parseInt(tokens[2]));
                        logger.info("Connection established.");
                    } catch(NumberFormatException nfe) {
                        this.kvStore = null;
                        printError("No valid address. Port must be a number.");
                        logger.info("Unable to parse argument <port>", nfe);
                    } catch (UnknownHostException e) {
                        this.kvStore = null;
                        printError("Unknown Host.");
                        logger.info("Unknown Host.", e);
                    } catch (Exception e) {
                        this.kvStore = null;
                        printError("Could not establish connection.");
                        logger.warn("Could not establish connection.", e);
                    }
                } else if (tokens.length != 3) {
                    printError("Invalid number of arguments.");
                } else {
                    printError("Connection already established.");
                }
                break;
            case "put":
                if (tokens.length == 3 && this.kvStore != null) {
                    try {
                        IKVMessage message = kvStore.put(tokens[1], tokens[2]);
                        IKVMessage.StatusType status = message.getStatus();
                        if (status == IKVMessage.StatusType.PUT_ERROR || status == IKVMessage.StatusType.DELETE_ERROR ||
                                status == IKVMessage.StatusType.FAILED || status == IKVMessage.StatusType.SERVER_WRITE_LOCK ||
                                status == IKVMessage.StatusType.SERVER_STOPPED) {
                            printError(message.getMessage());
                        } else {
                            System.out.println(message.getMessage());
                        }
                    } catch (Exception e) {
                        this.kvStore = null;
                        printError("Could not complete PUT request due to I/O error. Disconnecting...");
                        logger.warn("Could not complete PUT request due to I/O error. Disconnecting...");
                    }
                } else if (tokens.length != 3) {
                    printError("Invalid number of arguments");
                } else {
                    printError("Not connected to a server.");
                }
                break;
            case "get":
                if (tokens.length == 2 && this.kvStore != null) {
                    try {
                        IKVMessage message = kvStore.get(tokens[1]);
                        IKVMessage.StatusType status = message.getStatus();
                        if (status == IKVMessage.StatusType.FAILED || status == IKVMessage.StatusType.GET_ERROR ||
                                status == IKVMessage.StatusType.SERVER_STOPPED) {
                            printError(message.getMessage());
                        } else {
                            System.out.println(message.getMessage());
                        }
                    } catch (Exception e) {
                        this.kvStore = null;
                        printError("Could not complete GET request due to I/O error. Disconnecting...");
                        logger.warn("Could not complete GET request due to I/O error. Disconnecting...");
                    }
                } else if (tokens.length != 2) {
                    printError("Invalid number of arguments.");
                } else {
                    printError("Not connected to a server.");
                }
                break;
            case "keyrange":
                if (tokens.length == 1 && this.kvStore != null) {
                    try {
                        IKVMessage message = kvStore.keyRange();
                        IKVMessage.StatusType status = message.getStatus();
                        if (status == IKVMessage.StatusType.SERVER_STOPPED || status == IKVMessage.StatusType.FAILED) {
                            printError(message.getMessage());
                        } else {
                            System.out.println(message.getMessage());
                        }
                    } catch (Exception e) {
                        this.kvStore = null;
                        printError("Could not complete KEYRANGE request due to I/O error. Disconnecting...");
                        logger.warn("Could not complete KEYRANGE request due to I/O error. Disconnecting...");
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
        } catch (IOException e) {
            System.out.println("[ERROR] Unable to initialize logger.");
            e.printStackTrace();
            System.exit(1);
        }
    }
}
