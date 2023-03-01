package app_kvECS;

import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class ECS implements IECS {

    private boolean running;
    private static Logger logger = Logger.getRootLogger();

    private int port;
    private InetAddress address;
    private ServerSocket serverSocket;

    private HashMap<String, String> metadata;


    public ECS(int port, InetAddress address) {
        this.port = port;
        this.address = address;
        this.metadata = new HashMap<String, String>();
    }

    public addNode(String ip, String port, ) {
        // Determine ring position by hashing <ip>:<port>

        // Recalculate and update metadata

        // Send metadata to storage server -> successor (next server in ring) WRITE_LOCKs itself

        // Send affected data items to new server

        // IF Receive successor notification THEN update metadata for ALL servers
        // release WRITE_LOCK on successor (next server in ring) and delete data no longer handled by server


        return;
    }

    public deleteNode() {
        return ;
    }

    @Override
    public void run() {
        running = initializeECS();

        if (serverSocket != null) {
            while (this.running) {
                try {
                    Socket client = serverSocket.accept();
                    ECSComm connection = new ECSComm(client, this);
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
