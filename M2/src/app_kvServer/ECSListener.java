package app_kvServer;

import org.apache.log4j.Logger;
import shared.messages.IKVMessage;
import shared.messages.KVMessage;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.Map;

public class ECSListener implements Runnable {
    private static final int BUFFER_SIZE = 1000;
    private static final int DROP_SIZE = 129 * BUFFER_SIZE;
    private static Logger logger = Logger.getRootLogger();
    private KVServer kvServer;
    private InetAddress ecsAddress;
    private int ecsPort;
    private Socket socket;

    private BufferedInputStream input;

    private BufferedOutputStream output;

    private boolean running;

    public ECSListener(KVServer server, InetAddress address, int port) {
        this.kvServer = server;
        this.ecsAddress = address;
        this.ecsPort = port;
        this.running = true;
    }

    public void connect() throws Exception {
        this.socket = new Socket(this.ecsAddress, this.ecsPort);
        this.input = new BufferedInputStream(this.socket.getInputStream());
        this.output = new BufferedOutputStream(this.socket.getOutputStream());
    }

    public boolean initializeListener() {
        try {
            connect();
            return true;
        } catch (Exception e) {
            return false;
        }

    }

    public void sendMessage(KVMessage msg) throws IOException {
        byte[] msgBytes = msg.getMessageBytes();
        logger.debug(Arrays.toString(msgBytes));
        output.write(msgBytes, 0, msgBytes.length);
        output.flush();
        logger.info("SEND \t<"
                + socket.getInetAddress().getHostAddress() + ":"
                + socket.getPort() + ">: '"
                + msg.getMessage() +"'");
    }

    private KVMessage receiveMessage() throws IOException {

        int index = 0;
        byte[] msgBytes = null, tmp = null;
        byte[] bufferBytes = new byte[BUFFER_SIZE];

        /* read first char from stream */
        byte read = (byte) input.read();
        boolean reading = true;
        if (read == 13 || read == 10) {
            read = (byte) input.read();
        }

        while(read != 13 && read !=-1 && reading) {/* CR, LF, error */
            /* if buffer filled, copy to msg array */
            if(index == BUFFER_SIZE) {
                if(msgBytes == null){
                    tmp = new byte[BUFFER_SIZE];
                    System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
                } else {
                    tmp = new byte[msgBytes.length + BUFFER_SIZE];
                    System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
                    System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
                            BUFFER_SIZE);
                }

                msgBytes = tmp;
                bufferBytes = new byte[BUFFER_SIZE];
                index = 0;
            }

            /* only read valid characters, i.e. letters and constants */
            bufferBytes[index] = read;
            index++;

            /* stop reading is DROP_SIZE is reached */
            if(msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
                reading = false;
            }

            /* read next char from stream */
            read = (byte) input.read();
        }

        if(msgBytes == null){
            tmp = new byte[index];
            System.arraycopy(bufferBytes, 0, tmp, 0, index);
        } else {
            tmp = new byte[msgBytes.length + index];
            System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
            System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
        }

        msgBytes = tmp;
        logger.info(Arrays.toString(msgBytes));
        KVMessage msg;
        /* build final String */
        try {
            msg = new KVMessage(msgBytes);
        } catch (Exception e) {
            msg = new KVMessage(KVMessage.StatusType.FAILED, "Error");
        }
        logger.info("RECEIVE \t<"
                + socket.getInetAddress().getHostAddress() + ":"
                + socket.getPort() + ">: '"
                + msg.getMessage() + "'");
        return msg;
    }
    @Override
    public void run() {
        this.running = this.running && initializeListener();
        if (socket != null) {
            while (this.running) {
                try {
                    KVMessage latestMsg = receiveMessage();
                    handleMessage(latestMsg);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private String dataToString(Map<String, String> map) {
        StringBuilder kvString = new StringBuilder();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            kvString.append(entry.getKey()).append(";").append(entry.getValue());
            kvString.append(";");
        }
        return kvString.toString();
    }

    public void handleMessage(KVMessage message) throws IOException {
        String data;
        switch (message.getStatus()) {
            case TR_REQ:
                kvServer.setStatus("WRITE_LOCKED");
                data = dataToString(kvServer.exportData(message.getKey(), message.getValue()));
                data = data.substring(0, data.length() - 1);
                sendMessage(new KVMessage(IKVMessage.StatusType.TR_RES, data));
                break;
            case TR_INIT:
                data = message.getKey();
                String[] keyVals = data.split(";");
                if (keyVals.length % 2 != 0){
                    logger.error("Data transfer failed. Missing key or value");
                    sendMessage(new KVMessage(IKVMessage.StatusType.FAILED, "failed"));
                    break;
                }
                if (kvServer.importData(keyVals)) {
                    sendMessage(new KVMessage(IKVMessage.StatusType.TR_SUCC, "success"));
                    break;
                } else {
                    logger.error("Couldn't store key-values at server");
                    sendMessage(new KVMessage(IKVMessage.StatusType.FAILED, "failed"));
                }
        }

        // Message to assign key ranges
        //kvServer.setRange();

        // Message to get all data from server
        // kvServer.exportData()

        // Message to add data to Server
        // kvServer.importData(map)

        // Message to change status
        // kvServer.setStatus(status)

        // Message to update metadata
        // kvServer.setMetadata(map)

        // Message to shut down server
    }
}
