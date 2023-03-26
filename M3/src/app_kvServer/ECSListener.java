package app_kvServer;

import org.apache.log4j.Logger;
import shared.messages.IKVMessage;
import shared.messages.KVMessage;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.HashMap;
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

    public void shutdown() {
        kvServer.setStatus("WRITE_LOCKED");
        try {
            String data = dataToString(kvServer.exportData());
            sendMessage(new KVMessage(IKVMessage.StatusType.shutdown, data));
        } catch (IOException e) {
            logger.error("Error occurred while trying to send a shutdown message to ECS");
        }
    }

    public void sendMessage(KVMessage msg) throws IOException {
        byte[] msgBytes = msg.getMessageBytes();
        //logger.debug(Arrays.toString(msgBytes));
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
        //logger.info(Arrays.toString(msgBytes));
        KVMessage msg;
        /* build final String */
        try {
            msg = new KVMessage(msgBytes);
        } catch (Exception e) {
            msg = new KVMessage(KVMessage.StatusType.failed, "Error");
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
        logger.info("Listener started on " + socket.getLocalPort());
        if (socket != null) {
            try {
                sendMessage(new KVMessage(IKVMessage.StatusType.serv_init, kvServer.getHostname(), String.valueOf(kvServer.getPort())));
            } catch (IOException e) {
                logger.error("Could not communicate with ECS");
            }
            while (this.running) {
                try {
                    KVMessage latestMsg = receiveMessage();
                    handleMessage(latestMsg);
                    if (latestMsg.getStatus() == IKVMessage.StatusType.failed) {
                        this.running = false;
                        kvServer.close();
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public String dataToString(Map<String, String> map) {
        StringBuilder kvString = new StringBuilder();
        StringBuilder kvStringReplica = new StringBuilder();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (kvServer.keyInRange(entry.getKey())) {
                kvString.append(entry.getKey()).append(";").append(entry.getValue());
                kvString.append(";");
            } else {
                kvStringReplica.append(entry.getKey()).append(";").append(entry.getValue());
                kvStringReplica.append(";");
            }
        }
        System.out.println(kvString);
        System.out.println(kvStringReplica);
        kvString.append(kvStringReplica);
        return kvString.toString();
    }

    public String getServerIpAndPort() {
        return kvServer.getHostname() + ":" + kvServer.getPort();
    }

    public void handleMessage(KVMessage message) throws IOException {
        String data;
        switch (message.getStatus()) {
            case tr_req:
                kvServer.setStatus("WRITE_LOCKED");
                data = dataToString(kvServer.exportData(message.getKey(), message.getValue()));
                if (data.length() == 0) {
                    sendMessage(new KVMessage(IKVMessage.StatusType.tr_res, data));
                    break;
                }
                data = data.substring(0, data.length() - 1);
                sendMessage(new KVMessage(IKVMessage.StatusType.tr_res, data));
                break;
            case tr_init:
                data = message.getKey();
                if (data.length() == 0) {
                    sendMessage(new KVMessage(IKVMessage.StatusType.tr_succ, "success"));
                    break;
                }
                String[] keyVals = data.split(";");
                if (keyVals.length % 2 != 0){
                    logger.error("Data transfer failed. Missing key or value");
                    sendMessage(new KVMessage(IKVMessage.StatusType.failed, "failed"));
                    break;
                }
                if (kvServer.importData(keyVals)) {
                    sendMessage(new KVMessage(IKVMessage.StatusType.tr_succ, "success"));
                } else {
                    logger.error("Couldn't store key-values at server");
                    sendMessage(new KVMessage(IKVMessage.StatusType.failed, "failed"));
                }
                break;
            case meta_update:
                data = message.getKey();
                String[] metadata = data.split(";");
                HashMap<String, String> metadataMap = new HashMap<>();
//                Boolean shutdown = false;
                for (int i = 0; i < metadata.length; i++) {
                    String[] record = metadata[i].split(",");
                    if (this.getServerIpAndPort().compareTo(record[2]) == 0) {
                        kvServer.setRange(record[0], record[1]);
                        if (this.kvServer.isWriteLocked()) {
                            Boolean deleted = kvServer.removeRedundantData();
                        }
                       //kvServer.removeRedundantData();
                    }
                    metadataMap.put(record[2], record[0] + "," + record[1]);
                }
                kvServer.setMetadata(metadataMap);
                kvServer.removeRedundantData();
                if (!kvServer.inMetadata(getServerIpAndPort())) {
                    kvServer.clearStorage();
                    this.running = false;
                    this.socket.close();
                    kvServer.close();
                } else if(kvServer.isWriteLocked() || kvServer.isStopped()){
                    kvServer.setStatus("ACTIVE");
                }
                break;
            case last_one:
                this.socket.close();
                kvServer.close();

        }

    }
}
