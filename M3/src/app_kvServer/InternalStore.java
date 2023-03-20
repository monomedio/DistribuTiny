package app_kvServer;

import client.KVCommInterface;
import shared.messages.IKVMessage;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;

import org.apache.log4j.Logger;
import shared.messages.KVMessage;

public class InternalStore {

    private Logger logger = Logger.getRootLogger();

    private String hostAddress;
    private int hostPort;

    private BufferedInputStream input;
    private BufferedOutputStream output;
    private Socket socket;

    private int BUFFER_SIZE = 120100;
    private int DROP_SIZE = 128 * BUFFER_SIZE;

    /**
     * Initialize KVStore with address and port of KVServer
     * @param hostAddress the address of the KVServer that owns this
     * @param hostPort the port of the KVServer that owns this
     */
    public InternalStore(String hostAddress, int hostPort) {
        this.hostAddress = hostAddress;
        this.hostPort = hostPort;
    }

    public void connect(String address, int port) throws Exception {
        this.socket = new Socket(address, port);
        this.input = new BufferedInputStream(this.socket.getInputStream());
        this.output = new BufferedOutputStream(this.socket.getOutputStream());
    }

    public void disconnect() {
        try {
            this.input.close();
            this.output.close();
        } catch (IOException e) {
            logger.warn("Unable to close I/O.", e);
        } finally {
            try {
                this.socket.close();
            } catch (IOException e) {
                logger.warn("Unable to close socket.", e);
            }
        }
    }

    public IKVMessage put(String key, String value) throws Exception {
        IKVMessage message = new KVMessage(IKVMessage.StatusType.PUT_R, key, value);
        sendMessage(message);
        message = receiveMessage();
        return message;
    }

    public IKVMessage get(String key) throws Exception {
        IKVMessage message = new KVMessage(IKVMessage.StatusType.GET_R, key);
        sendMessage(message);
        message = receiveMessage();
        return message;
    }

    /**
     * Method sends a TextMessage using this socket.
     * @param msg the message that is to be sent.
     * @throws IOException some I/O error regarding the output stream
     */
    public void sendMessage(IKVMessage msg) throws IOException {
        byte[] msgBytes = msg.getMessageBytes();
//		System.out.println(Arrays.toString(msgBytes));
        this.output.write(msgBytes, 0, msgBytes.length);
        this.output.flush();
        logger.info("Send message:\t '" + msg.getMessage().substring(0, msg.getMessage().length() - 2) + "'");
    }

    public IKVMessage receiveMessage() throws IOException {

        int index = 0;
        byte[] msgBytes = null, tmp = null;
        byte[] bufferBytes = new byte[BUFFER_SIZE];

        /* read first char from stream */
        byte read = (byte) input.read();
        boolean reading = true;

        if (read == 13 || read == 10) {
            read = (byte) input.read();
        }

        while(read != 13 && reading && read != -1) {/* carriage return */
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

            /* only read valid characters, i.e. letters and numbers */
            if((read > 31 && read < 127)) {
                bufferBytes[index] = read;
                index++;
            }

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

        /* build final String */
        IKVMessage msg = new KVMessage(msgBytes);
        logger.info("Receive message:\t '" + msg.getMessage().substring(0, msg.getMessage().length() - 2) + "'");
        return msg;
    }
}

