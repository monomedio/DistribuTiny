package app_kvServer;

import org.apache.log4j.Logger;
import shared.messages.IKVMessage;
import shared.messages.KVMessage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Arrays;
import java.util.Objects;


/**
 * Represents a connection end point for a particular client that is 
 * connected to the server. This class is responsible for message reception 
 * and sending. 
 * The class also implements the echo functionality. Thus whenever a message 
 * is received it is going to be echoed back to the client.
 */
public class KVServerComm implements Runnable {

	private static Logger logger = Logger.getRootLogger();

	private boolean isOpen;
	private static final int BUFFER_SIZE = 1000;
	private static final int DROP_SIZE = 129 * BUFFER_SIZE;

	private Socket clientSocket;
	private InputStream input;
	private OutputStream output;

	private KVServer kvServer;

	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 * @param clientSocket the Socket object for the client connection.
	 */
	public KVServerComm(Socket clientSocket, KVServer kvServer) {
		this.clientSocket = clientSocket;
		this.isOpen = true;
		this.kvServer = kvServer;
	}
	
	/**
	 * Initializes and starts the client connection. 
	 * Loops until the connection is closed or aborted by the client.
	 */
	public void run() {
		try {
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();
			
			while(isOpen) {
				try {
					KVMessage latestMsg = receiveMessage();
					sendMessage(handleMessage(latestMsg));
				/* connection either terminated by the client or lost due to 
				 * network problems*/	
				} catch (IOException ioe) {
					logger.error("Error! Connection lost!");
					isOpen = false;
				} catch (IllegalArgumentException ia) {
					logger.error("Client was terminated!");
					isOpen = false;
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			
		} catch (IOException ioe) {
			logger.error("Error! Connection could not be established!", ioe);
			
		} finally {
			
			try {
				kvServer.deleteClientConnection(clientSocket.getInetAddress().getHostAddress() + ":" + clientSocket.getPort());
				if (clientSocket != null) {
					logger.debug("Cleaning up resources");
					input.close();
					output.close();
					clientSocket.close();

				}
			} catch (IOException ioe) {
				logger.error("Error! Unable to tear down connection!", ioe);
			}
		}
	}
	private KVMessage handlePUTMessage(KVMessage msg) throws Exception {

		if (kvServer.isWriteLocked()) {
			logger.info("Server is currently write locked");
			return new KVMessage(IKVMessage.StatusType.SERVER_WRITE_LOCK, "error");
		}
		boolean keyExists = kvServer.inCache(msg.getKey()) || kvServer.inStorage(msg.getKey());
		if (!keyExists && msg.getValue().equals("null")) {
			logger.debug("Trying to DELETE for non-existent key:" + msg.getKey());
			return new KVMessage(IKVMessage.StatusType.DELETE_ERROR, msg.getKey());
		}
		boolean validDeletion = keyExists && (Objects.equals(msg.getValue(), "null"));
		KVMessage res;
		if (validDeletion) {
			logger.debug("Trying to DELETE for key:" + msg.getKey());
			kvServer.deleteKV(msg.getKey());
			// TODO SEND TO ECS FOR BROADCAST
			KVMessage ecsBroadcast = new KVMessage(IKVMessage.StatusType.BROADCAST_DELETE, msg.getKey());
			this.kvServer.getEcsListener().sendMessage(ecsBroadcast);
			return new KVMessage(KVMessage.StatusType.DELETE_SUCCESS, msg.getKey());
		}

		if (!keyExists) {
			logger.debug("Trying to PUT key: " + msg.getKey() + " with value: " + msg.getValue());
			kvServer.putKV(msg.getKey(), msg.getValue());
			return new KVMessage(KVMessage.StatusType.PUT_SUCCESS,
					msg.getKey(), msg.getValue());
		}

		logger.debug("Trying to PUT_UPDATE for key: " + msg.getKey() + " with value: " + msg.getValue());
		kvServer.putKV(msg.getKey(), msg.getValue());
		res = new KVMessage(KVMessage.StatusType.PUT_UPDATE,
				msg.getKey(), msg.getValue());
		// TODO: SEND TO ECS FOR BROADCAST
		KVMessage ecsBroadcast = new KVMessage(IKVMessage.StatusType.BROADCAST_UPDATE, msg.getKey(), msg.getValue());
		this.kvServer.getEcsListener().sendMessage(ecsBroadcast);
		return res;

	}
	private KVMessage handleMessage(KVMessage msg) throws IOException {
		KVMessage res;
		if (kvServer.isStopped()) {
			return res = new KVMessage(IKVMessage.StatusType.SERVER_STOPPED, "error");
		}
		boolean keyExists;
		if (msg.getKey() != null && msg.getKey().length() > 10 ) {
			logger.info("Key (" + msg.getKey().length() +") too long");
			return res = new KVMessage(IKVMessage.StatusType.FAILED, "Key too long!");
		}

		if (msg.getValue() != null && msg.getValue().length() > 60000) {
			logger.info("Value (" + msg.getValue().length() +" too long\"");
			return res = new KVMessage(IKVMessage.StatusType.FAILED, "Value too long");
		}
		// TODO: send metadata to client when SERVER_NOT_RESPONSIBLE
		if ((msg.getStatus() == IKVMessage.StatusType.GET || msg.getStatus() == IKVMessage.StatusType.PUT) && !kvServer.keyInRange(msg.getKey())) {
			logger.info("KVServer not responsible for this key:" + msg.getKey());
			return res = new KVMessage(IKVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
		}
		try{
			switch (msg.getStatus()) {
				case GET:
					logger.debug("Trying to GET " + msg.getKey());
					keyExists = kvServer.inCache(msg.getKey()) || kvServer.inStorage(msg.getKey());
					if (keyExists) {
						logger.debug("Key found: "  + msg.getKey());
						res = new KVMessage(KVMessage.StatusType.GET_SUCCESS, msg.getKey(), kvServer.getKV(msg.getKey()));
						return res;
					}
					logger.debug("Key not found: " + msg.getKey());
					res = new KVMessage(KVMessage.StatusType.GET_ERROR, msg.getKey());
					return res;
				case PUT:
					return handlePUTMessage(msg);
				case KEYRANGE:
					return res = new KVMessage(IKVMessage.StatusType.KEYRANGE_SUCCESS, kvServer.metadataToString());
				case KEYRANGE_READ:
					return res = new KVMessage(IKVMessage.StatusType.KEYRANGE_READ_SUCCESS, kvServer.extendedMetadataToString()); // TODO: Sam's job
				default:
					return res = new KVMessage(IKVMessage.StatusType.FAILED, "Unknown request");
				}
		} catch (Exception e) {
			//TODO: Can client handle error messages?
			logger.debug(e.getMessage());

			switch (e.getMessage()) {
				case "PUT_ERROR":
					return res = new KVMessage(IKVMessage.StatusType.PUT_ERROR, msg.getKey(), msg.getValue());
				case "GET_ERROR":
					return res = new KVMessage(IKVMessage.StatusType.GET_ERROR, msg.getKey());
			}
			return res = new KVMessage(IKVMessage.StatusType.FAILED, "An IO-error occurred at the server");
		}

	}

	/**
	 * Method sends a TextMessage using this socket.
	 * @param msg the message that is to be sent.
	 * @throws IOException some I/O error regarding the output stream 
	 */
	public void sendMessage(KVMessage msg) throws IOException {
		byte[] msgBytes = msg.getMessageBytes();
		logger.debug(Arrays.toString(msgBytes));
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		logger.info("SEND \t<" 
				+ clientSocket.getInetAddress().getHostAddress() + ":" 
				+ clientSocket.getPort() + ">: '" 
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
		if (msgBytes.length == 0) {
			isOpen = false;
		}
		try {
			msg = new KVMessage(msgBytes);
		} catch (Exception e) {
			msg = new KVMessage(KVMessage.StatusType.FAILED, "Error");
		}
		logger.info("RECEIVE \t<" 
				+ clientSocket.getInetAddress().getHostAddress() + ":" 
				+ clientSocket.getPort() + ">: '" 
				+ msg.getMessage() + "'");
		return msg;
    }

}
