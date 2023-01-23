package app_kvServer;

import org.apache.log4j.Logger;
import shared.messages.KVMessage;
import shared.messages.KVMessageClass;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Arrays;


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
	private static final int BUFFER_SIZE = 120100;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;

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
		
//			sendMessage(new TextMessage(
//					"Connection to MSRG Echo server established: "
//					+ clientSocket.getLocalAddress() + " / "
//					+ clientSocket.getLocalPort()));
			
			while(isOpen) {
				try {
					KVMessageClass latestMsg = receiveMessage();
					sendMessage(handleMessage(latestMsg));
				/* connection either terminated by the client or lost due to 
				 * network problems*/	
				} catch (IOException ioe) {
					logger.error("Error! Connection lost!");
					isOpen = false;
				}				
			}
			
		} catch (IOException ioe) {
			logger.error("Error! Connection could not be established!", ioe);
			
		} finally {
			
			try {
				if (clientSocket != null) {
					input.close();
					output.close();
					clientSocket.close();
				}
			} catch (IOException ioe) {
				logger.error("Error! Unable to tear down connection!", ioe);
			}
		}
	}
	private KVMessageClass handlePUTMessage(KVMessageClass msg) throws Exception {
		boolean keyExists = kvServer.inCache(msg.getKey()) || kvServer.inStorage(msg.getKey());
		boolean validDeletion = keyExists && (msg.getValue() == null);
		KVMessageClass res;
		if (validDeletion) {
			logger.debug("Trying to DELETE for key:" + msg.getKey());
			kvServer.deleteKV(msg.getKey());
			return new KVMessageClass(KVMessage.StatusType.DELETE_SUCCESS.toString() + ",,");
		}

		if (!keyExists) {
			logger.debug("Trying to PUT key: " + msg.getKey() + " with value: " + msg.getValue());
			kvServer.putKV(msg.getKey(), msg.getValue());
			return new KVMessageClass(KVMessage.StatusType.PUT_SUCCESS.toString() + "," +
					msg.getKey() + "," + msg.getValue());
		}

		logger.debug("Trying to PUT_UPDATE for key: " + msg.getKey() + " with value: " + msg.getValue());
		kvServer.putKV(msg.getKey(), msg.getValue());
		res = new KVMessageClass(KVMessage.StatusType.PUT_UPDATE.toString() + "," +
				msg.getKey() + "," + msg.getValue());
		logger.debug("res msg array:" + Arrays.toString(res.getMsgBytes()));
		return res;

	}
	private KVMessageClass handleMessage(KVMessageClass msg){
		KVMessageClass res;
		boolean keyExists;
		try{
			switch (msg.getStatus()) {
				case GET:
					logger.debug("Trying to GET " + msg.getKey());
					keyExists = kvServer.inCache(msg.getKey()) || kvServer.inStorage(msg.getKey());
					if (keyExists) {
						logger.debug("Key found: "  + msg.getKey());
						res = new KVMessageClass(KVMessage.StatusType.GET_SUCCESS.toString() + "," + msg.getKey() + "," +
								kvServer.getKV(msg.getKey()));
						return res;
					}
					logger.debug("Key not found: " + msg.getKey());
					res = new KVMessageClass(KVMessage.StatusType.GET_ERROR.toString() + "," + msg.getKey() + ",");
					return res;
				case PUT:
					return handlePUTMessage(msg);
				default:
					//TODO: add invalid message class
					return res = new KVMessageClass("NULL, NULL, NULL");
				}
		} catch (Exception e) {
			//TODO: add unique exceptions + logger interaction
			return res = new KVMessageClass("NULL");
		}

	}

	/**
	 * Method sends a TextMessage using this socket.
	 * @param msg the message that is to be sent.
	 * @throws IOException some I/O error regarding the output stream 
	 */
	public void sendMessage(KVMessageClass msg) throws IOException {
		byte[] msgBytes = msg.getMsgBytes();
		logger.debug(Arrays.toString(msgBytes));
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		logger.info("SEND \t<" 
				+ clientSocket.getInetAddress().getHostAddress() + ":" 
				+ clientSocket.getPort() + ">: '" 
				+ msg.getMsg() +"'");
    }

	private KVMessageClass receiveMessage() throws IOException {
		
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];
		
		/* read first char from stream */
		byte read = (byte) input.read();	
		boolean reading = true;
		if (read == 13) {
			read = (byte) input.read();
		}
//		logger.info("First Char: " + read);
//		Check if stream is closed (read returns -1)
//		if (read == -1){
//			TextMessage msg = new TextMessage("");
//			return msg;
//		}

		while(/*read != 13 &&*/  read != 10 && read !=-1 && reading) {/* CR, LF, error */
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
		logger.info("Msg:" + Arrays.toString(msgBytes));
		/* build final String */
		KVMessageClass msg = new KVMessageClass(msgBytes);
		logger.info("RECEIVE \t<" 
				+ clientSocket.getInetAddress().getHostAddress() + ":" 
				+ clientSocket.getPort() + ">: '" 
				+ msg.getMsg().trim() + "'");
		return msg;
    }

}