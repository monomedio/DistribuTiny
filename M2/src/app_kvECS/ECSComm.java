package app_kvECS;

import org.apache.log4j.Logger;
import shared.messages.IKVMessage;
import shared.messages.KVMessage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;


/**
 * Represents a connection end point for a particular server that is
 * connected to ECS. This class is responsible for message reception
 * and sending.
 */
public class ECSComm implements Runnable {

	private static Logger logger = Logger.getRootLogger();

	private boolean isOpen;
	private static final int BUFFER_SIZE = 1000;
	private static final int DROP_SIZE = 129 * BUFFER_SIZE;

	private Socket clientSocket;
	private InputStream input;
	private OutputStream output;

	private ECS ecs;

	/**
	 * Constructs a new ClientConnection object for a given TCP socket.
	 * @param clientSocket the Socket object for the client connection.
	 */
	public ECSComm(Socket clientSocket, ECS ecs) {
		this.clientSocket = clientSocket;
		this.isOpen = true;
		this.ecs = ecs;
	}
	
	/**
	 * Initializes and starts the client connection. 
	 * Loops until the connection is closed or aborted by the client.
	 */
	public void run() {
		try {
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();
			ecs.addServer(this.clientSocket.getInetAddress().getHostAddress() + ":" + this.clientSocket.getPort());
//			// TODO: Call updateMetadataAdd to hash ip:port and update metadata
//			ECSComm successorConnection = this.ecs.updateMetadataAdd(this.clientSocket.getInetAddress().getHostAddress() + ":" + this.clientSocket.getPort());
//			// TODO: update metadata of new server
//			if (successorConnection == null) {
//				// TODO: send metadata update to new server only
//			} else {
//				// TODO: else tell successor node (via ECSComm?) to WRITE_LOCK and start copying data
//				// TODO: wait for write success from new server after copying, then update metadata for all servers
//				// TODO: release WRITE_LOCK on successor after updating metadata, then remove data items it is no longer responsible for (memoize during data transfer?)
//			}

			while(isOpen) {
				try {
					KVMessage latestMsg = receiveMessage();
					KVMessage handledMessage = handleMessage(latestMsg);
					if (handledMessage != null) {
						sendMessage(handledMessage);
					}
				/* connection either terminated by the client or lost due to 
				 * network problems*/	
				} catch (IOException ioe) {
					logger.error("Error! Connection lost!");
					isOpen = false;
				} catch (IllegalArgumentException ia) {
					logger.error("Server was terminated!");
					isOpen = false;
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		} catch (IOException ioe) {
			logger.error("Error! Connection could not be established!", ioe);
		} finally {
			try {
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


	private KVMessage handleMessage(KVMessage msg){
		KVMessage res;

		try{
			switch (msg.getStatus()) {
				case TR_RES:
					// Assuming key1;value1;key2;value2;key3;value3 etc.
					String[] alternatingKV = msg.getKey().split(";");
					String sampleKey = alternatingKV[0];
					String responsibleIpPort = this.ecs.findResponsibleServer(sampleKey);
					ECSComm responsibleECSComm = this.ecs.getECSComm(responsibleIpPort);
					responsibleECSComm.sendData(msg.getValue());
					return null;
				case TR_SUCC:

				default:
					return res = new KVMessage(IKVMessage.StatusType.FAILED, "Unknown request");
				}
		} catch (Exception e) {
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

	/**
	 * Used to send a TR_REQ message to successor node to initiate data transfer
	 * @param range updated "<lower>,<upper>" ranges of the successor
	 * @throws IOException
	 */
	public void retrieveData(String range) throws IOException {
		String[] bothRange = range.split(",");
		sendMessage(new KVMessage(IKVMessage.StatusType.TR_REQ, bothRange[0], bothRange[1]));
	}

	/**
	 * Send a TR_INIT message to new server when adding a server for data transfer from successor
	 *
	 * @param data "key1,value1,key2,value2,..." alternating, comma separated keys and values
	 * @throws IOException
	 */
	public void sendData(String data) throws IOException {
		sendMessage(new KVMessage(IKVMessage.StatusType.TR_INIT, data));
	}

	public String getIpAndPort() {
		return clientSocket.getInetAddress().getHostAddress() + ":" + clientSocket.getPort();
	}

	public static void main(String[] args) {

	}

}
