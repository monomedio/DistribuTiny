package client;

import app_kvECS.ECSComm;
import org.apache.commons.codec.digest.DigestUtils;
import shared.messages.IKVMessage;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import shared.messages.KVMessage;

public class KVStore implements KVCommInterface {

	private Logger logger = Logger.getRootLogger();

	private String address;
	private int port;
	private HashMap<String, String> metadata;

	private BufferedInputStream input;
	private BufferedOutputStream output;
	private Socket socket;

	private int BUFFER_SIZE = 120100;
	private int DROP_SIZE = 128 * BUFFER_SIZE;

	private boolean isConnected = false;
	private KVMessage retryCache;

	private ArrayList<String> subscriptions;

	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		this.address = address;
		this.port = port;
		this.metadata = new HashMap<String, String>();
		this.subscriptions = new ArrayList<>();
	}

	@Override
	public void connect() throws Exception {
		try {
			this.socket = new Socket(this.address, this.port);
			this.input = new BufferedInputStream(this.socket.getInputStream());
			this.output = new BufferedOutputStream(this.socket.getOutputStream());
			this.isConnected = true;
		} catch (Exception e) {
			this.isConnected = false;
			throw new IOException();
		}
	}

	@Override
	public void disconnect() {
		try {
			this.isConnected = false;
			if (this.socket != null) {
				this.socket.close();
			}
			// TODO: cleanup stuff
//			this.address = null;
//			this.port = null;
		} catch (IOException e) {
			logger.warn("Unable to close socket.");
		}
	}

	public void addSubscription(String key) {
		this.subscriptions.add(key);
	}

	public void removeSubscription(String key) {
		this.subscriptions.remove(key);
	}

	public ArrayList<String> getSubscriptions() {
		return subscriptions;
	}

	public boolean getIsConnected() {
		return this.isConnected;
	}

	public String getAddress() {
		return this.address;
	}

	public int getPort() {
		return this.port;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public void setPort(int port) {
		this.port = port;
	}

	@Override
	public void put(String key, String value) throws Exception {
		KVMessage message = new KVMessage(IKVMessage.StatusType.PUT, key, value);

		if (!this.metadata.isEmpty()) {
			String[] lowerUpper = this.metadata.get(this.address + ":" + this.port).split(",");
			String lower = lowerUpper[0];
			String upper = lowerUpper[1];
			if (!keyInRange(key, lower, upper)) {
				String responsibleServer = findResponsibleServer(key);
				String[] ipPort = responsibleServer.split(":");
				disconnect();
				this.address = ipPort[0];
				this.port = Integer.parseInt(ipPort[1]);
				connect();
			}
		}
		this.retryCache = message;
		sendMessage(message);
	}

	@Override
	public void get(String key) throws Exception {
		KVMessage message = new KVMessage(IKVMessage.StatusType.GET, key);

		if (!this.metadata.isEmpty()) {
			String[] lowerUpper = this.metadata.get(this.address + ":" + this.port).split(",");
			String lower = lowerUpper[0];
			String upper = lowerUpper[1];
			if (!keyInRange(key, lower, upper)) {
				String responsibleServer = findResponsibleServer(key);
				String[] ipPort = responsibleServer.split(":");
				disconnect();
				this.address = ipPort[0];
				this.port = Integer.parseInt(ipPort[1]);
				connect();
			}
		}
		this.retryCache = message;
		sendMessage(message);
	}

	@Override
	public void keyRange() throws Exception {
		KVMessage message = new KVMessage(IKVMessage.StatusType.KEYRANGE);
		sendMessage(message);
	}

	private void handleMessage(IKVMessage message) throws Exception {
		IKVMessage.StatusType status = message.getStatus();
		if (status == IKVMessage.StatusType.PUT_ERROR || status == IKVMessage.StatusType.GET_ERROR || status == IKVMessage.StatusType.DELETE_ERROR ||
				status == IKVMessage.StatusType.FAILED || status == IKVMessage.StatusType.SERVER_WRITE_LOCK ||
				status == IKVMessage.StatusType.SERVER_STOPPED) {
//			System.out.println("BRO");
			this.retryCache = null;
			logger.error(message.getMessage());
			System.out.print("KVClient> ");
		} else if (status == IKVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
			System.out.println(message.getMessage());
			sendMessage(new KVMessage(IKVMessage.StatusType.KEYRANGE));
		} else if (status == IKVMessage.StatusType.KEYRANGE_SUCCESS || status == IKVMessage.StatusType.QUIET_KEYRANGE) {
			HashMap<String, String> newHashMap = new HashMap<>();
			String metaString = message.getKey();
			String[] tokens = metaString.split(";");

			for (String token : tokens) {
				String[] tokenSplit = token.split(",");
				newHashMap.put(tokenSplit[2], tokenSplit[0] + "," + tokenSplit[1]);
			}
			// SET NEW METADATA
			this.metadata = newHashMap;
//			System.out.println(this.metadata);
			// TODO
			if (status == IKVMessage.StatusType.KEYRANGE_SUCCESS) {
				printMessage(message.getMessage());
			}

			// Find responsible server of retryCache, then disconnect, connect to the right one, and sendMessage
			if (this.retryCache != null) {
				String responsibleServer = findResponsibleServer(retryCache.getKey());
				disconnect();
				this.address = responsibleServer.split(":")[0];
				this.port = Integer.parseInt(responsibleServer.split(":")[1]);
				connect();
				sendMessage(this.retryCache);
				this.retryCache = null;
			}
		} else if (status == IKVMessage.StatusType.BROADCAST_DELETE || status == IKVMessage.StatusType.BROADCAST_UPDATE) {
			if (this.subscriptions.contains(message.getKey())) {
				switch (status) {
					case BROADCAST_UPDATE:
							if (this.subscriptions.contains(message.getKey())) {
								System.out.println("[SUBSCRIPTION NOTICE] " + message.getKey() + " has been UPDATED with a new value of " + message.getValue());
								System.out.print("KVClient> ");
							}
							break;
					case BROADCAST_DELETE:
							if (this.subscriptions.contains(message.getKey())) {
								System.out.println("[SUBSCRIPTION NOTICE] " + message.getKey() + " has been DELETED");
								System.out.print("KVClient> ");
							}
							break;
				}
			}
		} else {
			this.retryCache = null;
			printMessage(message.getMessage());
		}
	}

	private void printError(String error){
		System.out.println("KVClient> " + "[ERROR] " +  error);
		System.out.print("KVClient> ");
	}

	private void printMessage(String message){
		System.out.println("KVClient> " + "[MESSAGE] " +  message);
		System.out.print("KVClient> ");
	}


	private boolean keyInRange(String key, String lower, String upper) {
		String hashedKey = DigestUtils.md5Hex(key);
		if (hashedKey.compareTo(upper) == 0) {
			return true;
		}
		// if upperRange is larger than lowerRange
		if (upper.compareTo(lower) > 0) {
			// hashedkey <= upperRange and hashedkey > lowerRange
			return ((hashedKey.compareTo(upper) <= 0) && hashedKey.compareTo(lower) > 0);
		} else {
			// upperRange is smaller than lowerRange (wrap around)
			return ((hashedKey.compareTo(upper) <= 0 && (lower.compareTo(hashedKey) > 0))) ||
					((hashedKey.compareTo(upper) > 0) && (lower.compareTo(hashedKey) < 0));
		}
	}

	public String findResponsibleServer(String sampleKey) {
//		System.out.println("Sample key: " + sampleKey + ". Metadata: " + metadataToString());
		for (Map.Entry<String, String> entry: this.metadata.entrySet()) {
			String[] lowerUpper = entry.getValue().split(",");
			String lower = lowerUpper[0];
			String upper = lowerUpper[1];
			if (keyInRange(sampleKey, lower, upper)) {
				return entry.getKey();
			}
		}
		return "No responsible server found";
	}

	public void updateMetadataRemove(String ipAndPort) {
		String[] myRange = this.metadata.get(ipAndPort).split(",");
		String myToHash = myRange[0];
		String myFromHash = myRange[1];
		this.metadata.remove(ipAndPort);
		String fromHash = "";
		String toHash = "";
		String entryIp = "";
		for (Map.Entry<String, String> entry: this.metadata.entrySet()) {
			String[] range = entry.getValue().split(",");
			toHash = range[0];
			fromHash = range[1];
			entryIp = entry.getKey();
			if (toHash.equals(myFromHash)) {
				toHash = myToHash;
				break;
			}
		}

		this.metadata.replace(entryIp, toHash + "," + fromHash);
//		return this.connections.get(entryIp);
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

	public KVMessage receiveMessage() throws IOException {

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

		if (read == -1) {
			throw new IOException();
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
//		logger.info(Arrays.toString(msgBytes));
		KVMessage msg;
		/* build final String */
		// TODO: isOpen? what's this for
//		if (msgBytes.length == 0) {
//			isOpen = false;
//		}
		try {
			msg = new KVMessage(msgBytes);
		} catch (Exception e) {
			msg = new KVMessage(KVMessage.StatusType.FAILED, "Error");
		}
//		System.out.println("RECEIVE \t<"
//				+ this.address + ":"
//				+ this.port + ">: '"
//				+ msg.getMessage() + "'");
		return msg;
	}

	@Override
	public void run() {
		while (true) {
			if (this.isConnected) {
				try {
					KVMessage received = receiveMessage();
//					System.out.println(received.getMessage());
					handleMessage(received);
				} catch (IOException ioe) {
					// TODO: socket closed
					if (this.isConnected) {
						System.out.println("\nSocket closed! (Connected)");
						System.out.print("KVClient> ");
					}
					disconnect();
				} catch (IllegalArgumentException ia) {
					logger.error("Server was terminated!");
					disconnect();
				} catch (Exception e) {
					e.printStackTrace();
				}
			} else {
				System.out.print("");
			}
		}
	}
}

