package client;

import app_kvECS.ECSComm;
import org.apache.commons.codec.digest.DigestUtils;
import shared.messages.IKVMessage;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;
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

	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		this.address = address;
		this.port = port;
		this.metadata = new HashMap<String, String>();
	}

	@Override
	public void connect() throws Exception {
		this.socket = new Socket(this.address, this.port);
		this.input = new BufferedInputStream(this.socket.getInputStream());
		this.output = new BufferedOutputStream(this.socket.getOutputStream());
	}

	@Override
	public void disconnect() {
		try {
			this.input.close();
			this.output.close();
		} catch (IOException e) {
			logger.warn("Unable to close I/O.");
		} finally {
			try {
				this.socket.close();
			} catch (IOException e) {
				logger.warn("Unable to close socket.");
			}
		}
	}

	private IKVMessage retryPut(String key, String value) throws Exception {
		try {
			return put(key, value);
		} catch (Exception e) { // Server is offline
			System.out.println("START FOR LOOP");
			for (Map.Entry<String, String> entry : this.metadata.entrySet()) { // Try every server that client knows
				disconnect();
				this.address = entry.getKey().split(":")[0];
				this.port = Integer.parseInt(entry.getKey().split(":")[1]);
				System.out.println("Trying server: " + entry.getKey());
				try {
					connect();
					System.out.println("Connected to: " + entry.getKey());
					IKVMessage message = put(key, value);
					System.out.println("Got message from:" + entry.getKey() + ":" + message.getMessage());
					return message;
				} catch (Exception f) { // Server is offline
					continue;
				}
			}
			throw new Exception();
		}
	}

	@Override
	public IKVMessage put(String key, String value) throws Exception {
		IKVMessage message = new KVMessage(IKVMessage.StatusType.PUT, key, value);

		if (this.metadata.isEmpty() || keyInRange(key,
				this.metadata.get(address+":"+port).split(",")[0],
				this.metadata.get(address+":"+port).split(",")[1]))
		{ // Client doesn't know about anyone else, try the server it is connected to OR Client has metadata and this server is responsible
			sendMessage(message);
		} else // Client is not responsible according to current metadata. Disconnect from current server, connect to responsible server and retry
		{
			String responsibleIpAndPort = findResponsibleServer(key);
			String[] splitResponsible = responsibleIpAndPort.split(":");
			String respIp = splitResponsible[0];
			int respPort = Integer.parseInt(splitResponsible[1]);
			this.address = respIp;
			this.port = respPort;
			disconnect();
			try {
				connect();
			} catch (Exception e) {
				updateMetadataRemove(this.address + ":" + this.port);
				return retryPut(key, value);
			}
			return retryPut(key, value);
		}

		try {
			message = receiveMessage();
		} catch (Exception e) {
			System.out.println("SERVER DOWN, TRY DIFF SERVER");
			updateMetadataRemove(this.address + ":" + this.port);
			return retryPut(key, value);
		}

		if (message.getStatus() == IKVMessage.StatusType.SERVER_NOT_RESPONSIBLE) // Currently connected server is not responsible
		{
			// Send KEYRANGE request for metadata
			sendMessage(new KVMessage(IKVMessage.StatusType.KEYRANGE, "null"));
			IKVMessage metaMessage = receiveMessage();
			// REPLACE METADATA AND RETRY
			HashMap<String, String> newHashMap = new HashMap<>();
			String metaString = metaMessage.getKey();
			String[] tokens = metaString.split(";");

			for (String token : tokens)
			{
				String[] tokenSplit = token.split(",");
				newHashMap.put(tokenSplit[2], tokenSplit[0] + "," + tokenSplit[1]);
			}
			// SET NEW METADATA AND RETRY GET
			this.metadata = newHashMap;
			return retryPut(key, value);
		}
		return message;
	}

	private IKVMessage retryGet(String key) throws Exception {
		try {
			return get(key);
		} catch (Exception e) { // Server is offline
			System.out.println("START FOR LOOP");
			for (Map.Entry<String, String> entry : this.metadata.entrySet()) { // Try every server that client knows
				disconnect();
				this.address = entry.getKey().split(":")[0];
				this.port = Integer.parseInt(entry.getKey().split(":")[1]);
				System.out.println("Trying server: " + entry.getKey());
				try {
					connect();
					System.out.println("Connected to: " + entry.getKey());
					IKVMessage message = get(key);
					System.out.println("Got message from:" + entry.getKey() + ":" + message.getMessage());
					return message;
				} catch (Exception f) { // Server is offline
					continue;
				}
			}
			throw new Exception();
		}
	}

	@Override
	public IKVMessage get(String key) throws Exception {
		IKVMessage message = new KVMessage(IKVMessage.StatusType.GET, key);

		if (this.metadata.isEmpty() || keyInRange(key,
				this.metadata.get(address+":"+port).split(",")[0],
				this.metadata.get(address+":"+port).split(",")[1]))
		{ // Client doesn't know about anyone else, try the server it is connected to OR Client has metadata and this server is responsible
			sendMessage(message);
		} else // Client is not responsible according to current metadata. Disconnect from current server, connect to responsible server and retry
		{
			String responsibleIpAndPort = findResponsibleServer(key);
			String[] splitResponsible = responsibleIpAndPort.split(":");
			String respIp = splitResponsible[0];
			int respPort = Integer.parseInt(splitResponsible[1]);
			this.address = respIp;
			this.port = respPort;
			disconnect();
			try {
				connect();
			} catch (Exception e) {
				updateMetadataRemove(this.address + ":" + this.port);
				return retryGet(key);
			}
			return retryGet(key);
		}

		try {
			message = receiveMessage();
		} catch (Exception e) {
			System.out.println("SERVER DOWN, TRY DIFF SERVER");
			updateMetadataRemove(this.address + ":" + this.port);
			return retryGet(key);
		}

		if (message.getStatus() == IKVMessage.StatusType.SERVER_NOT_RESPONSIBLE) // Currently connected server is not responsible
		{
			// Send KEYRANGE request for metadata
			sendMessage(new KVMessage(IKVMessage.StatusType.KEYRANGE, "null"));
			IKVMessage metaMessage = receiveMessage();
			// REPLACE METADATA AND RETRY
			HashMap<String, String> newHashMap = new HashMap<>();
			String metaString = metaMessage.getKey();
			String[] tokens = metaString.split(";");

			for (String token : tokens)
			{
				String[] tokenSplit = token.split(",");
				newHashMap.put(tokenSplit[2], tokenSplit[0] + "," + tokenSplit[1]);
			}
			// SET NEW METADATA AND RETRY GET
			this.metadata = newHashMap;
			return retryGet(key);
		}
		return message;
	}

	private IKVMessage retryKeyrange() throws Exception {
		try {
			return keyRange();
		} catch (Exception e) { // Server is offline
			for (Map.Entry<String, String> entry : this.metadata.entrySet()) { // Try every server that client knows
				disconnect();
				this.address = entry.getKey().split(":")[0];
				this.port = Integer.parseInt(entry.getKey().split(":")[1]);
				System.out.println("Trying server: " + entry.getKey());
				try {
					connect();
					System.out.println("Connected to: " + entry.getKey());
					IKVMessage message = keyRange();
					return message;
				} catch (Exception f) { // Server is offline
					continue;
				}
			}
			throw new Exception();
		}
	}

	public IKVMessage keyRange() throws Exception {
		IKVMessage message = new KVMessage(IKVMessage.StatusType.KEYRANGE, "null");

		sendMessage(message); // Send keyrange message to the currently connected server

		try {
			message = receiveMessage();
		} catch (Exception e) {
			System.out.println("SERVER DOWN, TRY DIFF SERVER");
			updateMetadataRemove(this.address + ":" + this.port);
			return retryKeyrange();
		}

		if (message.getStatus() == IKVMessage.StatusType.KEYRANGE_SUCCESS) { //update metadata if successful
			// REPLACE METADATA
			HashMap<String, String> newHashMap = new HashMap<>();
			String metaString = message.getKey();
			String[] tokens = metaString.split(";");

			for (String token : tokens)
			{
				String[] tokenSplit = token.split(",");
				newHashMap.put(tokenSplit[2], tokenSplit[0] + "," + tokenSplit[1]);
			}
			// SET NEW METADATA
			this.metadata = newHashMap;
		}

		return message;
	}

	private boolean keyInRange(String key, String lower, String upper) {
		String hashedKey = DigestUtils.md5Hex(key);
		if (hashedKey.compareTo(lower) == 0) {
			return true;
		}
		// if lowerRange is larger than upperRange
		if (lower.compareTo(upper) > 0) {
			// hashedkey <= lowerRange and hasedkey > upperRange
			return ((hashedKey.compareTo(lower) <= 0) && hashedKey.compareTo(upper) > 0);
		} else {
			// lowerRange is smaller than upperRange (wrap around)
			return ((hashedKey.compareTo(lower) <= 0 && (upper.compareTo(hashedKey) > 0))) ||
					((hashedKey.compareTo(lower) > 0) && (upper.compareTo(hashedKey) < 0));
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
		String myFromHash = myRange[0];
		String myToHash = myRange[1];
		this.metadata.remove(ipAndPort);
		String fromHash = "";
		String toHash = "";
		String entryIp = "";
		for (Map.Entry<String, String> entry: this.metadata.entrySet()) {
			String[] range = entry.getValue().split(",");
			fromHash = range[0];
			toHash = range[1];
			entryIp = entry.getKey();
			if (toHash.equals(myFromHash)) {
				toHash = myToHash;
				break;
			}
		}

		this.metadata.replace(entryIp, fromHash + "," + toHash);
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

	private IKVMessage receiveMessage() throws IOException {

		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];

		/* read first char from stream */
		byte read = (byte) input.read();
		boolean reading = true;

		while(read != 13 && reading) {/* carriage return */
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

