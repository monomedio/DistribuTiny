package client;

import org.apache.commons.codec.digest.DigestUtils;
import shared.messages.IKVMessage;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.sql.Array;
import java.util.*;

import org.apache.log4j.Logger;
import shared.messages.KVMessage;

public class KVStore implements KVCommInterface {

	private Logger logger = Logger.getRootLogger();

	private String address;
	private int port;
	private HashMap<String, String> metadata;
	private HashMap<String, String> read_metadata;

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
//			System.out.println("START FOR LOOP");
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
		IKVMessage message = new KVMessage(IKVMessage.StatusType.put, key, value);

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

		if (message.getStatus() == IKVMessage.StatusType.server_not_responsible) // Currently connected server is not responsible
		{
			// Send KEYRANGE request for metadata
			sendMessage(new KVMessage(IKVMessage.StatusType.keyrange));
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
			ArrayList<String> arr = new ArrayList();
			for (Map.Entry<String, String> entry : this.read_metadata.entrySet()) { // Try every server that client knows
				disconnect();
				this.address = entry.getKey().split(":")[0];
				this.port = Integer.parseInt(entry.getKey().split(":")[1]);
				System.out.println("Trying server: " + entry.getKey());
				if (keyInRange(key, entry.getValue().split(",")[0], entry.getValue().split(",")[1])) {
					arr.add(entry.getKey());
				}
			}

			Collections.shuffle(arr);
//			System.out.println(arr);

			for (String entry : arr) {
				try {
					this.address = entry.split(":")[0];
					this.port = Integer.parseInt(entry.split(":")[1]);
					connect();
					System.out.println("Connected to: " + entry);
					IKVMessage message = get(key);
					System.out.println("Got message from:" + entry + ":" + message.getMessage());
					// Ask for updated metadata
					message = new KVMessage(IKVMessage.StatusType.keyrange_read);
					sendMessage(message);
					message = receiveMessage();

					HashMap<String, String> newHashMap = new HashMap<>();
					String metaString = message.getKey();
					String[] tokens = metaString.split(";");

					for (String token : tokens) {
						String[] tokenSplit = token.split(",");
						newHashMap.put(tokenSplit[2], tokenSplit[0] + "," + tokenSplit[1]);
					}

					this.read_metadata = newHashMap;
					return message;
				} catch (Exception f) { // Server is offline
//					System.out.println("DELETE retry");
					f.printStackTrace();
					this.read_metadata.remove(entry);
				}
			}
			throw new Exception();
		}
	}

	@Override
	public IKVMessage get(String key) throws Exception {
		IKVMessage message = new KVMessage(IKVMessage.StatusType.get, key);
		try {
			if (this.read_metadata == null) {
				sendMessage(message);
				message = receiveMessage();
			} else {
				ArrayList<String> arr = new ArrayList();
				disconnect();
				for (Map.Entry<String, String> entry : this.read_metadata.entrySet()) { // Try every server that client knows
//					this.address = entry.getKey().split(":")[0];
//					this.port = Integer.parseInt(entry.getKey().split(":")[1]);
//					System.out.println("Trying server: " + entry.getKey());
					if (keyInRange(key, entry.getValue().split(",")[0], entry.getValue().split(",")[1])) {
						arr.add(entry.getKey());
					}
				}

				Collections.shuffle(arr);
//				System.out.println(arr);

				boolean found = false;

				for (String entry : arr) {
					try {
						this.address = entry.split(":")[0];
						this.port = Integer.parseInt(entry.split(":")[1]);
						connect();
						System.out.println("Connected to: " + entry);
						sendMessage(new KVMessage(IKVMessage.StatusType.get, key));
						IKVMessage loop_message = receiveMessage();
						System.out.println("Got message from:" + entry + ":" + loop_message.getMessage());
						found = true;
						// Ask for updated metadata
						keyRangeRead();
						message = loop_message;
						if (message.getStatus() == IKVMessage.StatusType.get_success || message.getStatus() == IKVMessage.StatusType.get_error) {
							break;
						}
					} catch (Exception f) { // Server is offline
//						System.out.println("DELETE");
						this.read_metadata.remove(entry);
					}
				}
				if (found == false) {
					throw new Exception();
				}
			}
//			sendMessage(message);
//			message = receiveMessage();
			if (message.getStatus() == IKVMessage.StatusType.get_success || message.getStatus() == IKVMessage.StatusType.get_error) {
				return message;
			}
			else if (message.getStatus() == IKVMessage.StatusType.server_not_responsible) {
				message = new KVMessage(IKVMessage.StatusType.keyrange_read);
				sendMessage(message);
				message = receiveMessage();

				HashMap<String, String> newHashMap = new HashMap<>();
				String metaString = message.getKey();
				String[] tokens = metaString.split(";");

				for (String token : tokens) {
					String[] tokenSplit = token.split(",");
					newHashMap.put(tokenSplit[2], tokenSplit[0] + "," + tokenSplit[1]);
				}

				this.read_metadata = newHashMap;
				return retryGet(key);
			}
		} catch (IOException e) {
			if (read_metadata == null) { // connected server is dead and read_metadata is null
				throw new IOException();
			}
			else {
				return retryGet(key);
			}
		}
		return new KVMessage(IKVMessage.StatusType.failed);
	}

	private IKVMessage retryKeyrange() throws Exception {
		try {
			return keyRange();
		} catch (Exception e) { // Server is offline
//			System.out.println("START FOR LOOP");
			for (Map.Entry<String, String> entry : this.metadata.entrySet()) { // Try every server that client knows
				disconnect();
				this.address = entry.getKey().split(":")[0];
				this.port = Integer.parseInt(entry.getKey().split(":")[1]);
				System.out.println("Trying server: " + entry.getKey());
				try {
					connect();
					System.out.println("Connected to: " + entry.getKey());
					IKVMessage message = keyRange();
					System.out.println("Got message from:" + entry.getKey() + ":" + message.getMessage());
					return message;
				} catch (Exception f) { // Server is offline
					continue;
				}
			}
			throw new Exception();
		}
	}

	public IKVMessage keyRange() throws Exception {
		IKVMessage message = new KVMessage(IKVMessage.StatusType.keyrange);

		sendMessage(message); // Send keyrange message to the currently connected server

		try {
			message = receiveMessage();
		} catch (Exception e) {
//			System.out.println("SERVER DOWN, TRY DIFF SERVER");
			updateMetadataRemove(this.address + ":" + this.port);
			return retryKeyrange();
		}

		if (message.getStatus() == IKVMessage.StatusType.keyrange_success) { //update metadata if successful
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
		String myLower = myRange[0];
		String myUpper = myRange[1];
		this.metadata.remove(ipAndPort);
		String fromHash = "";
		String toHash = "";
		String entryIp = "";
		for (Map.Entry<String, String> entry: this.metadata.entrySet()) {
			String[] range = entry.getValue().split(",");
			toHash = range[0];
			fromHash = range[1];
			entryIp = entry.getKey();
			if (toHash.equals(myUpper)) {
				toHash = myLower;
				break;
			}
		}

		this.metadata.replace(entryIp, toHash + "," + fromHash);
	}

	public IKVMessage keyRangeRead() throws IOException {
		IKVMessage message = new KVMessage(IKVMessage.StatusType.keyrange_read);
		sendMessage(message);
		message = receiveMessage();
		HashMap<String, String> newHashMap = new HashMap<>();
		String metaString = message.getKey();
		String[] tokens = metaString.split(";");

		for (String token : tokens) {
			String[] tokenSplit = token.split(",");
			newHashMap.put(tokenSplit[2], tokenSplit[0] + "," + tokenSplit[1]);
		}

		this.read_metadata = newHashMap;

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

	private IKVMessage receiveMessage() throws IOException {

		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];
		
		/* read first char from stream */
		byte read = (byte) input.read();
		if (read == -1) {
			throw new IOException();
		}
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
			if (read == -1) {
				throw new IOException();
			}
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

