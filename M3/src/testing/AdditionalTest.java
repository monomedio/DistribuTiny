package testing;

import client.KVStore;
import junit.framework.TestCase;
import org.junit.Test;
import shared.messages.IKVMessage;
import shared.messages.KVMessage;

public class AdditionalTest extends TestCase {

	private KVStore kvClient;
	private KVStore kvClient2;

	private KVStore kvClient3;
	private IKVMessage kvMessage;

	public void setUp() {
		kvClient = new KVStore("localhost", 50000);
		kvClient2 = new KVStore("localhost", 50000);
		kvClient3 = new KVStore("localhost", 50000);
		try {
			kvClient.connect();
			kvClient2.connect();
			kvClient3.connect();

		} catch (Exception e) {
		}
	}

	public void tearDown() {
		kvClient.disconnect();
		kvClient2.disconnect();
		kvClient3.disconnect();
	}

	// Our Test Cases
	@Test
	public void testMessageSizeExceeded() {
		String key = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
				"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
				"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
				"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
		String value = "hi";
		IKVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == IKVMessage.StatusType.failed);

	}

	@Test
	public void testValueSizeExceeded() {
		String key = "hello";
		StringBuilder value = new StringBuilder("a");
		for (int i = 0; i <= 59999; i++) {
			value.append("a");
		}
		IKVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, value.toString());
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == IKVMessage.StatusType.failed);

	}

	@Test
	public void testGetDeletedValue() {
		String key = "k1";
		String value = "v1";
		IKVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, value);
			kvClient.put(key, "null");
			response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == IKVMessage.StatusType.get_error);
	}

	@Test
	public void testTwoClientsGet() {
		String key = "k2";
		String value = "v2";
		IKVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, value);
			response = kvClient2.get(key);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null
				&& response.getStatus() == IKVMessage.StatusType.get_success && response.getValue().equals(value));
	}

	@Test
	public void testTwoClientsUpdate() {
		String key = "k3";
		String value = "v3";
		IKVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, value);
			kvClient2.put(key, "v3-update");
			response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null
				&& response.getStatus() == IKVMessage.StatusType.get_success && response.getValue().equals("v3-update"));
	}

	@Test
	public void testThreeClientsUpdate() {
		String key = "k3";
		String value = "v3";
		IKVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, value);
			kvClient2.put(key, "v3-update");
			kvClient3.put(key, "v4-update");
			response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null
				&& response.getStatus() == IKVMessage.StatusType.get_success && response.getValue().equals("v4-update"));
	}
	@Test
	public void testTwoClientsDeleteGet() {
		String key = "k4";
		String value = "v4";
		IKVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, value);
			kvClient2.put(key, "null");
			response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null
				&& response.getStatus() == IKVMessage.StatusType.get_error);
	}

	@Test
	public void testThreeAryConstructor() {
		kvMessage = new KVMessage(IKVMessage.StatusType.get, "key", "value");
		assertTrue(kvMessage.getMessage().equals("get key value\r\n")
				&& kvMessage.getStatus().equals(IKVMessage.StatusType.get)
				&& kvMessage.getKey().equals("key")
				&& kvMessage.getValue().equals("value"));
	}

	@Test
	public void testBinaryConstructor() {
		kvMessage = new KVMessage(IKVMessage.StatusType.get, "key");
		assertTrue(kvMessage.getMessage().equals("get key\r\n")
				&& kvMessage.getStatus().equals(IKVMessage.StatusType.get)
				&& kvMessage.getKey().equals("key"));
	}

	@Test
	public void testByteConstructor() {
		byte[] msgArray = {112, 117, 116, 32, 104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100};
		kvMessage = new KVMessage(msgArray);
		assertTrue(kvMessage.getMessage().equals("put hello world\r\n")
				&& kvMessage.getStatus().equals(IKVMessage.StatusType.put)
				&& kvMessage.getKey().equals("hello")
				&& kvMessage.getValue().equals("world"));
	}



}
