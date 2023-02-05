package testing;

import client.KVStore;
import org.junit.Test;

import junit.framework.TestCase;
import shared.messages.IKVMessage;

public class AdditionalTest extends TestCase {

	private KVStore kvClient;
	private KVStore kvClient2;

	public void setUp() {
		kvClient = new KVStore("localhost", 50000);
		kvClient2 = new KVStore("localhost", 50000);
		try {
			kvClient.connect();
			kvClient2.connect();

		} catch (Exception e) {
		}
	}

	public void tearDown() {
		kvClient.disconnect();
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

		assertTrue(ex == null && response.getStatus() == IKVMessage.StatusType.FAILED);

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

		assertTrue(ex == null && response.getStatus() == IKVMessage.StatusType.GET_ERROR);
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
				&& response.getStatus() == IKVMessage.StatusType.GET_SUCCESS && response.getValue().equals(value));
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
				&& response.getStatus() == IKVMessage.StatusType.GET_SUCCESS && response.getValue().equals("v3-update"));
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
				&& response.getStatus() == IKVMessage.StatusType.GET_ERROR);
	}

}
