package testing;

import app_kvServer.KVServer;
import org.junit.Test;

import client.KVStore;
import junit.framework.TestCase;
import shared.messages.IKVMessage;
import shared.messages.IKVMessage.StatusType;


public class InteractionTest extends TestCase {

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


	@Test
	public void testPut() {
		String key = "foo2";
		String value = "bar2";
		IKVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, value);
			response = kvClient.receiveMessage();
			kvClient.put("foo2", "null");
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
	}

	@Test
	public void testPutDisconnected() {

		kvClient.disconnect();
		String key = "foo";
		String value = "bar";
		Exception ex = null;

		try {
			kvClient.put(key, value);
			kvClient.receiveMessage();
		} catch (Exception e) {
			ex = e;
		}

		assertNotNull(ex);
	}

	@Test
	public void testUpdate() {
		String key = "udTestVal";
		String initialValue = "initial";
		String updatedValue = "updated";

		IKVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, initialValue);
			response = kvClient.receiveMessage();
			kvClient.put(key, updatedValue);
			response = kvClient.receiveMessage();
			while (response.getStatus() != StatusType.PUT_UPDATE || response.getValue().compareTo(updatedValue) != 0) {
//				System.out.println(response.getMessage());
				response = kvClient.receiveMessage();
			}
		} catch (Exception e) {
			ex = e;
		}


		assertTrue(ex == null && response.getStatus() == StatusType.PUT_UPDATE
				&& response.getValue().equals(updatedValue));
	}

	@Test
	public void testDelete() {
		String key = "delTestVal";
		String value = "toDelete";

		IKVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, value);
			kvClient.put(key, "null");
			response = kvClient.receiveMessage();
			response = kvClient.receiveMessage();
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.DELETE_SUCCESS);
	}

	@Test
	public void testGet() {
		String key = "foo";
		String value = "bar";
		IKVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, value);
			kvClient.get(key);
			response = kvClient.receiveMessage();
			while (response.getValue().compareTo("bar") != 0) {
				response = kvClient.receiveMessage();
			}
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getValue().equals("bar"));
	}

	@Test
	public void testGetUnsetValue() {
		String key = "an unset value";
		IKVMessage response = null;
		Exception ex = null;

		try {
			kvClient.get(key);
			response = kvClient.receiveMessage();
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR);
	}




}
