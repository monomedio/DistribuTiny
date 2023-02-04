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
			response = kvClient.put(key, value);
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
		} catch (Exception e) {
			ex = e;
		}

		assertNotNull(ex);
	}

	@Test
	public void testUpdate() {
		String key = "updateTestValue";
		String initialValue = "initial";
		String updatedValue = "updated";
		
		IKVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, initialValue);
			response = kvClient.put(key, updatedValue);
			
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.PUT_UPDATE
				&& response.getValue().equals(updatedValue));
	}
	
	@Test
	public void testDelete() {
		String key = "deleteTestValue";
		String value = "toDelete";
		
		IKVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, value);
			response = kvClient.put(key, "null");
			
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
				response = kvClient.get(key);
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
			response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR);
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

		assertTrue(ex == null && response.getStatus() == StatusType.FAILED);

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

		assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR);
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
				&& response.getStatus() == StatusType.GET_SUCCESS && response.getValue().equals(value));
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
				&& response.getStatus() == StatusType.GET_SUCCESS && response.getValue().equals("v3-update"));
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
				&& response.getStatus() == StatusType.GET_ERROR);
	}


}
