package testing;

import client.KVStore;
import junit.framework.TestCase;
import org.junit.Test;
import shared.messages.IKVMessage;
import shared.messages.IKVMessage.StatusType;


public class InteractionTest extends TestCase {

	private KVStore kvClient;
	private KVStore kvClient2;

	private KVStore kvClient3;
	
	public void setUp() {
		kvClient = new KVStore("localhost", 50000);
		kvClient2 = new KVStore("localhost", 50000);
		kvClient3 = new KVStore("localhost", 50001);
		try {
			kvClient.connect();
			kvClient2.connect();
			kvClient3.connect();

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

		assertTrue(ex == null && response.getStatus() == StatusType.put_success);
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
		String key = "udTestVal";
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

		assertTrue(ex == null && response.getStatus() == StatusType.put_update
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
			response = kvClient.put(key, "null");
			
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.delete_success);
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

		assertTrue(ex == null && response.getStatus() == StatusType.get_error);
	}


	@Test
	public void testGetReplica() {
		String key = "foo";
		String value = "bar";
		IKVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient3.get(key);
			System.out.println(response.getMessage());
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.get_success && response.getValue().equals(value));
	}

	@Test
	public void testGetDeletedValueFromReplica() {
		String key = "delTestVal";
		String value = "toDelete";

		IKVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, value);
			response = kvClient3.get(key);
			assertEquals(response.getValue(), value);
			kvClient.put(key, "null");
			response = kvClient3.get(key);

		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.get_error);
	}

	@Test
	public void testGetUpdatedValueFromReplica() {
		String key = "keykey";
		String value = "initial";

		IKVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, value);
			response = kvClient3.get(key);
			assertEquals(response.getValue(), value);
			kvClient.put(key, "updated");
			response = kvClient3.get(key);

		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getValue().equals("updated"));
	}


	@Test
	public void testMultipleReplicasGet() {
		String key = "keykey";
		String value = "valval";

		IKVMessage response = null;
		IKVMessage response2 = null;
		Exception ex = null;

		try {
			kvClient.put(key, value);
			response = kvClient3.get(key);
			response2 = kvClient2.get(key);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.get_success && response.getValue().equals(value));
		assertTrue(response2.getStatus() == StatusType.get_success && response2.getValue().equals(value));

	}

	@Test
	public void testMultipleReplicasDeletedValue() {
		String key = "keykey";
		String value = "valval";

		IKVMessage response = null;
		IKVMessage response2 = null;
		Exception ex = null;

		try {
			kvClient.put(key, value);
			response = kvClient3.get(key);
			response2 = kvClient2.get(key);
			assertEquals(response.getValue(), value);
			assertEquals(response2.getValue(), value);
			kvClient.put(key, "null");
			response = kvClient3.get(key);
			response2 = kvClient2.get(key);

		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.get_error);
		assertTrue(response2.getStatus() == StatusType.get_error);

	}

	@Test
	public void testMultipleReplicasUpdatedValue() {
		String key = "keykey";
		String value = "valval";

		IKVMessage response = null;
		IKVMessage response2 = null;
		Exception ex = null;

		try {
			kvClient.put(key, value);
			response = kvClient3.get(key);
			response2 = kvClient2.get(key);
			assertEquals(response.getValue(), value);
			assertEquals(response2.getValue(), value);
			kvClient.put(key, "updated");
			response = kvClient3.get(key);
			response2 = kvClient2.get(key);

		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getValue().equals("updated") && response2.getValue().equals("updated"));

	}








}
