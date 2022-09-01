package testing;

import java.io.File;

import junit.framework.TestCase;

import org.apache.log4j.Logger;

import client.KVStore;

import app_kvServer.KVServer;
import app_kvECS.ECS;

import shared.messages.IKVMessage;
import shared.messages.IKVMessage.StatusType;

public class BasicTest extends TestCase {

	private final String STORAGE_DIRECTORY = "storage/xman/";

	private KVStore kvClient;
	private static Logger logger = Logger.getRootLogger();
	public static ECS server;
	public static int port;

	public void setUp() {
		if(server.testGetServerCount() == 0){
			server.addNode("any",20);
			try {
				logger.info("sleeping");
				Thread.currentThread().sleep(2000);
				server.start();
			} catch (Exception e) {

				logger.info("error sleep: " + e.getMessage());
			}
		}
		kvClient = new KVStore("localhost", port);
		try {
			kvClient.connect();
		} catch (Exception e) {
			logger.error(e);
		}
	}

	public void tearDown() {
		kvClient.disconnect();
		try {
			logger.info("sleeping");
			server.removeNode();
			Thread.currentThread().sleep(2000);
		} catch (Exception e) {

			logger.info("error sleep: " + e.getMessage());
		}
		
	}

	public void testPut() {
		logger.info("====TEST PUT====");
		final String KEY = "foo";
		final String VALUE = "bar";

		IKVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(KEY, VALUE);
		} catch (Exception e) {
			ex = e;
		}

		assertNull(ex);
		assertTrue(new File(STORAGE_DIRECTORY + KEY).isFile());
		assertTrue(response.getStatus() == StatusType.PUT_SUCCESS);
		assertTrue(response.getKey().equals(KEY));
		assertTrue(response.getValue().equals(VALUE));
	}

	public void testUpdate() {
		logger.info("====TEST PUT UPDATE====");
		final String KEY = "updateTestValue";
		String value = "initial";

		IKVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(KEY, value);
			assertTrue(new File(STORAGE_DIRECTORY + KEY).isFile());
			assertTrue(response.getStatus() == StatusType.PUT_SUCCESS);
			assertTrue(response.getKey().equals(KEY));
			assertTrue(response.getValue().equals(value));

			// Perform update:
			value = "updated";
			response = kvClient.put(KEY, value);
		} catch (Exception e) {
			ex = e;
		}

		assertNull(ex);
		assertTrue(new File(STORAGE_DIRECTORY + KEY).isFile());
		assertTrue(response.getStatus() == StatusType.PUT_UPDATE);
		assertTrue(response.getKey().equals(KEY));
		assertTrue(response.getValue().equals(value));
	}

	public void testDelete() {
		logger.info("====TEST PUT DELETE====");
		final String KEY = "deleteTestValue";
		String value = "toDelete";

		IKVMessage response = null;
		Exception ex = null;

		try {
			// Insert the record
			response = kvClient.put(KEY, value);
			assertTrue(new File(STORAGE_DIRECTORY + KEY).isFile());
			assertTrue(response.getStatus() == StatusType.PUT_SUCCESS);
			assertTrue(response.getKey().equals(KEY));

			// Delete the record
			value = "null";
			response = kvClient.put(KEY, value);
			// Wait for delete pruning to complete:
			Thread.sleep(500);
		} catch (Exception e) {
			ex = e;
		}

		assertNull(ex);
		assertFalse(new File(STORAGE_DIRECTORY + KEY).isFile());
		assertTrue(response.getStatus() == StatusType.DELETE_SUCCESS);
		assertTrue(response.getKey().equals(KEY));
	}

	public void testDeleteError() {
		logger.info("====TEST GET Error====");
		final String KEY = "foo-udit";
		final String VALUE = "null";
		IKVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(KEY, VALUE);
		} catch (Exception e) {
			ex = e;
		}

		logger.info("delete error: " + response.getStatus());
		assertNull(ex);
		assertTrue(response.getStatus() == StatusType.DELETE_ERROR);
		assertTrue(response.getKey().equals(KEY));
	}

	public void testPutDisconnected() {
		logger.info("====TEST PUT DISCONNECTED====");
		tearDown();

		final String KEY = "foo";
		final String VALUE = "bar";
		Exception ex = null;

		try {
			kvClient.put(KEY, VALUE);
		} catch (Exception e) {
			ex = e;
		}

		assertNotNull(ex);
	}

	public void testGet() {
		logger.info("====TEST GET====");
		final String KEY = "foo";
		final String VALUE = "bar";
		IKVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(KEY, VALUE);
			response = kvClient.get(KEY);
		} catch (Exception e) {
			ex = e;
		}

		assertNull(ex);
		assertTrue(response.getStatus() == StatusType.GET_SUCCESS);
		assertTrue(response.getKey().equals(KEY));
		assertTrue(response.getValue().equals(VALUE));
	}

	public void testGetUnsetValue() {
		logger.info("====TEST GET UNSET====");
		final String KEY = "an_unset_value";
		IKVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.get(KEY);
		} catch (Exception e) {
			ex = e;
		}

		assertNull(ex);
		assertTrue(response.getStatus() == StatusType.GET_ERROR);
		assertTrue(response.getKey().equals(KEY));
	}

}
